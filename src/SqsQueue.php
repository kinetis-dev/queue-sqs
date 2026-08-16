<?php

declare(strict_types=1);

namespace Kinetis\QueueSqs;

use Kinetis\Instrumentation\Telemetry;
use AsyncAws\Sqs\Enum\MessageSystemAttributeName;
use AsyncAws\Sqs\Enum\QueueAttributeName;
use AsyncAws\Sqs\SqsClient;
use InvalidArgumentException;
use Kinetis\Queue\Job;
use Kinetis\Queue\JobSerializer;
use Kinetis\Queue\QueueInterface;
use Kinetis\Queue\QueuedJob;
use Kinetis\QueueSqs\Exception\SqsQueueException;
use Throwable;

/**
 * SQS already solves what Kinetis\Queue\RedisQueue and Kinetis\Queue\SqlQueue
 * each needed their own mechanism for: per-message delay (SendMessage's own
 * DelaySeconds, capped at 900 seconds — SQS's own hard limit, thrown against
 * here rather than silently clamped) and reliable at-least-once delivery
 * (a message stays invisible, not deleted, for its queue's visibility
 * timeout once received — release()/ack()/fail() are all just
 * ChangeMessageVisibility/DeleteMessage calls, no separate "processing
 * list"/"reserved_at column" of our own to maintain).
 *
 * $attempts (see QueuedJob) comes directly from SQS's own
 * ApproximateReceiveCount system attribute — no attempts bookkeeping of our
 * own needed, unlike RedisQueue (embedded in the JSON payload) or SqlQueue
 * (a dedicated column). AWS documents this count as *approximate*, not
 * exact, under rare failure conditions — a disclosed imprecision, the same
 * category as RedisQueue's own delayed-job-promotion timing note.
 *
 * $maxAttempts has no native SQS equivalent, so it travels as a custom
 * "maxAttempts" MessageAttribute, set at push() and read back at pop() —
 * absent means null, deferring to the processing QueueWorker's own
 * $defaultMaxAttempts, identical to every other backend.
 *
 * Queue names are resolved to SQS queue URLs via GetQueueUrl and cached for
 * this instance's lifetime — one instance is constructed once per worker
 * (the same lifecycle RedisQueue/SqlQueue already have), so the cache never
 * spans more than one worker process. $queueNamePrefix (optional) lets
 * "high"/"default" map to e.g. "myapp-high"/"myapp-default" so multiple
 * environments sharing one AWS account don't collide on plain queue names.
 * A queue itself is never auto-created here — the same "real
 * infrastructure resource, provisioned explicitly, not a side effect of
 * normal runtime operation" reasoning SqlQueue's own `kinetis_queue_jobs`
 * table (deliberately not auto-created, unlike SqlMigrationRepository's
 * tiny bookkeeping table) already applies.
 *
 * pop()'s per-queue long-poll uses SQS's own ReceiveMessage WaitTimeSeconds
 * (capped at 20 seconds, SQS's own hard limit) to block without busy-polling
 * — no Kinetis\Async\Timer::delay() between attempts the way SqlQueue needs
 * (SQL has no blocking-wait primitive at all), and no concurrently()
 * wrapper either: the injected AmpHttpClient transport tolerates being
 * called from plain top-level code without an existing Fiber. Checking
 * multiple $queues in priority order uses a short, fixed per-queue
 * WaitTimeSeconds rather than the full remaining budget on the first one —
 * a deliberate cost/responsiveness tradeoff disclosed here: a shorter
 * value notices a higher-priority queue's new job sooner at the cost of
 * more (cheap, but not free) ReceiveMessage calls; a longer one is the
 * opposite. Standard SQS queues only — FIFO queues (the `.fifo` suffix,
 * requiring MessageGroupId on every send) are not supported.
 */
final class SqsQueue implements QueueInterface
{
    private const MAX_DELAY_SECONDS = 900;

    private const MAX_WAIT_TIME_SECONDS = 20;

    private const PER_QUEUE_WAIT_TIME_SECONDS = 5;

    private const MAX_ATTEMPTS_ATTRIBUTE = 'maxAttempts';

    private const METADATA_ATTRIBUTE = 'metadata';

    /** @var array<string, string> */
    private array $queueUrlsByName = [];

    public function __construct(
        private readonly SqsClient $client,
        private readonly string $queueNamePrefix = '',
    ) {}

    #[\Override]
    public function push(Job $job, int $delaySeconds = 0, string $queue = 'default', ?int $maxAttempts = null): void
    {
        if ($delaySeconds > self::MAX_DELAY_SECONDS) {
            throw new InvalidArgumentException(
                'SQS cannot delay a message by more than ' . self::MAX_DELAY_SECONDS . " seconds (requested {$delaySeconds}).",
            );
        }

        $telemetry = Telemetry::global();
        $telemetryToken = $telemetry->jobPushStarted($job::class, $queue);

        try {
            $serialized = JobSerializer::serialize($job);

            $input = [
                'QueueUrl' => $this->resolveQueueUrl($queue),
                'MessageBody' => json_encode($serialized, JSON_THROW_ON_ERROR),
                'DelaySeconds' => $delaySeconds,
            ];

            $attributes = [];

            if ($maxAttempts !== null) {
                $attributes[self::MAX_ATTEMPTS_ATTRIBUTE] = [
                    'DataType' => 'Number',
                    'StringValue' => (string) $maxAttempts,
                ];
            }

            $metadata = $telemetry->jobPushMetadata($telemetryToken);

            if ($metadata !== []) {
                // One JSON-encoded attribute, whatever the carrier keys —
                // SQS caps a message at ten attributes, so per-key
                // attributes would leak that limit into the metadata
                // contract.
                $attributes[self::METADATA_ATTRIBUTE] = [
                    'DataType' => 'String',
                    'StringValue' => json_encode($metadata, JSON_THROW_ON_ERROR),
                ];
            }

            if ($attributes !== []) {
                $input['MessageAttributes'] = $attributes;
            }

            $this->client->sendMessage($input);
            $telemetry->jobPushEnded($telemetryToken, null);
        } catch (Throwable $e) {
            $telemetry->jobPushEnded($telemetryToken, $e);

            throw $e;
        }
    }

    #[\Override]
    public function pop(int $timeoutSeconds = 0, array $queues = ['default']): ?QueuedJob
    {
        if ($queues === []) {
            return null;
        }

        $deadline = $timeoutSeconds > 0 ? microtime(true) + $timeoutSeconds : null;

        while (true) {
            foreach ($queues as $queue) {
                if ($deadline !== null && microtime(true) >= $deadline) {
                    return null;
                }

                $waitTimeSeconds = self::PER_QUEUE_WAIT_TIME_SECONDS;

                if ($deadline !== null) {
                    $waitTimeSeconds = max(0, min($waitTimeSeconds, (int) floor($deadline - microtime(true))));
                }

                $job = $this->receiveFrom($queue, min($waitTimeSeconds, self::MAX_WAIT_TIME_SECONDS));

                if ($job !== null) {
                    return $job;
                }
            }
        }
    }

    #[\Override]
    public function ack(QueuedJob $job): void
    {
        $this->client->deleteMessage([
            'QueueUrl' => $this->resolveQueueUrl($job->queue),
            'ReceiptHandle' => (string) $job->handle,
        ]);
    }

    #[\Override]
    public function release(QueuedJob $job): void
    {
        // VisibilityTimeout: 0 makes the message visible again immediately
        // rather than waiting out its queue's normal visibility timeout —
        // the same "available for retry right away" intent RedisQueue's
        // pushHead()-back-onto-pending and SqlQueue's reserved_at = NULL
        // both give.
        $this->client->changeMessageVisibility([
            'QueueUrl' => $this->resolveQueueUrl($job->queue),
            'ReceiptHandle' => (string) $job->handle,
            'VisibilityTimeout' => 0,
        ]);
    }

    #[\Override]
    public function fail(QueuedJob $job): void
    {
        $this->client->deleteMessage([
            'QueueUrl' => $this->resolveQueueUrl($job->queue),
            'ReceiptHandle' => (string) $job->handle,
        ]);
    }

    /**
     * SQS reports message counts as estimates rather than exact figures —
     * `ApproximateNumberOfMessages` plus `ApproximateNumberOfMessagesDelayed`
     * here, so a delayed job counts as outstanding the same way it does on
     * every other backend. Accurate enough to alert on, never a value to
     * branch on.
     */
    #[\Override]
    public function size(string $queue = 'default'): int
    {
        $attributes = $this->client->getQueueAttributes([
            'QueueUrl' => $this->resolveQueueUrl($queue),
            'AttributeNames' => [
                QueueAttributeName::APPROXIMATE_NUMBER_OF_MESSAGES,
                QueueAttributeName::APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED,
            ],
        ])->getAttributes();

        return (int) ($attributes[QueueAttributeName::APPROXIMATE_NUMBER_OF_MESSAGES] ?? 0)
            + (int) ($attributes[QueueAttributeName::APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED] ?? 0);
    }

    /**
     * PurgeQueue deletes everything and returns no count, so the figure
     * reported is the estimate taken immediately beforehand. AWS also
     * rate-limits this to once per 60 seconds per queue and may take up
     * to 60 seconds to finish, during which messages sent meanwhile can
     * also be deleted.
     */
    #[\Override]
    public function clear(string $queue = 'default'): int
    {
        $size = $this->size($queue);
        $this->client->purgeQueue(['QueueUrl' => $this->resolveQueueUrl($queue)]);

        return $size;
    }

    private function receiveFrom(string $queue, int $waitTimeSeconds): ?QueuedJob
    {
        $result = $this->client->receiveMessage([
            'QueueUrl' => $this->resolveQueueUrl($queue),
            'MaxNumberOfMessages' => 1,
            'WaitTimeSeconds' => $waitTimeSeconds,
            'AttributeNames' => [MessageSystemAttributeName::APPROXIMATE_RECEIVE_COUNT],
            'MessageAttributeNames' => [self::MAX_ATTEMPTS_ATTRIBUTE, self::METADATA_ATTRIBUTE],
        ]);

        $messages = $result->getMessages();

        if ($messages === []) {
            return null;
        }

        $message = $messages[0];
        $messageAttributes = $message->getMessageAttributes();

        $maxAttempts = isset($messageAttributes[self::MAX_ATTEMPTS_ATTRIBUTE])
            ? (int) $messageAttributes[self::MAX_ATTEMPTS_ATTRIBUTE]->getStringValue()
            : null;

        $receiveCount = $message->getAttributes()[MessageSystemAttributeName::APPROXIMATE_RECEIVE_COUNT] ?? '1';

        /** @var array{class: class-string<Job>, args: array<string, mixed>} $decoded */
        $decoded = json_decode((string) $message->getBody(), true, flags: JSON_THROW_ON_ERROR);

        /** @var array<string, string> $metadata */
        $metadata = isset($messageAttributes[self::METADATA_ATTRIBUTE])
            ? json_decode((string) $messageAttributes[self::METADATA_ATTRIBUTE]->getStringValue(), true, flags: JSON_THROW_ON_ERROR)
            : [];

        return new QueuedJob(
            $decoded['class'],
            $decoded['args'],
            handle: (string) $message->getReceiptHandle(),
            queue: $queue,
            attempts: (int) $receiveCount,
            maxAttempts: $maxAttempts,
            metadata: $metadata,
        );
    }

    private function resolveQueueUrl(string $queue): string
    {
        if (isset($this->queueUrlsByName[$queue])) {
            return $this->queueUrlsByName[$queue];
        }

        $url = $this->client->getQueueUrl(['QueueName' => $this->queueNamePrefix . $queue])->getQueueUrl()
            ?? throw SqsQueueException::noQueueUrlReturned($queue);

        return $this->queueUrlsByName[$queue] = $url;
    }
}
