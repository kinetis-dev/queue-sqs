<?php

declare(strict_types=1);

namespace Kinetis\QueueSqs;

use Kinetis\Instrumentation\Telemetry;
use AsyncAws\Sqs\Enum\MessageSystemAttributeName;
use AsyncAws\Sqs\Enum\QueueAttributeName;
use AsyncAws\Sqs\SqsClient;
use InvalidArgumentException;
use Kinetis\Queue\Exception\InvalidQueueNameException;
use Kinetis\Queue\Exception\MalformedQueuedJobDataException;
use Kinetis\Queue\Job;
use Kinetis\Queue\JobSerializer;
use Kinetis\Queue\QueueContract;
use Kinetis\Queue\QueueInterface;
use Kinetis\Queue\QueuedJob;
use Kinetis\Queue\Support\PopSweep;
use Kinetis\QueueSqs\Exception\SqsQueueException;
use LogicException;
use Throwable;

/**
 * SQS already solves what Kinetis\QueueRedis\RedisQueue and Kinetis\QueueSql\SqlQueue
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
 * pop()'s whole priority/timeout algorithm is Kinetis\Queue\Support\PopSweep
 * — see that class and QueueInterface's own docblock for the full
 * cross-backend contract. This class supplies exactly one thing PopSweep
 * needs: probe(), a single-queue check that can spend up to a given wait
 * budget — receiveFrom() underneath, with the wait budget mapped
 * straight onto ReceiveMessage's own WaitTimeSeconds (capped at 20
 * seconds, SQS's own hard limit).
 *
 * Unlike Redis's BRPOPLPUSH (where a literal 0 timeout means "block
 * forever"), SQS's WaitTimeSeconds: 0 genuinely means an immediate,
 * non-blocking call — exactly PopSweep's own zero-wait, immediate-sweep
 * meaning, so probe() maps a sub-one-second remaining budget (WaitTimeSeconds
 * is an integer; SQS has no fractional long-poll) onto that same 0
 * rather than rounding up (materially overshooting the deadline) or down
 * to a real wait that never happens. No Kinetis\Async\Timer::delay()
 * between attempts the way SqlQueue needs (SQL has no blocking-wait
 * primitive at all), and no concurrently() wrapper either: the injected
 * AmpHttpClient transport tolerates being called from plain top-level
 * code without an existing Fiber. Standard SQS queues only — FIFO queues
 * (the `.fifo` suffix, requiring MessageGroupId on every send) are not
 * supported.
 */
final class SqsQueue implements QueueInterface
{
    private const MAX_DELAY_SECONDS = 900;

    private const MAX_WAIT_TIME_SECONDS = 20;

    private const PER_QUEUE_WAIT_TIME_SECONDS = 5;

    private const MAX_ATTEMPTS_ATTRIBUTE = 'maxAttempts';

    private const METADATA_ATTRIBUTE = 'metadata';

    /**
     * Amazon SQS's own real cap on the resolved queue name it actually
     * receives — $queueNamePrefix and a caller-supplied queue name are
     * each individually validated against QueueContract's own 80-character
     * grammar, but concatenating two individually-valid strings can still
     * exceed this once combined.
     */
    private const int MAX_RESOLVED_NAME_LENGTH = 80;

    /** @var array<string, string> */
    private array $queueUrlsByName = [];

    public function __construct(
        private readonly SqsClient $client,
        private readonly string $queueNamePrefix = '',
    ) {
        QueueContract::assertValidQueueNamePrefix($queueNamePrefix);
    }

    #[\Override]
    public function push(Job $job, int $delaySeconds = 0, string $queue = 'default', ?int $maxAttempts = null): void
    {
        QueueContract::assertValidPushArguments($delaySeconds, $queue, $maxAttempts);

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
                // PRESERVE_ZERO_FRACTION: without it, an integral-valued
                // float argument (4.0) encodes as "4" and decodes back
                // as an int — a silent type change JobSerializer's own
                // portable-value contract promises never happens.
                'MessageBody' => json_encode($serialized, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION),
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
                    'StringValue' => json_encode($metadata, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION),
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
        // PopSweep::run() itself validates $timeoutSeconds/$queues via
        // QueueContract before touching either — see that class's own
        // docblock for why it doesn't trust a caller to have already
        // done so.
        return PopSweep::run(
            timeoutSeconds: $timeoutSeconds,
            queues: $queues,
            probe: function (string $queue, float $waitSeconds): ?QueuedJob {
                // WaitTimeSeconds: 0 is a genuine, correct immediate,
                // non-blocking ReceiveMessage on SQS (unlike Redis's
                // BRPOPLPUSH, where a literal 0 timeout means "block
                // forever") — so PopSweep's own zero-wait immediate
                // sweep, and a sub-one-second remaining budget (SQS's
                // WaitTimeSeconds has no fractional form), both map onto
                // it directly rather than needing a separate
                // non-blocking primitive the way Redis does.
                $waitTimeSeconds = $waitSeconds < 1.0 ? 0 : min(self::MAX_WAIT_TIME_SECONDS, (int) floor($waitSeconds));

                return $this->receiveFrom($queue, $waitTimeSeconds);
            },
            probeCanBlock: true,
            waitCapSeconds: (float) self::PER_QUEUE_WAIT_TIME_SECONDS,
            sleep: static function (): never {
                throw new LogicException('SqsQueue never paces via sleep() — every probe either long-polls natively or is instant.');
            },
        );
    }

    #[\Override]
    public function ack(QueuedJob $job): void
    {
        $this->deleteMessage($job->queue, (string) $job->handle);
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
        $this->deleteMessage($job->queue, (string) $job->handle);
    }

    /**
     * Shared by ack()/fail() (a real QueuedJob's own queue/handle) and
     * the malformed-message settlement path in receiveFrom() (the raw
     * queue/receipt handle a decode failure was caught for, with no
     * QueuedJob to read them off of) — the same DeleteMessage either way,
     * just reached from two different starting shapes.
     */
    private function deleteMessage(string $queue, string $receiptHandle): void
    {
        $this->client->deleteMessage([
            'QueueUrl' => $this->resolveQueueUrl($queue),
            'ReceiptHandle' => $receiptHandle,
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
        $receiptHandle = (string) $message->getReceiptHandle();

        return QueueContract::settleIfMalformed(
            $queue,
            fn (): QueuedJob => self::buildQueuedJob(
                queue: $queue,
                body: (string) $message->getBody(),
                receiptHandle: $receiptHandle,
                rawMaxAttempts: isset($messageAttributes[self::MAX_ATTEMPTS_ATTRIBUTE])
                    ? $messageAttributes[self::MAX_ATTEMPTS_ATTRIBUTE]->getStringValue()
                    : null,
                rawReceiveCount: $message->getAttributes()[MessageSystemAttributeName::APPROXIMATE_RECEIVE_COUNT] ?? null,
                rawMetadata: isset($messageAttributes[self::METADATA_ATTRIBUTE])
                    ? $messageAttributes[self::METADATA_ATTRIBUTE]->getStringValue()
                    : null,
            ),
            fn () => $this->deleteMessage($queue, $receiptHandle),
        );
    }

    /**
     * Extracted out of receiveFrom() and taking plain scalars rather than
     * AsyncAws's own Message/MessageAttributeValue objects specifically so
     * it's independently testable with hand-built strings, no real SQS
     * round trip needed. Every field is read through one of QueueContract's
     * own coercion helpers rather than trusted at a PHPStan-asserted @var
     * shape — $body might not even be valid JSON, or might decode to
     * something other than a {class, args} object; $rawMaxAttempts is read
     * through QueueContract::coerceStoredMaxAttempts() rather than a lossy
     * `(int)` cast — every SQS attribute value here is *already* a string
     * by the API's own design ("Number" is a DataType label, not a
     * distinct wire type), so a non-numeric one cast that way would
     * silently become 0 instead of surfacing the corruption it actually
     * represents. $rawReceiveCount is nullable and *not* defaulted to "1"
     * here or by receiveFrom(): ApproximateReceiveCount is explicitly
     * requested on every ReceiveMessage call as this backend's own
     * required attempt counter, so its genuine absence from the response
     * is a malformed message, not evidence of a first attempt — treating
     * it as "1" would silently accept a corrupted or non-conformant
     * response as ordinary. Read through coerceStoredAttempts(), not
     * coerceStoredCompletedAttempts() (used by RedisQueue's/SqlQueue's/
     * RabbitMqQueue's own equivalent decode methods): unlike those three,
     * this value is never incremented — SQS's own ApproximateReceiveCount
     * is already 1-indexed — so the bound checked is QueuedJob's own
     * floor (>= 1) directly, not "must not be negative" (>= 0). Every
     * failure here is caught by receiveFrom() — see
     * QueueContract::settleIfMalformed() — so a malformed message settles
     * the already-reserved receive rather than crashing the worker.
     */
    private static function buildQueuedJob(
        string $queue,
        string $body,
        string $receiptHandle,
        ?string $rawMaxAttempts,
        ?string $rawReceiveCount,
        ?string $rawMetadata,
    ): QueuedJob {
        $decoded = QueueContract::coerceStoredJsonArray($body, 'body');

        $class = QueueContract::coerceStoredClass($decoded['class'] ?? null);
        $args = QueueContract::coerceStoredArgs($decoded['args'] ?? null);
        $metadata = QueueContract::coerceStoredMetadata($rawMetadata);

        if ($rawReceiveCount === null) {
            throw MalformedQueuedJobDataException::missingField('ApproximateReceiveCount');
        }

        return new QueuedJob(
            $class,
            $args,
            handle: $receiptHandle,
            queue: $queue,
            attempts: QueueContract::coerceStoredAttempts($rawReceiveCount, 'ApproximateReceiveCount'),
            maxAttempts: QueueContract::coerceStoredMaxAttempts($rawMaxAttempts, 'maxAttempts'),
            metadata: $metadata,
        );
    }

    /**
     * The one choke point every real SQS operation (push, pop's own
     * probe, size, clear) reaches this backend's storage through —
     * validating $queue here, not separately in each of those, is what
     * closes size()/clear() having had no validation of their own at
     * all. push()/pop() each also validate independently, ahead of this
     * (push() before building anything else; pop() via PopSweep before
     * touching any queue) — redundant for those two specifically, but
     * this is the one call every path actually shares.
     */
    private function resolveQueueUrl(string $queue): string
    {
        QueueContract::assertValidQueueName($queue);

        if (isset($this->queueUrlsByName[$queue])) {
            return $this->queueUrlsByName[$queue];
        }

        $resolvedName = $this->queueNamePrefix . $queue;

        if (\strlen($resolvedName) > self::MAX_RESOLVED_NAME_LENGTH) {
            throw InvalidQueueNameException::resolvedNameTooLong($resolvedName, self::MAX_RESOLVED_NAME_LENGTH);
        }

        $url = $this->client->getQueueUrl(['QueueName' => $resolvedName])->getQueueUrl()
            ?? throw SqsQueueException::noQueueUrlReturned($queue);

        return $this->queueUrlsByName[$queue] = $url;
    }
}
