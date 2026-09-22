<?php

declare(strict_types=1);

namespace Kinetis\QueueSqs;

use Kinetis\Instrumentation\Telemetry;
use AsyncAws\Sqs\Enum\MessageSystemAttributeName;
use AsyncAws\Sqs\Enum\QueueAttributeName;
use AsyncAws\Sqs\SqsClient;
use InvalidArgumentException;
use Kinetis\Queue\Exception\InvalidQueueArgumentException;
use Kinetis\Queue\Exception\MalformedQueuedJobDataException;
use Kinetis\Queue\Job;
use Kinetis\Queue\JobSerializer;
use Kinetis\Queue\QueueContract;
use Kinetis\Queue\QueuedJob;
use Kinetis\Queue\RenewableQueueInterface;
use Kinetis\QueueSqs\Exception\SqsQueueException;
use Throwable;

/**
 * SQS provides natively what RedisQueue and SqlQueue each need their own
 * mechanism for: per-message delay (SendMessage's DelaySeconds, capped at
 * 900 seconds — SQS's hard limit, raised against here rather than
 * silently clamped) and at-least-once delivery (a received message stays
 * invisible rather than deleted for its queue's visibility timeout, so
 * ack()/release()/fail() are DeleteMessage and ChangeMessageVisibility
 * calls with no lease set or reserved_at column to maintain).
 *
 * A delayed release() is that same native invisibility: release()'s
 * $delaySeconds is passed straight through as ChangeMessageVisibility's
 * VisibilityTimeout, whose request field accepts 0 to 43200 seconds — a
 * wider limit than DelaySeconds, and this backend raises against it
 * separately. Being in range is not the same as being accepted: SQS
 * refuses a timeout longer than the time left in that received
 * message's own 12-hour maximum, and that refusal propagates. See
 * release().
 *
 * $attempts comes from SQS's own ApproximateReceiveCount system
 * attribute. AWS documents that count as approximate under rare failure
 * conditions — a disclosed imprecision, not an exact counter.
 *
 * $maxAttempts has no native equivalent, so it travels as a custom
 * "maxAttempts" message attribute; absent means null, deferring to the
 * processing worker's own default as on every other backend.
 *
 * Queue names resolve to queue URLs via GetQueueUrl, cached for this
 * instance's lifetime — one instance per worker process, so the cache
 * never outlives one. $queueNamePrefix maps "high"/"default" onto
 * "myapp-high"/"myapp-default" so environments sharing an AWS account do
 * not collide. A queue is never auto-created: provisioning is an
 * infrastructure operation, the same stance SqlQueue takes toward its
 * own table. Standard queues only — FIFO queues, which require
 * MessageGroupId on every send, are not supported.
 *
 * **Every ReceiveMessage carries an explicit VisibilityTimeout**, the
 * $visibilityTimeoutSeconds this instance was built with, so the window
 * a delivery gets is the application's setting rather than whatever the
 * remote queue attribute happens to be — that attribute is overridden
 * per receive. The same value is what renew() writes: this backend
 * declares Kinetis\Queue\RenewableQueueInterface, and QueueWorker
 * extends a delivery at half the window for as long as its handler
 * runs. AWS caps the field at 43200 seconds and counts a message's own
 * 12-hour maximum from the receive, not from the last renewal, so a job
 * running past that limit is redelivered whatever this backend sends —
 * and that refusal propagates as SQS's own error.
 *
 * pop() sweeps every queue with an immediate ReceiveMessage first, then
 * long-polls the highest-priority queue for a bounded slice before
 * sweeping again — see QueueInterface for the contract. WaitTimeSeconds
 * maps straight onto that: 0 is a genuine non-blocking receive on SQS,
 * and the maximum long poll is 20 seconds. The injected AmpHttpClient
 * transport suspends the calling Fiber and tolerates being called from
 * top-level code with no existing Fiber, so no Timer or concurrently()
 * wrapper is needed.
 *
 * Every mutation resolves at its call site. SendMessage, DeleteMessage
 * and ChangeMessageVisibility each answer with a Result whose request is
 * only known to have reached SQS once it is resolved, and each of those
 * three operations reports nothing else a caller reads, so resolve() is
 * what turns a service or network failure into the failure of the queue
 * operation itself rather than of a temporary being destroyed. The read
 * paths resolve through the getters they already call.
 *
 * **Settlements here are unfenced.** QueuedJob::$handle is the message's
 * ReceiptHandle, which SQS scopes to the receive that produced it, but
 * this backend raises no Exception\StaleJobHandleException of its own:
 * whatever SQS answers a settlement with propagates as its own error.
 * A settlement against a delivery whose visibility timeout already
 * expired is therefore not reported as a lost delivery the way
 * RedisQueue and SqlQueue report one.
 *
 * ClearableQueueInterface is not implemented and PurgeQueue is never
 * called: SQS offers no operation matching that contract. PurgeQueue
 * deletes in-flight messages a worker already holds along with waiting
 * ones, keeps deleting messages sent during the up-to-60-second window
 * it takes to finish, reports no count, and is rate-limited to once per
 * minute per queue. Deleting only waiting messages cannot be assembled
 * out of ReceiveMessage/DeleteMessage either, since a delayed message is
 * invisible until its delay elapses. Emptying an SQS queue is an
 * infrastructure operation — `aws sqs purge-queue`, or recreating the
 * queue.
 */
final class SqsQueue implements RenewableQueueInterface
{
    private const MAX_DELAY_SECONDS = 900;

    /**
     * The widest VisibilityTimeout SQS's request field accepts — 0 to
     * 43200 seconds, 12 hours — a different and far wider limit than
     * SendMessage's 900-second DelaySeconds, because it re-times an
     * existing message's invisibility rather than scheduling a new
     * delivery. One field and one cap for all three uses this backend
     * makes of it: the window ReceiveMessage asks for, the window
     * renew() restores, and the delay release() sets. Raised against
     * here rather than silently clamped, the same stance
     * MAX_DELAY_SECONDS takes.
     *
     * A field cap, not a promise that everything under it is accepted:
     * how much of a received message's own 12-hour maximum is left is
     * service state only SQS knows. See release().
     */
    private const int MAX_VISIBILITY_TIMEOUT_SECONDS = 43200;

    /**
     * The narrowest window a worker can be given. 1 rather than SQS's
     * own 0, which means "visible again immediately" — a release, not a
     * reservation a worker could hold and renew. It bounds only the
     * reservation window; release() still admits 0.
     */
    private const int MIN_VISIBILITY_TIMEOUT_SECONDS = 1;

    /**
     * The longest pop() long-polls the highest-priority queue when
     * nothing is waiting anywhere. Well under SQS's own 20-second
     * maximum, so a lower-priority queue is re-checked promptly.
     */
    private const int BLOCK_WAIT_TIME_SECONDS = 5;

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

    /**
     * @param int $visibilityTimeoutSeconds sent on every ReceiveMessage
     *     and written by every renew(), overriding the remote queue's
     *     own attribute for the deliveries this instance takes
     */
    public function __construct(
        private readonly SqsClient $client,
        private readonly string $queueNamePrefix = '',
        private readonly int $visibilityTimeoutSeconds = 300,
    ) {
        QueueContract::assertValidQueueNamePrefix($queueNamePrefix);
        self::assertValidVisibilityTimeout($visibilityTimeoutSeconds);
    }

    /**
     * Exposed so SqsQueueFactory can reject a configured value before it
     * builds a client, and so the admitted range cannot drift between
     * the two call sites — the same reason
     * Kinetis\Queue\QueueWorker exposes its own pre-flight checks.
     */
    public static function assertValidVisibilityTimeout(int $seconds): void
    {
        if ($seconds < self::MIN_VISIBILITY_TIMEOUT_SECONDS || $seconds > self::MAX_VISIBILITY_TIMEOUT_SECONDS) {
            throw new InvalidArgumentException(
                'a renewable SQS delivery needs a visibility timeout between '
                . self::MIN_VISIBILITY_TIMEOUT_SECONDS . ' and ' . self::MAX_VISIBILITY_TIMEOUT_SECONDS
                . " seconds, got {$seconds}.",
            );
        }
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

            $this->client->sendMessage($input)->resolve();
            $telemetry->jobPushEnded($telemetryToken, null);
        } catch (Throwable $e) {
            $telemetry->jobPushEnded($telemetryToken, $e);

            throw $e;
        }
    }

    #[\Override]
    public function pop(int $timeoutSeconds = 0, array $queues = ['default']): ?QueuedJob
    {
        QueueContract::assertValidPopArguments($timeoutSeconds, $queues);

        if ($queues === []) {
            return null;
        }

        $deadline = $timeoutSeconds > 0 ? microtime(true) + $timeoutSeconds : null;

        while (true) {
            foreach ($queues as $queue) {
                $job = $this->receiveFrom($queue, waitTimeSeconds: 0);

                if ($job !== null) {
                    return $job;
                }
            }

            if ($deadline !== null && microtime(true) >= $deadline) {
                return null;
            }

            // Nothing waiting anywhere, so long-poll the highest-priority
            // queue rather than spinning. The lower-priority queues are
            // re-checked on the next sweep. The wait is capped by what is
            // left of the deadline, rounded up to a whole second:
            // WaitTimeSeconds counts whole seconds, and 0 would make the
            // receive non-blocking again.
            $waitTimeSeconds = self::BLOCK_WAIT_TIME_SECONDS;

            if ($deadline !== null) {
                $waitTimeSeconds = max(1, min($waitTimeSeconds, (int) ceil($deadline - microtime(true))));
            }

            $job = $this->receiveFrom($queues[0], $waitTimeSeconds);

            if ($job !== null) {
                return $job;
            }

            // That long poll can consume the rest of the deadline on its
            // own. Rechecking here, rather than only at the top of the
            // next sweep, keeps an expired deadline from reserving a
            // message the caller has already stopped waiting for — one
            // that would then sit invisible until its visibility timeout
            // expired.
            if ($deadline !== null && microtime(true) >= $deadline) {
                return null;
            }
        }
    }

    #[\Override]
    public function ack(QueuedJob $job): void
    {
        $this->deleteMessage($job->queue, (string) $job->handle);
    }

    /**
     * $delaySeconds *is* the new VisibilityTimeout. Nothing is
     * republished and no delay structure is needed — this is the same
     * single request the immediate release already made, carrying the
     * delay the worker asked for. On a call SQS accepts, the new timeout
     * counts from the call, so 0 makes the message visible again at once
     * rather than waiting out its queue's normal visibility timeout.
     *
     * Two separate limits apply, and only the first is this backend's to
     * enforce:
     *
     * - The request field accepts 0 to 43200 seconds. That is a property
     *   of the API, knowable before the call, so an over-range value is
     *   rejected here rather than sent.
     * - SQS additionally refuses a timeout longer than the time left in
     *   this received message's own 12-hour maximum, and documents that
     *   it does not recalculate down to that remaining time. How much is
     *   left is service state this process cannot see, so an in-range
     *   delay is a request SQS may still refuse. That refusal propagates
     *   as SQS's own error, like every other settlement failure here,
     *   with nothing settled — which is why there is no local guess at
     *   the remaining window.
     */
    #[\Override]
    public function release(QueuedJob $job, int $delaySeconds = 0): void
    {
        QueueContract::assertValidReleaseDelay($delaySeconds);

        if ($delaySeconds > self::MAX_VISIBILITY_TIMEOUT_SECONDS) {
            throw new InvalidArgumentException(
                'ChangeMessageVisibility accepts a VisibilityTimeout of at most '
                . self::MAX_VISIBILITY_TIMEOUT_SECONDS . " seconds (requested {$delaySeconds}).",
            );
        }

        $this->client->changeMessageVisibility([
            'QueueUrl' => $this->resolveQueueUrl($job->queue),
            'ReceiptHandle' => (string) $job->handle,
            'VisibilityTimeout' => $delaySeconds,
        ])->resolve();
    }

    #[\Override]
    public function fail(QueuedJob $job): void
    {
        $this->deleteMessage($job->queue, (string) $job->handle);
    }

    #[\Override]
    public function visibilityTimeoutSeconds(): int
    {
        return $this->visibilityTimeoutSeconds;
    }

    /**
     * One ChangeMessageVisibility re-timing this receipt's invisibility
     * to the full configured window, counted from the call. The receipt
     * handle is the fence SQS itself applies; as everywhere else in this
     * backend, whatever SQS answers propagates unchanged and no
     * stale-receipt detection is fabricated locally.
     */
    #[\Override]
    public function renew(QueuedJob $job): void
    {
        $this->client->changeMessageVisibility([
            'QueueUrl' => $this->resolveQueueUrl($job->queue),
            'ReceiptHandle' => (string) $job->handle,
            'VisibilityTimeout' => $this->visibilityTimeoutSeconds,
        ])->resolve();
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
        ])->resolve();
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

    private function receiveFrom(string $queue, int $waitTimeSeconds): ?QueuedJob
    {
        $result = $this->client->receiveMessage([
            'QueueUrl' => $this->resolveQueueUrl($queue),
            'MaxNumberOfMessages' => 1,
            'WaitTimeSeconds' => $waitTimeSeconds,
            'VisibilityTimeout' => $this->visibilityTimeoutSeconds,
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
     * Takes plain scalars rather than AsyncAws's Message objects so it is
     * testable with hand-built strings, no SQS round trip needed.
     *
     * Every field goes through a QueueContract helper. Every SQS
     * attribute value is a string by the API's design ("Number" is a
     * DataType label, not a wire type), so a `(int)` cast would turn a
     * corrupted one into 0 rather than surfacing it. $rawReceiveCount is
     * not defaulted to 1: ApproximateReceiveCount is requested on every
     * ReceiveMessage call as this backend's attempt counter, so its
     * absence is a malformed message rather than evidence of a first
     * attempt. Unlike the backends that store a completed-attempts count,
     * it is already 1-indexed and never incremented, so the floor checked
     * is QueuedJob's own. Every failure here is caught by receiveFrom()
     * through QueueContract::settleIfMalformed(), so a malformed message
     * settles the already-reserved receive instead of crashing the
     * worker.
     */
    private static function buildQueuedJob(
        string $queue,
        string $body,
        string $receiptHandle,
        ?string $rawMaxAttempts,
        ?string $rawReceiveCount,
        ?string $rawMetadata,
    ): QueuedJob {
        $decoded = QueueContract::storedJsonArray($body, 'body');

        $class = QueueContract::storedClass($decoded['class'] ?? null);
        $args = QueueContract::storedArgs($decoded['args'] ?? null);
        $metadata = QueueContract::storedMetadata($rawMetadata);

        if ($rawReceiveCount === null) {
            throw MalformedQueuedJobDataException::missingField('ApproximateReceiveCount');
        }

        return new QueuedJob(
            $class,
            $args,
            handle: $receiptHandle,
            queue: $queue,
            attempts: QueueContract::storedInt($rawReceiveCount, 'ApproximateReceiveCount', 1),
            maxAttempts: QueueContract::storedNullableInt($rawMaxAttempts, 'maxAttempts', 0),
            metadata: $metadata,
        );
    }

    /**
     * The one call every SQS operation reaches storage through, so
     * validating $queue here covers size() as well as push() and pop(),
     * which each validate ahead of this on their own.
     */
    private function resolveQueueUrl(string $queue): string
    {
        QueueContract::assertValidQueueName($queue);

        if (isset($this->queueUrlsByName[$queue])) {
            return $this->queueUrlsByName[$queue];
        }

        $resolvedName = $this->queueNamePrefix . $queue;

        if (\strlen($resolvedName) > self::MAX_RESOLVED_NAME_LENGTH) {
            throw InvalidQueueArgumentException::resolvedNameTooLong($resolvedName, self::MAX_RESOLVED_NAME_LENGTH);
        }

        $url = $this->client->getQueueUrl(['QueueName' => $resolvedName])->getQueueUrl()
            ?? throw SqsQueueException::noQueueUrlReturned($queue);

        return $this->queueUrlsByName[$queue] = $url;
    }
}
