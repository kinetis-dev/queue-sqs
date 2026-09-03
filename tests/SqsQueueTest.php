<?php

declare(strict_types=1);

namespace Kinetis\QueueSqs\Tests;

use InvalidArgumentException;
use Kinetis\Config\Config;
use Kinetis\Queue\Exception\InvalidDelaySecondsException;
use Kinetis\Queue\Exception\InvalidMaxAttemptsException;
use Kinetis\Queue\Exception\InvalidQueueNameException;
use Kinetis\Queue\Exception\MalformedQueuedJobDataException;
use Kinetis\Queue\Job;
use Kinetis\QueueSqs\SqsClientFactory;
use Kinetis\QueueSqs\SqsQueue;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;

/**
 * Queue-name validation only — SqsQueue's own backend-specific
 * correctness (a real GetQueueUrl round trip, priority cycling, a real
 * delay) is deliberately never unit-tested against a fake, matching this
 * package's established "swap the storage, not the whole system, and
 * don't fake what a real backend has to prove" discipline —
 * real-backend verification lives in tests-integration/ against
 * LocalStack instead. This one check is pure PHP validation that throws
 * before resolveQueueUrl() ever calls the client, so a real queue has
 * nothing to prove that a fast unit test can't already prove faster.
 *
 * SqsClientFactory::fromConfig() never makes a network call — confirmed
 * by SqsClientFactoryTest's own existing tests, which already construct
 * a real SqsClient with just a region and no reachable AWS endpoint — so
 * an SqsClient built this way is safe to construct and pass to SqsQueue
 * here with no real queue required.
 */
final class SqsQueueTest extends TestCase
{
    private function neverConnectedQueue(): SqsQueue
    {
        return new SqsQueue(SqsClientFactory::fromConfig(new Config(['QUEUE_SQS_REGION' => 'us-east-1'])));
    }

    public function test_size_rejects_an_empty_queue_name_before_ever_resolving_a_queue_url(): void
    {
        $queue = $this->neverConnectedQueue();

        $this->expectException(InvalidQueueNameException::class);
        $queue->size('');
    }

    public function test_size_rejects_a_malformed_queue_name_before_ever_resolving_a_queue_url(): void
    {
        $queue = $this->neverConnectedQueue();

        $this->expectException(InvalidQueueNameException::class);
        $queue->size('has spaces');
    }

    public function test_clear_rejects_an_empty_queue_name_before_ever_resolving_a_queue_url(): void
    {
        $queue = $this->neverConnectedQueue();

        $this->expectException(InvalidQueueNameException::class);
        $queue->clear('');
    }

    public function test_a_queue_name_prefix_that_pushes_the_resolved_name_over_the_cap_is_rejected(): void
    {
        $queue = new SqsQueue(
            SqsClientFactory::fromConfig(new Config(['QUEUE_SQS_REGION' => 'us-east-1'])),
            queueNamePrefix: str_repeat('a', 75),
        );

        $this->expectException(InvalidQueueNameException::class);
        $this->expectExceptionMessage('over the 80-character limit');
        $queue->size(str_repeat('b', 10));
    }

    public function test_push_rejects_a_negative_delay_before_ever_resolving_a_queue_url(): void
    {
        $queue = $this->neverConnectedQueue();

        $this->expectException(InvalidDelaySecondsException::class);
        $queue->push(new class implements Job {}, delaySeconds: -1);
    }

    public function test_push_rejects_a_negative_max_attempts_before_ever_resolving_a_queue_url(): void
    {
        $queue = $this->neverConnectedQueue();

        $this->expectException(InvalidMaxAttemptsException::class);
        $queue->push(new class implements Job {}, maxAttempts: -1);
    }

    /**
     * push()'s own $delaySeconds > 900 check comes strictly after the
     * shared floor check both here prove — this confirms the shared
     * check runs first rather than being bypassed by SQS's own
     * independent upper bound.
     */
    public function test_push_still_rejects_a_delay_over_the_sqs_cap(): void
    {
        $queue = $this->neverConnectedQueue();

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('cannot delay a message by more than 900');
        $queue->push(new class implements Job {}, delaySeconds: 901);
    }

    /**
     * @return list<array{mixed}>
     */
    public static function malformedStoredIntegers(): array
    {
        return [
            'non-numeric garbage' => ['garbage'],
            'a float-looking string' => ['5.0'],
            'an empty string' => [''],
        ];
    }

    /**
     * buildQueuedJob() is where a corrupted SQS message attribute's
     * malformed value is actually caught — proven directly with hand-built
     * strings (no real SQS round trip needed, since this method was
     * extracted specifically to make that possible), so the wiring
     * between it and QueueContract::coerceStoredInteger() is exercised
     * too, not just coerceStoredInteger()'s own unit-level behavior.
     */
    #[DataProvider('malformedStoredIntegers')]
    public function test_build_queued_job_rejects_a_non_numeric_stored_max_attempts_value(mixed $raw): void
    {
        $buildQueuedJob = new ReflectionMethod(SqsQueue::class, 'buildQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"maxAttempts"');
        $buildQueuedJob->invoke(
            null,
            'default',
            '{"class":"Fixture\\\\Job","args":[]}',
            'receipt-handle',
            (string) $raw,
            '1',
            null,
        );
    }

    #[DataProvider('malformedStoredIntegers')]
    public function test_build_queued_job_rejects_a_non_numeric_stored_receive_count(mixed $raw): void
    {
        $buildQueuedJob = new ReflectionMethod(SqsQueue::class, 'buildQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"ApproximateReceiveCount"');
        $buildQueuedJob->invoke(
            null,
            'default',
            '{"class":"Fixture\\\\Job","args":[]}',
            'receipt-handle',
            null,
            (string) $raw,
            null,
        );
    }

    public function test_build_queued_job_rejects_a_body_that_is_not_valid_json(): void
    {
        $buildQueuedJob = new ReflectionMethod(SqsQueue::class, 'buildQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"body"');
        $buildQueuedJob->invoke(null, 'default', '{not valid json', 'receipt-handle', null, '1', null);
    }

    public function test_build_queued_job_rejects_a_body_missing_the_class_field(): void
    {
        $buildQueuedJob = new ReflectionMethod(SqsQueue::class, 'buildQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"class"');
        $buildQueuedJob->invoke(null, 'default', '{"args":[]}', 'receipt-handle', null, '1', null);
    }

    public function test_build_queued_job_rejects_a_body_whose_args_field_is_not_an_array(): void
    {
        $buildQueuedJob = new ReflectionMethod(SqsQueue::class, 'buildQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"args"');
        $buildQueuedJob->invoke(
            null,
            'default',
            '{"class":"Fixture\\\\Job","args":"not an array"}',
            'receipt-handle',
            null,
            '1',
            null,
        );
    }

    /**
     * A JSON *list* args value ("args": ["value"], no object keys) is a
     * real, distinct malformed shape from "not an array at all" above —
     * is_array() alone would have accepted it. Confirming it throws
     * MalformedQueuedJobDataException here, from buildQueuedJob() itself
     * (the exact function receiveFrom() wraps in
     * QueueContract::settleIfMalformed()), is what proves this reaches
     * the settle-and-remove path rather than QueueWorker's ordinary
     * job-execution failure handling.
     */
    public function test_build_queued_job_rejects_a_body_whose_args_field_is_a_json_list(): void
    {
        $buildQueuedJob = new ReflectionMethod(SqsQueue::class, 'buildQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"args"');
        $buildQueuedJob->invoke(
            null,
            'default',
            '{"class":"Fixture\\\\Job","args":["positional value"]}',
            'receipt-handle',
            null,
            '1',
            null,
        );
    }

    public function test_build_queued_job_rejects_a_metadata_attribute_that_is_not_a_string_to_string_map(): void
    {
        $buildQueuedJob = new ReflectionMethod(SqsQueue::class, 'buildQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"metadata"');
        $buildQueuedJob->invoke(
            null,
            'default',
            '{"class":"Fixture\\\\Job","args":[]}',
            'receipt-handle',
            null,
            '1',
            '["not","a","map"]',
        );
    }

    /**
     * ApproximateReceiveCount is explicitly requested on every
     * ReceiveMessage call as this backend's own required attempt
     * counter — its genuine absence from the response is a malformed
     * message, not evidence of a first attempt. A prior version of this
     * code defaulted a missing count to "1" (silently treating a
     * corrupted or non-conformant response as an ordinary first
     * attempt); this proves the real, wired decode path now rejects the
     * absence outright instead.
     */
    public function test_build_queued_job_rejects_a_missing_receive_count(): void
    {
        $buildQueuedJob = new ReflectionMethod(SqsQueue::class, 'buildQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"ApproximateReceiveCount"');
        $this->expectExceptionMessage('missing entirely');
        $buildQueuedJob->invoke(
            null,
            'default',
            '{"class":"Fixture\\\\Job","args":[]}',
            'receipt-handle',
            null,
            null,
            null,
        );
    }
}
