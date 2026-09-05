<?php

declare(strict_types=1);

namespace Kinetis\QueueSqs\Tests;

use AsyncAws\Core\Credentials\NullProvider;
use AsyncAws\Sqs\SqsClient;
use InvalidArgumentException;
use Kinetis\Config\Config;
use Kinetis\Queue\ClearableQueueInterface;
use Kinetis\Queue\Exception\InvalidDelaySecondsException;
use Kinetis\Queue\Exception\InvalidMaxAttemptsException;
use Kinetis\Queue\Exception\InvalidQueueNameException;
use Kinetis\Queue\Exception\MalformedQueuedJobDataException;
use Kinetis\Console\CommandArguments;
use Kinetis\Queue\Console\ClearCommand;
use Kinetis\Queue\Job;
use Kinetis\Queue\QueuedJob;
use Kinetis\Queue\QueueInterface;
use Kinetis\QueueSqs\SqsClientFactory;
use Kinetis\QueueSqs\SqsQueue;
use Kinetis\QueueSqs\Tests\Fixtures\RecordedJob;
use Kinetis\QueueSqs\Tests\Fixtures\RecordingSqsTransport;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;

/**
 * Queue-name validation and this backend's declared capabilities —
 * SqsQueue's own backend-specific correctness (a real GetQueueUrl round
 * trip, priority cycling, a real delay) is never unit-tested against a
 * fake, matching this package's established "swap the storage, not the
 * whole system, and don't fake what a real backend has to prove"
 * discipline — real-backend verification lives in tests-integration/
 * against LocalStack instead. The name checks here are pure PHP
 * validation that throws before resolveQueueUrl() ever calls the client,
 * so a real queue has nothing to prove that a fast unit test can't
 * already prove faster.
 *
 * SqsClientFactory::fromConfig() never makes a network call — confirmed
 * by SqsClientFactoryTest's own existing tests, which already construct
 * a real SqsClient with just a region and no reachable AWS endpoint — so
 * an SqsClient built this way is safe to construct and pass to SqsQueue
 * here with no real queue required.
 *
 * The clear-capability checks are the other kind of thing a unit test
 * can settle outright. Whether this backend declares
 * Kinetis\Queue\ClearableQueueInterface is a property of the class as
 * written; whether it ever asks SQS to purge a queue is answered by
 * driving its whole public surface against a recording transport (see
 * RecordingSqsTransport) and reading back the operations that reached
 * the wire — behavior a caller could observe, rather than a scan of
 * this package's own source.
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

    public function test_the_backend_is_a_queue_but_not_a_clearable_one(): void
    {
        $queue = $this->neverConnectedQueue();

        self::assertInstanceOf(QueueInterface::class, $queue);
        self::assertNotInstanceOf(ClearableQueueInterface::class, $queue);
    }

    public function test_the_backend_exposes_no_clear_method_at_all(): void
    {
        // Not only "the interface is absent": a public clear() on the
        // concrete class is callable whether or not the interface names
        // it, so the method itself has to be gone.
        self::assertFalse(method_exists(SqsQueue::class, 'clear'));
    }

    /**
     * The generic command holds a QueueInterface and decides from the
     * capability alone, so an operator pointed at SQS is told which
     * backend cannot clear before a single request leaves the process —
     * the transport recorded nothing, and --force would not change that.
     */
    public function test_the_clear_command_refuses_this_backend_before_any_sqs_request(): void
    {
        $transport = new RecordingSqsTransport();
        $queue = new SqsQueue(new SqsClient(['region' => 'us-east-1'], new NullProvider(), $transport->client()));

        $output = fopen('php://memory', 'r+');
        self::assertIsResource($output);

        $code = new ClearCommand($queue, $output)->run(CommandArguments::parse(['--queue=default', '--force']));
        rewind($output);
        $reported = (string) stream_get_contents($output);

        self::assertSame(1, $code);
        self::assertStringContainsString(SqsQueue::class, $reported);
        self::assertStringContainsString(ClearableQueueInterface::class, $reported);
        self::assertSame([], $transport->operations);
    }

    /**
     * Every operation this backend performs, recorded off the wire —
     * push, size, pop, and each of the three settlements — with
     * PurgeQueue absent from the result. A backend that added a
     * purge on any of these paths would show it here as an extra
     * recorded operation, which is what an assertion over the whole
     * exchange proves and a check of any single call could not.
     *
     * Each settlement gets its own delivery, with its own receipt: SQS
     * scopes a ReceiptHandle to the receive that produced it, so acking,
     * releasing and failing one receipt is an exchange no real queue
     * would accept and would prove nothing about three settlements that
     * each have to work.
     */
    public function test_the_backend_asks_sqs_to_do_only_what_the_queue_contract_needs(): void
    {
        $transport = new RecordingSqsTransport([
            'GetQueueUrl' => self::queueUrlResponse(),
            'SendMessage' => self::sentResponse(),
            'GetQueueAttributes' => self::attributesResponse(waiting: 3, delayed: 1),
            'ReceiveMessage' => [
                self::receivedResponse('receipt-ack'),
                self::receivedResponse('receipt-release'),
                self::receivedResponse('receipt-fail'),
            ],
        ]);

        $queue = new SqsQueue(new SqsClient(['region' => 'us-east-1'], new NullProvider(), $transport->client()));

        $queue->push(new RecordedJob());
        self::assertSame(4, $queue->size());

        $queue->ack(self::popped($queue, 'receipt-ack'));
        $queue->release(self::popped($queue, 'receipt-release'));
        $queue->fail(self::popped($queue, 'receipt-fail'));

        self::assertSame(
            [
                'GetQueueUrl',
                'SendMessage',
                'GetQueueAttributes',
                'ReceiveMessage',
                'DeleteMessage',
                'ReceiveMessage',
                'ChangeMessageVisibility',
                'ReceiveMessage',
                'DeleteMessage',
            ],
            $transport->operations,
            'the queue URL is resolved once per instance and every other call maps onto one queue operation',
        );
        self::assertNotContains('PurgeQueue', $transport->operations);
    }

    /**
     * The four states a queue holds at once, and what each says about
     * emptying it:
     *
     * - reserved — a worker has it in flight and a settlement still to
     *   make. PurgeQueue deletes it anyway.
     * - waiting and delayed — the jobs a clear() would be *asked* to
     *   discard, and the only ones size() counts.
     * - pushed concurrently — sent while a purge is still running, and
     *   deleted by it for up to 60 seconds after the call returns.
     *
     * So the count this backend can report and the work a purge destroys
     * are two different sets, in both directions. That gap is why there
     * is no clear() here to return a number for: nothing this backend
     * can say would be true of what happened.
     */
    public function test_size_reports_only_waiting_work_while_a_purge_would_destroy_more(): void
    {
        $transport = new RecordingSqsTransport([
            'GetQueueUrl' => self::queueUrlResponse(),
            'SendMessage' => self::sentResponse(),
            'ReceiveMessage' => self::receivedResponse('receipt-in-flight'),
            'GetQueueAttributes' => self::attributesResponse(waiting: 1, delayed: 1),
        ]);

        $queue = new SqsQueue(new SqsClient(['region' => 'us-east-1'], new NullProvider(), $transport->client()));

        $queue->push(new RecordedJob());
        $queue->push(new RecordedJob(), delaySeconds: 60);

        $reserved = $queue->pop(0);
        self::assertNotNull($reserved);
        self::assertSame('receipt-in-flight', $reserved->handle);

        // Sent after the reserve, the way a producer keeps pushing
        // through a purge's own window.
        $queue->push(new RecordedJob());

        // One waiting plus one delayed. The reserved delivery is another
        // worker's, and the concurrent push is not counted here either —
        // ApproximateNumberOfMessages is an estimate, which is the whole
        // reason this number can never stand in for "how many were
        // destroyed".
        self::assertSame(2, $queue->size());

        self::assertSame(
            [
                'GetQueueUrl',
                'SendMessage',
                'SendMessage',
                'ReceiveMessage',
                'SendMessage',
                'GetQueueAttributes',
            ],
            $transport->operations,
            'nothing here empties a queue, and there is no operation available that could',
        );
    }

    private static function queueUrlResponse(): string
    {
        return self::json(['QueueUrl' => 'https://sqs.us-east-1.amazonaws.com/123456789012/default']);
    }

    private static function sentResponse(): string
    {
        return self::json(['MessageId' => 'm-1', 'MD5OfMessageBody' => 'd41d8cd98f00b204e9800998ecf8427e']);
    }

    private static function attributesResponse(int $waiting, int $delayed): string
    {
        return self::json(['Attributes' => [
            'ApproximateNumberOfMessages' => (string) $waiting,
            'ApproximateNumberOfMessagesDelayed' => (string) $delayed,
        ]]);
    }

    private static function receivedResponse(string $receiptHandle): string
    {
        return self::json(['Messages' => [[
            'MessageId' => 'm-' . $receiptHandle,
            'ReceiptHandle' => $receiptHandle,
            'Body' => self::json(['class' => RecordedJob::class, 'args' => []]),
            'Attributes' => ['ApproximateReceiveCount' => '1'],
        ]]]);
    }

    /**
     * @param array<string, mixed> $value
     */
    private static function json(array $value): string
    {
        return json_encode($value, JSON_THROW_ON_ERROR);
    }

    private static function popped(SqsQueue $queue, string $expectedHandle): QueuedJob
    {
        $job = $queue->pop(1);

        self::assertNotNull($job);
        self::assertSame($expectedHandle, $job->handle);

        return $job;
    }
}
