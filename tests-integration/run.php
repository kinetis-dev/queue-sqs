<?php

declare(strict_types=1);

/**
 * Real-backend regression coverage for SqsQueue — push/pop/ack/release/
 * fail, attempt counting via SQS's own ApproximateReceiveCount, maxAttempts
 * round-tripping through a message attribute, and priority-queue
 * fallthrough — against a real LocalStack SQS endpoint. The queue itself
 * is never auto-created by SqsQueue (a deliberate design choice, not a
 * setup race to work around), so this script creates it directly first.
 */

require __DIR__ . '/../vendor/autoload.php';

use Kinetis\Config\Config;
use Kinetis\Queue\Job;
use Kinetis\QueueSqs\SqsClientFactory;
use Kinetis\QueueSqs\SqsQueue;

function check(string $label, bool $condition): void
{
    echo ($condition ? "OK   " : "FAIL ") . $label . "\n";

    if (!$condition) {
        exit(1);
    }
}

final readonly class SqsIntegrationTestJob implements Job
{
    public function __construct(
        public string $message,
    ) {}

    public function handle(): void
    {
    }
}

$config = new Config([
    'QUEUE_SQS_REGION' => getenv('AWS_REGION') ?: 'us-east-1',
    'QUEUE_SQS_ENDPOINT' => getenv('LOCALSTACK_ENDPOINT') ?: 'http://127.0.0.1:4566',
]);

$client = SqsClientFactory::fromConfig($config);

foreach (['default', 'high'] as $queueName) {
    $client->createQueue(['QueueName' => $queueName])->resolve();
}

$queue = new SqsQueue($client);

$queue->push(new SqsIntegrationTestJob('hello'));
$popped = $queue->pop(timeoutSeconds: 10);
check('pop() returns the pushed job', $popped !== null);
check('job data round-trips correctly', $popped?->args['message'] === 'hello');
check('attempts is 1 on first pop', $popped?->attempts === 1);

$queue->ack($popped);
check('nothing left after ack()', $queue->pop(timeoutSeconds: 3) === null);

// release() makes the job available again, with attempts incremented.
$queue->push(new SqsIntegrationTestJob('retry-me'), maxAttempts: 3);
$first = $queue->pop(timeoutSeconds: 10);
$queue->release($first);
$second = $queue->pop(timeoutSeconds: 10);
check('released job comes back with attempts incremented', $second?->attempts === 2);
check('maxAttempts round-trips through the message attribute', $second?->maxAttempts === 3);
$queue->ack($second);

// fail() removes the job permanently.
$queue->push(new SqsIntegrationTestJob('doomed'));
$doomed = $queue->pop(timeoutSeconds: 10);
$queue->fail($doomed);
check('nothing left after fail()', $queue->pop(timeoutSeconds: 3) === null);

// A job pushed with no maxAttempts comes back null, not some default.
$queue->push(new SqsIntegrationTestJob('no-max-attempts'));
$noMax = $queue->pop(timeoutSeconds: 10);
check('a job with no maxAttempts comes back null', $noMax?->maxAttempts === null);
$queue->ack($noMax);

// Priority queues: the higher-priority queue is checked first.
$queue->push(new SqsIntegrationTestJob('low-priority'), queue: 'default');
$queue->push(new SqsIntegrationTestJob('high-priority'), queue: 'high');

$priorityPop = $queue->pop(timeoutSeconds: 10, queues: ['high', 'default']);
check('the high-priority queue is checked first', $priorityPop?->args['message'] === 'high-priority');
$queue->ack($priorityPop);

$remaining = $queue->pop(timeoutSeconds: 10, queues: ['high', 'default']);
check('falls through to the default queue next', $remaining?->args['message'] === 'low-priority');
$queue->ack($remaining);

echo "ALL CHECKS PASSED\n";
