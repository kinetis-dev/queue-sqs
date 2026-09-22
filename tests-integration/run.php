<?php

declare(strict_types=1);

/**
 * Real-backend regression coverage for SqsQueue — push/pop/ack/release/
 * fail, attempt counting via SQS's own ApproximateReceiveCount, maxAttempts
 * round-tripping through a message attribute, reservation renewal, and
 * priority-queue fallthrough — against a real LocalStack SQS endpoint. The queue itself
 * is never auto-created by SqsQueue (a deliberate design choice, not a
 * setup race to work around), so this script creates it directly first.
 */

require __DIR__ . '/../vendor/autoload.php';

use Kinetis\Config\Config;
use Kinetis\Queue\Exception\InvalidQueueArgumentException;
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
    // LocalStack answers plain HTTP, which the factory accepts only when
    // the deployment says so.
    'QUEUE_SQS_PLAINTEXT' => 'true',
]);

$client = SqsClientFactory::fromConfig($config);

foreach (['default', 'high', 'empty-one', 'empty-two', 'empty-three', 'lowest', 'renewal'] as $queueName) {
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

// A delayed release is SQS's own visibility timeout, counted from the
// call: only a real endpoint proves the message is actually held. Three
// seconds on a freshly received message is well inside the 12 hours it
// has left, so this exercises the delay rather than SQS's own
// remaining-time refusal.
$queue->push(new SqsIntegrationTestJob('back-off'), maxAttempts: 3);
$delayed = $queue->pop(timeoutSeconds: 10);
$queue->release($delayed, 3);
check('a delayed release stays invisible while the delay runs', $queue->pop(timeoutSeconds: 1) === null);
$retried = $queue->pop(timeoutSeconds: 15);
check('the delayed job becomes visible once its delay has run', $retried?->args['message'] === 'back-off');
check('the delayed retry still carries its incremented attempt', $retried?->attempts === 2);
$queue->ack($retried);

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

// Reservation renewal against a real endpoint. A created queue carries
// LocalStack's own 30-second default VisibilityTimeout, so the control
// below coming back after eight seconds is also what proves the window
// every ReceiveMessage sends overrides that attribute.
//
// Five seconds rather than two: each LocalStack call carries roughly a
// real second of emulation overhead, so a two-second window would be
// racing that overhead instead of the renewal.
$renewable = new SqsQueue($client, visibilityTimeoutSeconds: 5);

$renewable->push(new SqsIntegrationTestJob('expires-without-renewal'), queue: 'renewal');
$abandoned = $renewable->pop(timeoutSeconds: 10, queues: ['renewal']);
check('the renewal queue delivers its first job', $abandoned?->args['message'] === 'expires-without-renewal');
sleep(8);
$redelivered = $renewable->pop(timeoutSeconds: 10, queues: ['renewal']);
check('an unrenewed delivery is redelivered once the sent window expires', $redelivered !== null);
check('that redelivery is the second receive of the same message', $redelivered?->attempts === 2);
$renewable->ack($redelivered);

// The discriminator: two renewals spanning more than the whole window,
// after which the message must still be invisible. Without renew(), this
// is the control above.
$renewable->push(new SqsIntegrationTestJob('outlives-its-window'), queue: 'renewal');
$held = $renewable->pop(timeoutSeconds: 10, queues: ['renewal']);
check('the job to be renewed was delivered', $held?->args['message'] === 'outlives-its-window');

$receivedAt = microtime(true);

for ($renewals = 0; $renewals < 2; $renewals++) {
    sleep(3);
    $renewable->renew($held);
}

$sinceReceive = microtime(true) - $receivedAt;
check(
    "more than one window has passed since the receive — {$sinceReceive}s against 5s",
    $sinceReceive > 5.0,
);
check(
    'a renewed delivery is still invisible past the window it was received with',
    $renewable->pop(timeoutSeconds: 3, queues: ['renewal']) === null,
);
$renewable->ack($held);
check('nothing is left on the renewal queue', $renewable->pop(timeoutSeconds: 3, queues: ['renewal']) === null);

// Priority queues: the higher-priority queue is checked first.
$queue->push(new SqsIntegrationTestJob('low-priority'), queue: 'default');
$queue->push(new SqsIntegrationTestJob('high-priority'), queue: 'high');

$priorityPop = $queue->pop(timeoutSeconds: 10, queues: ['high', 'default']);
check('the high-priority queue is checked first', $priorityPop?->args['message'] === 'high-priority');
$queue->ack($priorityPop);

$remaining = $queue->pop(timeoutSeconds: 10, queues: ['high', 'default']);
check('falls through to the default queue next', $remaining?->args['message'] === 'low-priority');
$queue->ack($remaining);

// An empty higher-priority queue must never delay finding a job already
// waiting in a lower-priority one. pop()'s immediate WaitTimeSeconds: 0
// sweep of every named queue is what gives that, timed here against a
// real ReceiveMessage rather than asserted from the algorithm alone.
//
// Each queue's URL is resolved (and cached for this SqsQueue instance's
// lifetime) once, warmed up here before timing anything — this
// LocalStack build's own GetQueueUrl/ReceiveMessage calls each carry a
// real, disclosed ~1 real second of emulation overhead regardless of
// WaitTimeSeconds, so timing a cold pop() would measure that one-time
// lookup cost, not the sweep algorithm itself — the same steady-state a
// real worker reaches after its first request against any of these
// queues, not its very first cold moment.
foreach (['empty-one', 'empty-two', 'empty-three', 'lowest'] as $warmQueue) {
    $queue->push(new SqsIntegrationTestJob('warm-up'), queue: $warmQueue);
    $queue->ack($queue->pop(timeoutSeconds: 10, queues: [$warmQueue]));
}

$queue->push(new SqsIntegrationTestJob('found-immediately'), queue: 'lowest');
$start = microtime(true);
$found = $queue->pop(timeoutSeconds: 15, queues: ['empty-one', 'empty-two', 'empty-three', 'lowest']);
$elapsed = microtime(true) - $start;
check(
    'a job in the last of four queues, the first three empty, is still found',
    $found?->args['message'] === 'found-immediately',
);
check(
    // A bound generous enough to absorb this LocalStack build's own
    // real per-call overhead (roughly 1s per ReceiveMessage, warmed URLs
    // included) across all four probes, while still confirming a job in
    // the last queue doesn't cost anywhere near a full per-queue
    // blocking wait (up to 5s) times three empty queues.
    "found across four queues in well under 8 real seconds — took {$elapsed}s",
    $elapsed < 8.0,
);
$queue->ack($found);

try {
    $queue->pop(timeoutSeconds: -1);
    check('a negative timeout is rejected', false);
} catch (InvalidQueueArgumentException) {
    check('a negative timeout is rejected', true);
}

try {
    $queue->pop(queues: ['default', '']);
    check('an empty queue name is rejected', false);
} catch (InvalidQueueArgumentException) {
    check('an empty queue name is rejected', true);
}

try {
    $queue->pop(queues: ['default', 'high', 'default']);
    check('a duplicate queue name is rejected', false);
} catch (InvalidQueueArgumentException) {
    check('a duplicate queue name is rejected', true);
}

check('an empty queue list returns null, not an error', $queue->pop(timeoutSeconds: 1, queues: []) === null);

echo "ALL CHECKS PASSED\n";
