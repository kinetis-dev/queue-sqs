<?php

declare(strict_types=1);

namespace Kinetis\QueueSqs\Tests\Fixtures;

use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Response\MockResponse;
use Symfony\Contracts\HttpClient\HttpClientInterface;

/**
 * An SqsClient transport that answers with canned JSON and records which
 * SQS operation each request carried.
 *
 * Amazon SQS speaks AWS JSON 1.0, so every request names its operation
 * in an X-Amz-Target header ("AmazonSQS.SendMessage") rather than in the
 * URL — which makes the recorded list an exact account of what a backend
 * asked SQS to do, including operations no assertion thought to look
 * for.
 *
 * An operation's response can be a list rather than one body, consumed
 * one entry per call with the last entry answering every call after it
 * — which is what lets a test hand out a distinct delivery per receive.
 *
 * A long-polling ReceiveMessage can also be given a duration, the way
 * SQS itself holds one open. A reply that arrives instantly cannot
 * exercise a pop() deadline that the long poll consumed.
 */
final class RecordingSqsTransport
{
    /** @var list<string> */
    public array $operations = [];

    /**
     * The decoded request body of every recorded call, index-aligned
     * with $operations — SQS speaks AWS JSON 1.0, so a request's own
     * parameters are readable here rather than only its operation name.
     *
     * @var list<array<string, mixed>>
     */
    public array $requests = [];

    /** @var array<string, list<string>> */
    private array $responses;

    /** @var array<string, int> */
    private array $calls = [];

    /**
     * @param array<string, string|list<string>> $responses JSON body per
     *     operation name; an operation with no entry answers with an
     *     empty object, which is what SQS returns for DeleteMessage and
     *     ChangeMessageVisibility
     * @param int $longPollMicroseconds how long a ReceiveMessage
     *     carrying a non-zero WaitTimeSeconds takes to answer; 0 answers
     *     every call at once
     */
    public function __construct(array $responses = [], private readonly int $longPollMicroseconds = 0)
    {
        $this->responses = array_map(
            static fn (string|array $bodies): array => \is_array($bodies) ? $bodies : [$bodies],
            $responses,
        );
    }

    public function client(): HttpClientInterface
    {
        return new MockHttpClient(function (string $method, string $url, array $options): MockResponse {
            $operation = self::operationOf($options);
            $request = self::requestOf($options);
            $this->operations[] = $operation;
            $this->requests[] = $request;

            if ($this->longPollMicroseconds > 0 && $operation === 'ReceiveMessage' && ($request['WaitTimeSeconds'] ?? 0) > 0) {
                usleep($this->longPollMicroseconds);
            }

            return new MockResponse(
                $this->bodyFor($operation),
                ['response_headers' => ['content-type' => 'application/x-amz-json-1.0']],
            );
        });
    }

    private function bodyFor(string $operation): string
    {
        $bodies = $this->responses[$operation] ?? [];

        if ($bodies === []) {
            return '{}';
        }

        $index = $this->calls[$operation] ?? 0;
        $this->calls[$operation] = $index + 1;

        return $bodies[min($index, \count($bodies) - 1)];
    }

    /**
     * @param array<string, mixed> $options
     * @return array<string, mixed>
     */
    private static function requestOf(array $options): array
    {
        $body = $options['body'] ?? '';
        $decoded = \is_string($body) ? json_decode($body, true) : null;

        return \is_array($decoded) ? $decoded : [];
    }

    /**
     * @param array<string, mixed> $options
     */
    private static function operationOf(array $options): string
    {
        /** @var list<string> $headers */
        $headers = $options['headers'] ?? [];

        foreach ($headers as $header) {
            if (preg_match('/^x-amz-target:\s*AmazonSQS\.(\w+)$/i', $header, $matches) === 1) {
                return $matches[1];
            }
        }

        return 'unidentified';
    }
}
