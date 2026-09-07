<?php

declare(strict_types=1);

namespace Kinetis\QueueSqs\Exception;

use RuntimeException;

/**
 * Configuration this package refuses to build a client from. Every one of
 * these is thrown while SqsClientFactory constructs, so a deployment that
 * would send SQS traffic somewhere unintended, or in the clear, fails at
 * boot rather than on the first push.
 *
 * A message names the configuration key to look at and nothing else. The
 * value under that key is an endpoint or a credential-adjacent setting,
 * and these messages reach logs and error trackers.
 */
final class SqsConfigurationException extends RuntimeException
{
    public static function ambientEndpoint(string $key): self
    {
        return new self(
            "AWS_ENDPOINT_URL is set in the environment and would redirect every SQS request away from the regional endpoint. Unset it, or set {$key} to the endpoint this connection is meant to use.",
        );
    }

    public static function malformedEndpoint(string $key, string $reason): self
    {
        return new self("{$key} is not a usable SQS endpoint: {$reason}.");
    }

    public static function plaintextEndpoint(string $key, string $plaintextKey): self
    {
        return new self(
            "{$key} is a plain-HTTP endpoint, which would carry credentials and job payloads in the clear. Use https, or set {$plaintextKey}=true to accept that on a trusted network.",
        );
    }

    public static function nonPositiveTimeout(string $key): self
    {
        return new self("{$key} must be a positive number of seconds.");
    }
}
