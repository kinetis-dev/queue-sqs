<?php

declare(strict_types=1);

namespace Kinetis\QueueSqs;

use InvalidArgumentException;
use Kinetis\Config\Config;

/**
 * Builds the SQS queue backend `QUEUE_CONNECTION=sqs` selects — called
 * by `kinetis/queue`'s own `QueueFactory::fromConfig()`, gated behind a
 * `class_exists()` check so core never depends on this package
 * directly.
 *
 * Returns the concrete `SqsQueue`, the narrowest type this backend
 * declares: it satisfies `Kinetis\Queue\RenewableQueueInterface` and
 * not `Kinetis\Queue\ClearableQueueInterface` — see `SqsQueue`'s own
 * docblock for why the second one is absent.
 */
final class SqsQueueFactory
{
    /**
     * The same default `kinetis/queue-redis` and `kinetis/queue-sql`
     * apply, so a deployment moving between backends keeps one window.
     */
    private const int DEFAULT_VISIBILITY_TIMEOUT_SECONDS = 300;

    public static function fromConfig(Config $config, string $connectionName = 'default'): SqsQueue
    {
        $queuePrefix = $config->string(Config::scopedKey('QUEUE_SQS_QUEUE_PREFIX', $connectionName), '');

        // Read and validated before the client is built, so a
        // misconfigured window is a startup error rather than something
        // a credential lookup or a ReceiveMessage discovers.
        $visibilityTimeout = self::visibilityTimeoutSeconds($config, $connectionName);

        return new SqsQueue(
            SqsClientFactory::fromConfig($config, $connectionName),
            $queuePrefix,
            $visibilityTimeout,
        );
    }

    /**
     * Unlike Redis and SQL, this window is not only a reclaim deadline:
     * every `ReceiveMessage` sends it, overriding the remote queue's own
     * `VisibilityTimeout` attribute, and every renewal restores it. The
     * admitted range is `SqsQueue`'s, so the two cannot drift.
     */
    private static function visibilityTimeoutSeconds(Config $config, string $connectionName): int
    {
        $key = Config::scopedKey('QUEUE_VISIBILITY_TIMEOUT_SECONDS', $connectionName);
        $seconds = $config->int($key, self::DEFAULT_VISIBILITY_TIMEOUT_SECONDS);

        // Rethrown with the key in front so an operator reading the
        // failure knows which setting to change — including which
        // named connection's, since the key is the scoped one.
        try {
            SqsQueue::assertValidVisibilityTimeout($seconds);
        } catch (InvalidArgumentException $e) {
            throw new InvalidArgumentException("{$key}: {$e->getMessage()}", previous: $e);
        }

        return $seconds;
    }
}
