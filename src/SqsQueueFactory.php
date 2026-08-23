<?php

declare(strict_types=1);

namespace Kinetis\QueueSqs;

use Kinetis\Config\Config;
use Kinetis\Queue\QueueInterface;

/**
 * Builds the SQS queue backend `QUEUE_CONNECTION=sqs` selects — called
 * by `kinetis/queue`'s own `QueueFactory::fromConfig()`, gated behind a
 * `class_exists()` check so core never depends on this package
 * directly.
 */
final class SqsQueueFactory
{
    public static function fromConfig(Config $config, string $connectionName = 'default'): QueueInterface
    {
        $queuePrefix = $config->string(Config::scopedKey('QUEUE_SQS_QUEUE_PREFIX', $connectionName), '');

        return new SqsQueue(SqsClientFactory::fromConfig($config, $connectionName), $queuePrefix);
    }
}
