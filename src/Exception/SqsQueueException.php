<?php

declare(strict_types=1);

namespace Kinetis\QueueSqs\Exception;

use RuntimeException;

final class SqsQueueException extends RuntimeException
{
    public static function noQueueUrlReturned(string $queue): self
    {
        return new self("SQS returned no URL for queue \"{$queue}\".");
    }
}
