<?php

declare(strict_types=1);

namespace Kinetis\QueueSqs\Tests\Fixtures;

use Kinetis\Queue\Job;

/**
 * A named, no-argument job — push()/pop() need a real class string to
 * serialize and reconstruct, which an anonymous class cannot supply.
 */
final class RecordedJob implements Job
{
    public function handle(): void {}
}
