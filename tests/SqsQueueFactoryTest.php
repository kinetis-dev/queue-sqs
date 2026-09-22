<?php

declare(strict_types=1);

namespace Kinetis\QueueSqs\Tests;

use InvalidArgumentException;
use Kinetis\Config\Config;
use Kinetis\Config\Exception\MissingConfigException;
use Kinetis\QueueSqs\SqsQueue;
use Kinetis\QueueSqs\SqsQueueFactory;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * The visibility window is this backend's only new setting, and unlike
 * Redis's and SQL's it is sent on every receive rather than only
 * compared locally — so what the factory reads is what SQS is told.
 *
 * `SqsClientFactory::fromConfig()` makes no network call, so building a
 * queue here needs no reachable AWS endpoint (see SqsClientFactoryTest).
 */
final class SqsQueueFactoryTest extends TestCase
{
    public function test_an_absent_setting_still_gives_a_finite_renewable_window(): void
    {
        $queue = SqsQueueFactory::fromConfig(new Config(['QUEUE_SQS_REGION' => 'us-east-1']));

        self::assertInstanceOf(SqsQueue::class, $queue);
        self::assertSame(300, $queue->visibilityTimeoutSeconds());
    }

    public function test_a_named_connection_reads_its_own_scoped_window(): void
    {
        $config = new Config([
            'QUEUE_SQS_REGION' => 'us-east-1',
            'QUEUE_REPORTS_SQS_REGION' => 'eu-west-1',
            'QUEUE_VISIBILITY_TIMEOUT_SECONDS' => '300',
            'QUEUE_REPORTS_VISIBILITY_TIMEOUT_SECONDS' => '900',
        ]);

        self::assertSame(900, SqsQueueFactory::fromConfig($config, 'reports')->visibilityTimeoutSeconds());
        self::assertSame(300, SqsQueueFactory::fromConfig($config)->visibilityTimeoutSeconds());
    }

    /**
     * @return list<array{string}>
     */
    public static function rejectedWindows(): array
    {
        return [
            'zero' => ['0'],
            'negative' => ['-1'],
            'past the request field cap' => ['43201'],
        ];
    }

    /**
     * No region is configured either, so a value outside the range has
     * to be refused before anything else is read or built for the
     * message an operator gets to name the window rather than the
     * region.
     */
    #[DataProvider('rejectedWindows')]
    public function test_a_window_outside_the_admitted_range_is_refused_before_a_client_exists(string $seconds): void
    {
        $config = new Config(['QUEUE_VISIBILITY_TIMEOUT_SECONDS' => $seconds]);

        try {
            SqsQueueFactory::fromConfig($config);
            self::fail('Expected the out-of-range window to be refused.');
        } catch (MissingConfigException) {
            self::fail('the region was read first, so the window was not validated before the client was built');
        } catch (InvalidArgumentException $e) {
            self::assertStringContainsString('QUEUE_VISIBILITY_TIMEOUT_SECONDS', $e->getMessage());
            self::assertStringContainsString('between 1 and 43200 seconds', $e->getMessage());
        }
    }
}
