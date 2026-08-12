<?php

declare(strict_types=1);

namespace Kinetis\QueueSqs\Tests;

use AsyncAws\Core\AbstractApi;
use AsyncAws\Core\Configuration;
use AsyncAws\Sqs\SqsClient;
use Kinetis\Config\Config;
use Kinetis\Config\Exception\MissingConfigException;
use Kinetis\QueueSqs\SqsClientFactory;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;

final class SqsClientFactoryTest extends TestCase
{
    public function test_builds_a_client_for_the_default_connection(): void
    {
        $config = new Config(['QUEUE_SQS_REGION' => 'us-east-1']);

        $client = SqsClientFactory::fromConfig($config);

        self::assertInstanceOf(SqsClient::class, $client);
        self::assertSame('us-east-1', $this->regionOf($client));
    }

    public function test_a_named_connection_reads_its_own_region_not_the_defaults(): void
    {
        $config = new Config([
            'QUEUE_SQS_REGION' => 'us-east-1',
            'QUEUE_REPORTS_SQS_REGION' => 'eu-west-1',
        ]);

        $default = SqsClientFactory::fromConfig($config);
        $reports = SqsClientFactory::fromConfig($config, 'reports');

        self::assertSame('us-east-1', $this->regionOf($default));
        self::assertSame('eu-west-1', $this->regionOf($reports));
    }

    public function test_a_missing_region_throws_a_clear_error(): void
    {
        $config = new Config([]);

        $this->expectException(MissingConfigException::class);
        $this->expectExceptionMessage('QUEUE_SQS_REGION');
        SqsClientFactory::fromConfig($config);
    }

    public function test_a_named_connections_missing_region_names_its_own_scoped_key(): void
    {
        $config = new Config([]);

        $this->expectException(MissingConfigException::class);
        $this->expectExceptionMessage('QUEUE_REPORTS_SQS_REGION');
        SqsClientFactory::fromConfig($config, 'reports');
    }

    public function test_an_optional_endpoint_is_accepted_without_error(): void
    {
        $config = new Config([
            'QUEUE_SQS_REGION' => 'us-east-1',
            'QUEUE_SQS_ENDPOINT' => 'http://localhost:4566',
        ]);

        self::assertInstanceOf(SqsClient::class, SqsClientFactory::fromConfig($config));
    }

    private function regionOf(SqsClient $client): ?string
    {
        $property = new ReflectionProperty(AbstractApi::class, 'configuration');

        /** @var Configuration $configuration */
        $configuration = $property->getValue($client);

        return $configuration->get('region');
    }
}
