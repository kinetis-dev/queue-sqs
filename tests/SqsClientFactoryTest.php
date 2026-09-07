<?php

declare(strict_types=1);

namespace Kinetis\QueueSqs\Tests;

use AsyncAws\Core\AbstractApi;
use AsyncAws\Core\Configuration;
use AsyncAws\Sqs\SqsClient;
use Kinetis\Config\Config;
use Kinetis\Config\Exception\MissingConfigException;
use Kinetis\QueueSqs\Exception\SqsConfigurationException;
use Kinetis\QueueSqs\SqsClientFactory;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;
use Symfony\Component\HttpClient\AmpHttpClient;

final class SqsClientFactoryTest extends TestCase
{
    public function test_builds_a_client_for_the_default_connection(): void
    {
        $client = SqsClientFactory::fromConfig(self::config());

        self::assertInstanceOf(SqsClient::class, $client);
        self::assertSame('us-east-1', self::configurationOf($client)->get('region'));
    }

    public function test_a_named_connection_reads_its_own_region_not_the_defaults(): void
    {
        $config = new Config([
            'QUEUE_SQS_REGION' => 'us-east-1',
            'QUEUE_REPORTS_SQS_REGION' => 'eu-west-1',
        ]);

        $default = SqsClientFactory::fromConfig($config);
        $reports = SqsClientFactory::fromConfig($config, 'reports');

        self::assertSame('us-east-1', self::configurationOf($default)->get('region'));
        self::assertSame('eu-west-1', self::configurationOf($reports)->get('region'));
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

    public function test_no_endpoint_leaves_async_aws_its_regional_endpoint_table(): void
    {
        $configuration = self::configurationOf(SqsClientFactory::fromConfig(self::config()));

        self::assertTrue($configuration->isDefault(Configuration::OPTION_ENDPOINT));
    }

    public function test_an_ambient_endpoint_url_is_refused_rather_than_silently_used(): void
    {
        $restore = $_ENV['AWS_ENDPOINT_URL'] ?? null;
        $_ENV['AWS_ENDPOINT_URL'] = 'https://sqs.somewhere-else.test';

        try {
            $this->expectException(SqsConfigurationException::class);
            $this->expectExceptionMessage('QUEUE_SQS_ENDPOINT');
            SqsClientFactory::fromConfig(self::config());
        } finally {
            if ($restore === null) {
                unset($_ENV['AWS_ENDPOINT_URL']);
            } else {
                $_ENV['AWS_ENDPOINT_URL'] = $restore;
            }
        }
    }

    public function test_an_explicit_endpoint_is_reduced_to_its_origin(): void
    {
        $configuration = self::configurationOf(SqsClientFactory::fromConfig(
            self::config(['QUEUE_SQS_ENDPOINT' => 'HTTPS://Sqs.example.test:4566/']),
        ));

        self::assertSame('https://sqs.example.test:4566', $configuration->get(Configuration::OPTION_ENDPOINT));
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function unusableEndpoints(): iterable
    {
        yield 'no scheme' => ['sqs.example.test'];
        yield 'unsupported scheme' => ['ftp://sqs.example.test'];
        yield 'no host' => ['https:///queue'];
        yield 'userinfo' => ['https://key:secret@sqs.example.test'];
        yield 'path' => ['https://sqs.example.test/123456789012'];
        yield 'query' => ['https://sqs.example.test?region=us-east-1'];
        yield 'fragment' => ['https://sqs.example.test#frag'];
    }

    #[DataProvider('unusableEndpoints')]
    public function test_an_endpoint_that_is_not_one_origin_is_refused(string $endpoint): void
    {
        $this->expectException(SqsConfigurationException::class);
        SqsClientFactory::fromConfig(self::config(['QUEUE_SQS_ENDPOINT' => $endpoint]));
    }

    public function test_a_plain_http_endpoint_needs_its_own_opt_in(): void
    {
        $this->expectException(SqsConfigurationException::class);
        $this->expectExceptionMessage('QUEUE_SQS_PLAINTEXT');
        SqsClientFactory::fromConfig(self::config(['QUEUE_SQS_ENDPOINT' => 'http://localstack:4566']));
    }

    public function test_the_plaintext_opt_in_accepts_a_compose_service_name(): void
    {
        $configuration = self::configurationOf(SqsClientFactory::fromConfig(self::config([
            'QUEUE_SQS_ENDPOINT' => 'http://localstack:4566',
            'QUEUE_SQS_PLAINTEXT' => 'true',
        ])));

        self::assertSame('http://localstack:4566', $configuration->get(Configuration::OPTION_ENDPOINT));
    }

    /**
     * The default leaves room for a full five-second long poll, which SQS
     * holds open on purpose and which a shorter idle or total budget would
     * abort as a failure. The check on a configured value is only that it
     * is positive — how much headroom a deployment needs follows from the
     * longest long poll it issues.
     */
    public function test_every_request_gets_the_default_timeout_and_follows_no_redirect(): void
    {
        $options = self::transportOptionsOf(SqsClientFactory::fromConfig(self::config()));

        self::assertSame(30.0, $options['timeout']);
        self::assertSame(30.0, $options['max_duration']);
        self::assertSame(0, $options['max_redirects']);
    }

    public function test_a_configured_timeout_covers_idle_and_transfer_alike(): void
    {
        $options = self::transportOptionsOf(SqsClientFactory::fromConfig(
            self::config(['QUEUE_SQS_TIMEOUT' => '7.5']),
        ));

        self::assertSame(7.5, $options['timeout']);
        self::assertSame(7.5, $options['max_duration']);
    }

    public function test_a_named_connection_reads_its_own_timeout(): void
    {
        $options = self::transportOptionsOf(SqsClientFactory::fromConfig(
            new Config(['QUEUE_REPORTS_SQS_REGION' => 'eu-west-1', 'QUEUE_REPORTS_SQS_TIMEOUT' => '12']),
            'reports',
        ));

        self::assertSame(12.0, $options['timeout']);
    }

    public function test_a_non_positive_timeout_is_refused(): void
    {
        $this->expectException(SqsConfigurationException::class);
        $this->expectExceptionMessage('QUEUE_SQS_TIMEOUT');
        SqsClientFactory::fromConfig(self::config(['QUEUE_SQS_TIMEOUT' => '-1']));
    }

    public function test_a_zero_timeout_is_refused(): void
    {
        $this->expectException(SqsConfigurationException::class);
        $this->expectExceptionMessage('QUEUE_SQS_TIMEOUT');
        SqsClientFactory::fromConfig(self::config(['QUEUE_SQS_TIMEOUT' => '0']));
    }

    /**
     * @param array<string, string> $extra
     */
    private static function config(array $extra = []): Config
    {
        return new Config(['QUEUE_SQS_REGION' => 'us-east-1', ...$extra]);
    }

    private static function configurationOf(SqsClient $client): Configuration
    {
        $configuration = new ReflectionProperty(AbstractApi::class, 'configuration')->getValue($client);

        self::assertInstanceOf(Configuration::class, $configuration);

        return $configuration;
    }

    /**
     * @return array<string, mixed>
     */
    private static function transportOptionsOf(SqsClient $client): array
    {
        $transport = new ReflectionProperty(AbstractApi::class, 'httpClient')->getValue($client);

        self::assertInstanceOf(AmpHttpClient::class, $transport);

        /** @var array<string, mixed> */
        return new ReflectionProperty(AmpHttpClient::class, 'defaultOptions')->getValue($transport);
    }
}
