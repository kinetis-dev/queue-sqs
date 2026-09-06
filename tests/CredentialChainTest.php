<?php

declare(strict_types=1);

namespace Kinetis\QueueSqs\Tests;

use AsyncAws\Core\AbstractApi;
use AsyncAws\Core\Credentials\CacheProvider;
use AsyncAws\Core\Credentials\ChainProvider;
use AsyncAws\Core\Credentials\ConfigurationProvider;
use AsyncAws\Core\Credentials\ContainerProvider;
use AsyncAws\Core\Credentials\IniFileProvider;
use AsyncAws\Core\Credentials\InstanceProvider;
use AsyncAws\Core\Credentials\WebIdentityProvider;
use AsyncAws\Sqs\SqsClient;
use Kinetis\Config\Config;
use Kinetis\QueueSqs\SqsClientFactory;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;
use Symfony\Contracts\HttpClient\HttpClientInterface;

/**
 * The credential chain is vendor wiring, and the invariant it has to
 * hold is that no provider in it reaches AWS on a transport this factory
 * did not supply — a provider left without one builds a blocking Symfony
 * client and assumes its role on the worker thread. Reflection is what
 * can see that: the providers expose no accessor, and making one call
 * resolve would need real AWS environment state.
 */
final class CredentialChainTest extends TestCase
{
    private static function client(): SqsClient
    {
        return SqsClientFactory::fromConfig(new Config(['QUEUE_SQS_REGION' => 'us-east-1']));
    }

    /**
     * @return list<object>
     */
    private static function providersOf(SqsClient $client): array
    {
        $provider = new ReflectionProperty(AbstractApi::class, 'credentialProvider')->getValue($client);

        self::assertInstanceOf(CacheProvider::class, $provider);

        $chain = new ReflectionProperty(CacheProvider::class, 'decorated')->getValue($provider);

        self::assertInstanceOf(ChainProvider::class, $chain);

        /** @var iterable<object> $providers */
        $providers = new ReflectionProperty(ChainProvider::class, 'providers')->getValue($chain);

        return array_values([...$providers]);
    }

    public function test_the_chain_holds_the_standard_providers_in_the_standard_order(): void
    {
        self::assertSame([
            ConfigurationProvider::class,
            WebIdentityProvider::class,
            IniFileProvider::class,
            ContainerProvider::class,
            InstanceProvider::class,
        ], array_map(
            static fn (object $provider): string => $provider::class,
            self::providersOf(self::client()),
        ));
    }

    public function test_every_provider_that_reaches_the_network_owns_the_clients_own_transport(): void
    {
        $client = self::client();
        $transport = new ReflectionProperty(AbstractApi::class, 'httpClient')->getValue($client);

        self::assertInstanceOf(HttpClientInterface::class, $transport);

        foreach (self::providersOf($client) as $provider) {
            self::assertSame(
                $transport,
                new ReflectionProperty($provider::class, 'httpClient')->getValue($provider),
                $provider::class . ' must resolve credentials over the injected transport.',
            );
        }
    }

    public function test_a_named_connections_client_owns_its_own_chain_on_its_own_transport(): void
    {
        $config = new Config([
            'QUEUE_SQS_REGION' => 'us-east-1',
            'QUEUE_REPORTS_SQS_REGION' => 'eu-west-1',
        ]);

        $reports = SqsClientFactory::fromConfig($config, 'reports');
        $transport = new ReflectionProperty(AbstractApi::class, 'httpClient')->getValue($reports);

        foreach (self::providersOf($reports) as $provider) {
            self::assertSame(
                $transport,
                new ReflectionProperty($provider::class, 'httpClient')->getValue($provider),
                $provider::class . ' must resolve credentials over the injected transport.',
            );
        }
    }
}
