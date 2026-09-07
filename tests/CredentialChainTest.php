<?php

declare(strict_types=1);

namespace Kinetis\QueueSqs\Tests;

use AsyncAws\Core\AbstractApi;
use AsyncAws\Core\Configuration;
use AsyncAws\Core\Credentials\ConfigurationProvider;
use AsyncAws\Core\Credentials\ContainerProvider;
use AsyncAws\Core\Credentials\CredentialProvider;
use AsyncAws\Core\Credentials\Credentials;
use AsyncAws\Core\Credentials\IniFileProvider;
use AsyncAws\Core\Credentials\InstanceProvider;
use AsyncAws\Core\Credentials\WebIdentityProvider;
use AsyncAws\Sqs\SqsClient;
use DateTimeImmutable;
use Kinetis\Config\Config;
use Kinetis\QueueSqs\CredentialChain;
use Kinetis\QueueSqs\SqsClientFactory;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;
use Symfony\Contracts\HttpClient\HttpClientInterface;

/**
 * Two things about credentials: the chain the factory composes, whose
 * invariant is that no provider in it reaches AWS on a transport the
 * factory did not supply — a provider left without one builds a blocking
 * Symfony client and assumes its role on the worker thread — and what
 * CredentialChain itself holds between calls. Reflection is what can see
 * the first: the providers expose no accessor, and making one call
 * resolve would need real AWS environment state.
 */
final class CredentialChainTest extends TestCase
{
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

    public function test_resolved_credentials_are_reused_without_consulting_the_chain_again(): void
    {
        $resolved = new Credentials('key', 'secret');
        $provider = self::provider($resolved);
        $chain = new CredentialChain($provider);

        self::assertSame($resolved, $chain->getCredentials(self::configuration()));
        self::assertSame($resolved, self::heldBy($chain));
        self::assertSame($resolved, $chain->getCredentials(self::configuration()));
        self::assertSame(1, $provider->calls);
    }

    public function test_an_expired_answer_does_not_shadow_a_later_provider(): void
    {
        $fresh = new Credentials('rotated', 'secret');
        $stale = self::provider(new Credentials('key', 'secret', null, new DateTimeImmutable('-1 hour')));
        $resolving = self::provider($fresh);
        $chain = new CredentialChain($stale, $resolving);

        self::assertSame($fresh, $chain->getCredentials(self::configuration()));
        self::assertSame($fresh, self::heldBy($chain));
        self::assertSame(1, $stale->calls);
        self::assertSame(1, $resolving->calls);
    }

    public function test_a_round_answering_only_expired_credentials_resolves_nothing_and_runs_again(): void
    {
        $expired = new Credentials('key', 'secret', null, new DateTimeImmutable('-1 hour'));
        $fresh = new Credentials('rotated', 'secret');
        $provider = self::provider($expired, $fresh);
        $chain = new CredentialChain($provider);

        self::assertNull($chain->getCredentials(self::configuration()));
        self::assertNull(self::heldBy($chain));
        self::assertSame($fresh, $chain->getCredentials(self::configuration()));
        self::assertSame(2, $provider->calls);
    }

    public function test_a_chain_that_resolved_nothing_is_run_again_rather_than_remembered(): void
    {
        $arriving = new Credentials('key', 'secret');
        $provider = self::provider(null, $arriving);
        $chain = new CredentialChain($provider);

        self::assertNull($chain->getCredentials(self::configuration()));
        self::assertNull(self::heldBy($chain));
        self::assertSame($arriving, $chain->getCredentials(self::configuration()));
        self::assertSame(2, $provider->calls);
    }

    public function test_the_first_provider_with_usable_credentials_wins_and_the_rest_are_left_alone(): void
    {
        $wanted = new Credentials('key', 'secret');
        $empty = self::provider(null);
        $resolving = self::provider($wanted);
        $later = self::provider(new Credentials('never', 'used'));

        self::assertSame($wanted, new CredentialChain($empty, $resolving, $later)->getCredentials(self::configuration()));
        self::assertSame(1, $empty->calls);
        self::assertSame(1, $resolving->calls);
        self::assertSame(0, $later->calls);
    }

    /**
     * Answers $results in order, then nothing, counting the calls.
     */
    private static function provider(?Credentials ...$results): CredentialProvider
    {
        return new class(array_values($results)) implements CredentialProvider {
            public int $calls = 0;

            /**
             * @param list<?Credentials> $results
             */
            public function __construct(private array $results)
            {
            }

            public function getCredentials(Configuration $configuration): ?Credentials
            {
                ++$this->calls;

                return array_shift($this->results);
            }
        };
    }

    /**
     * What survives to the next call. Nothing else can see it, and an
     * expired or absent credential reaching it is the failure the reuse
     * guard would otherwise hide.
     */
    private static function heldBy(CredentialChain $chain): ?Credentials
    {
        /** @var ?Credentials */
        return new ReflectionProperty(CredentialChain::class, 'held')->getValue($chain);
    }

    private static function configuration(): Configuration
    {
        return Configuration::create(['region' => 'us-east-1']);
    }

    private static function client(): SqsClient
    {
        return SqsClientFactory::fromConfig(new Config(['QUEUE_SQS_REGION' => 'us-east-1']));
    }

    /**
     * @return list<object>
     */
    private static function providersOf(SqsClient $client): array
    {
        $chain = new ReflectionProperty(AbstractApi::class, 'credentialProvider')->getValue($client);

        self::assertInstanceOf(CredentialChain::class, $chain);

        /** @var list<object> */
        return new ReflectionProperty(CredentialChain::class, 'providers')->getValue($chain);
    }
}
