<?php

declare(strict_types=1);

namespace Kinetis\QueueSqs;

use AsyncAws\Core\Configuration;
use AsyncAws\Core\Credentials\ConfigurationProvider;
use AsyncAws\Core\Credentials\ContainerProvider;
use AsyncAws\Core\Credentials\CredentialProvider;
use AsyncAws\Core\Credentials\IniFileProvider;
use AsyncAws\Core\Credentials\InstanceProvider;
use AsyncAws\Core\Credentials\WebIdentityProvider;
use AsyncAws\Sqs\SqsClient;
use Kinetis\Config\Config;
use Kinetis\QueueSqs\Exception\SqsConfigurationException;
use Kinetis\RevoltHttpClient\AmpHttpClientFactory;
use Symfony\Contracts\HttpClient\HttpClientInterface;

/**
 * Builds an AsyncAws\Sqs\SqsClient with
 * Kinetis\RevoltHttpClient\AmpHttpClientFactory::create() injected as its
 * transport instead of the default blocking one — the identical pattern
 * Kinetis\StorageS3\S3FilesystemFactory already establishes for its own
 * client. Credentials are not read from Kinetis\Config: AsyncAws resolves
 * them from AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, the shared
 * credentials and config files, or an IAM role on its own, the standard
 * AWS SDK convention, and a second source of truth for the same thing
 * would only compete with it.
 *
 * The chain that resolution runs through is built here rather than left
 * to AsyncAws's own default — see credentialProvider(). One transport
 * serves the client and every provider in that chain, so the timeout
 * below bounds a credential lookup as well as an SQS call.
 *
 * $connection selects a named connection via Config::scopedKey() — plugged
 * into kinetis/queue's QueueFactory dispatch when QUEUE_CONNECTION=sqs,
 * never resolved automatically by type.
 */
final class SqsClientFactory
{
    /**
     * Seconds. Applied to each SQS request on its own — idle and total
     * transfer alike — not as one deadline across a pop() that issues
     * several. Any positive value is accepted; the value a deployment
     * needs sits above the longest long poll it issues, which is
     * SqsQueue's five-second slice or whatever a pop() deadline caps that
     * to, and 30 leaves room for a full one.
     */
    private const DEFAULT_TIMEOUT = 30.0;

    public static function fromConfig(Config $config, string $connection = 'default'): SqsClient
    {
        $region = $config->required(Config::scopedKey('QUEUE_SQS_REGION', $connection));

        $transport = AmpHttpClientFactory::create(self::transportOptions($config, $connection));
        $configuration = self::configuration($config, $connection, $region);

        return new SqsClient($configuration, self::credentialProvider($transport), $transport);
    }

    /**
     * Without QUEUE_SQS_ENDPOINT the destination is AsyncAws's own
     * regional endpoint table. AsyncAws would otherwise fall back to an
     * ambient AWS_ENDPOINT_URL, which is how a machine-wide setting for
     * some other tool silently changes where this application's signed
     * requests go; Configuration::isDefault() is what distinguishes that
     * fallback from the table.
     */
    private static function configuration(Config $config, string $connection, string $region): Configuration
    {
        $endpointKey = Config::scopedKey('QUEUE_SQS_ENDPOINT', $connection);
        $endpoint = $config->string($endpointKey, '');

        if ($endpoint === '') {
            $configuration = Configuration::create(['region' => $region]);

            if (!$configuration->isDefault(Configuration::OPTION_ENDPOINT)) {
                throw SqsConfigurationException::ambientEndpoint($endpointKey);
            }

            return $configuration;
        }

        return Configuration::create([
            'region' => $region,
            'endpoint' => self::origin($config, $connection, $endpointKey, $endpoint),
        ]);
    }

    /**
     * An endpoint is an origin and nothing else: AsyncAws appends its own
     * path to it, so a path, query or fragment would land in the middle
     * of a signed request, and userinfo is a credential the SigV4
     * signature does not cover. Rebuilding the accepted parts rather than
     * passing the string through keeps whatever else parse_url()
     * tolerated out of the URL.
     *
     * Plain HTTP is an explicit opt-in rather than an address check:
     * `http://localstack:4566` between containers on one Compose network
     * is as legitimate as a loopback address, and no parse of the host
     * can tell either from a public one.
     */
    private static function origin(Config $config, string $connection, string $key, string $endpoint): string
    {
        $parts = parse_url($endpoint);

        if ($parts === false || !isset($parts['scheme'], $parts['host']) || $parts['host'] === '') {
            throw SqsConfigurationException::malformedEndpoint($key, 'it needs a scheme and a host');
        }

        if (isset($parts['user']) || isset($parts['pass'])) {
            throw SqsConfigurationException::malformedEndpoint($key, 'it carries userinfo');
        }

        if (isset($parts['query']) || isset($parts['fragment'])) {
            throw SqsConfigurationException::malformedEndpoint($key, 'it carries a query or fragment');
        }

        if (($parts['path'] ?? '/') !== '/') {
            throw SqsConfigurationException::malformedEndpoint($key, 'it carries a path');
        }

        $scheme = strtolower($parts['scheme']);
        $host = strtolower($parts['host']);

        if ($scheme === 'http') {
            $plaintextKey = Config::scopedKey('QUEUE_SQS_PLAINTEXT', $connection);

            if (!$config->bool($plaintextKey, false)) {
                throw SqsConfigurationException::plaintextEndpoint($key, $plaintextKey);
            }
        } elseif ($scheme !== 'https') {
            throw SqsConfigurationException::malformedEndpoint($key, 'only http and https are supported');
        }

        return $scheme . '://' . $host . (isset($parts['port']) ? ':' . $parts['port'] : '');
    }

    /**
     * One SQS request is one wire attempt at one host: AmpHttpClientFactory
     * installs no retry interceptor, and a request that reached the wrong
     * region is answered with a redirect, which following would replay a
     * request signed for the original host somewhere else.
     *
     * @return array<string, mixed>
     */
    private static function transportOptions(Config $config, string $connection): array
    {
        $timeoutKey = Config::scopedKey('QUEUE_SQS_TIMEOUT', $connection);
        $timeout = $config->float($timeoutKey, self::DEFAULT_TIMEOUT);

        if ($timeout <= 0.0) {
            throw SqsConfigurationException::nonPositiveTimeout($timeoutKey);
        }

        return [
            'timeout' => $timeout,
            'max_duration' => $timeout,
            'max_redirects' => 0,
        ];
    }

    /**
     * AsyncAws's own provider order, with one change: every provider that
     * reaches the network is handed $transport.
     * ChainProvider::createDefaultChain() builds ConfigurationProvider
     * with no client, and that provider is the one that calls STS when
     * AWS_ROLE_ARN is set — with no client it constructs a blocking
     * Symfony transport and assumes the role on the worker thread, on
     * first resolution and again on every expiry.
     *
     * CredentialChain holds what they resolve; see it for what is and is
     * not remembered between calls.
     *
     * The shared credentials file, the shared config file and any
     * web-identity or pod-identity token file are read with native
     * blocking calls inside the providers that consult them; that is
     * AsyncAws's resolution and this package does not reimplement it.
     */
    private static function credentialProvider(HttpClientInterface $transport): CredentialProvider
    {
        return new CredentialChain(
            new ConfigurationProvider($transport),
            new WebIdentityProvider(null, null, $transport),
            new IniFileProvider(null, null, $transport),
            new ContainerProvider($transport),
            new InstanceProvider($transport),
        );
    }
}
