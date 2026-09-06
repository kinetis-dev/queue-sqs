<?php

declare(strict_types=1);

namespace Kinetis\QueueSqs;

use AsyncAws\Core\Credentials\CacheProvider;
use AsyncAws\Core\Credentials\ChainProvider;
use AsyncAws\Core\Credentials\ConfigurationProvider;
use AsyncAws\Core\Credentials\ContainerProvider;
use AsyncAws\Core\Credentials\CredentialProvider;
use AsyncAws\Core\Credentials\IniFileProvider;
use AsyncAws\Core\Credentials\InstanceProvider;
use AsyncAws\Core\Credentials\WebIdentityProvider;
use AsyncAws\Sqs\SqsClient;
use Kinetis\Config\Config;
use Kinetis\RevoltHttpClient\AmpHttpClientFactory;
use Symfony\Contracts\HttpClient\HttpClientInterface;

/**
 * Builds an AsyncAws\Sqs\SqsClient with
 * Kinetis\RevoltHttpClient\AmpHttpClientFactory::create() injected as its
 * transport instead of the default blocking one — the identical pattern
 * Kinetis\StorageS3\S3FilesystemFactory already establishes for S3Client.
 * Credentials are deliberately never read from Kinetis\Config: AsyncAws's
 * own default credential provider chain (AWS_ACCESS_KEY_ID/
 * AWS_SECRET_ACCESS_KEY, or an IAM role) already resolves them, the
 * standard AWS SDK convention.
 *
 * The chain that resolution runs through is built here rather than left
 * to AsyncAws's own default — see credentialProvider().
 *
 * $connection selects a named connection via Config::scopedKey() — plugged
 * into kinetis/queue's QueueFactory dispatch when QUEUE_CONNECTION=sqs,
 * never resolved automatically by type.
 */
final class SqsClientFactory
{
    public static function fromConfig(Config $config, string $connection = 'default'): SqsClient
    {
        $region = $config->required(Config::scopedKey('QUEUE_SQS_REGION', $connection));
        $endpoint = $config->get(Config::scopedKey('QUEUE_SQS_ENDPOINT', $connection));

        $configuration = ['region' => $region];

        if ($endpoint !== null) {
            $configuration['endpoint'] = $endpoint;
        }

        $transport = AmpHttpClientFactory::create();

        return new SqsClient($configuration, self::credentialProvider($transport), $transport);
    }

    /**
     * AsyncAws's own default chain, in its own provider order, with one
     * change: every provider that reaches the network is handed
     * $transport. ChainProvider::createDefaultChain() builds
     * ConfigurationProvider with no client, and that provider is the one
     * that calls STS when AWS_ROLE_ARN is set — with no client it
     * constructs a blocking Symfony transport and assumes the role on
     * the worker thread, on first resolution and again on every expiry.
     *
     * CacheProvider holds the resolved credentials until they expire, so
     * the chain runs again only at refresh.
     *
     * The shared credentials file, the shared config file and any
     * web-identity or pod-identity token file are read with native
     * blocking calls inside the providers that consult them; that is
     * AsyncAws's resolution and this package does not reimplement it.
     */
    private static function credentialProvider(HttpClientInterface $transport): CredentialProvider
    {
        return new CacheProvider(new ChainProvider([
            new ConfigurationProvider($transport),
            new WebIdentityProvider(null, null, $transport),
            new IniFileProvider(null, null, $transport),
            new ContainerProvider($transport),
            new InstanceProvider($transport),
        ]));
    }
}
