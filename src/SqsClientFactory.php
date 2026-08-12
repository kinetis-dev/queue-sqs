<?php

declare(strict_types=1);

namespace Kinetis\QueueSqs;

use AsyncAws\Sqs\SqsClient;
use Kinetis\Config\Config;
use Kinetis\RevoltHttpClient\AmpHttpClientFactory;

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
 * $connection selects a named connection via Config::scopedKey() — plugged
 * into kinetis/queue's own bin/queue dispatch when QUEUE_CONNECTION=sqs,
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

        return new SqsClient($configuration, null, AmpHttpClientFactory::create());
    }
}
