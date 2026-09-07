<?php

declare(strict_types=1);

namespace Kinetis\QueueSqs;

use AsyncAws\Core\Configuration;
use AsyncAws\Core\Credentials\CredentialProvider;
use AsyncAws\Core\Credentials\Credentials;

/**
 * The credential providers this package builds, consulted in order until
 * one answers with unexpired credentials, which are then held for reuse.
 *
 * An expired answer is no answer: it is passed over for the next provider
 * within the same call, exactly as a null one is, since signing with it
 * would only produce a request SQS rejects. Nothing is held for a provider
 * or for a whole round that answered nothing usable, so an instance role,
 * a container credential endpoint or a token file that appears after a
 * worker has started is picked up on the next queue operation rather than
 * shadowed by a remembered failure. Expiry is the credentials' own
 * `isExpired()`, which carries AsyncAws's refresh drift, so nothing here
 * reads a clock.
 *
 * One instance serves one client, so what it holds is keyed by nothing:
 * SqsClientFactory builds a chain per client, and a client signs with one
 * Configuration for its whole life.
 */
final class CredentialChain implements CredentialProvider
{
    private ?Credentials $held = null;

    /**
     * @var list<CredentialProvider>
     */
    private readonly array $providers;

    public function __construct(CredentialProvider ...$providers)
    {
        $this->providers = array_values($providers);
    }

    public function getCredentials(Configuration $configuration): ?Credentials
    {
        if ($this->held !== null && !$this->held->isExpired()) {
            return $this->held;
        }

        $this->held = null;

        foreach ($this->providers as $provider) {
            $credentials = $provider->getCredentials($configuration);

            if ($credentials !== null && !$credentials->isExpired()) {
                return $this->held = $credentials;
            }
        }

        return null;
    }
}
