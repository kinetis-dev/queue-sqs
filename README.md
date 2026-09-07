<p align="center">
  <img src="logo.svg" alt="Kinetis" width="420">
</p>

<p align="center">
  <strong>kinetis/queue-sqs</strong>
  <br>
  <strong>A Fiber-native, non-blocking Amazon SQS backend for kinetis/queue's <code>QueueInterface</code></strong>
</p>

<p align="center">
  <a href="https://packagist.org/packages/kinetis/queue-sqs"><img src="https://img.shields.io/packagist/v/kinetis/queue-sqs?label=version" alt="Packagist Version"></a>
  <a href="https://packagist.org/packages/kinetis/queue-sqs"><img src="https://img.shields.io/packagist/dt/kinetis/queue-sqs" alt="Packagist Downloads"></a>
  <a href="https://packagist.org/packages/kinetis/queue-sqs"><img src="https://img.shields.io/packagist/php-v/kinetis/queue-sqs" alt="PHP Version"></a>
  <a href="https://packagist.org/packages/kinetis/queue-sqs"><img src="https://img.shields.io/packagist/l/kinetis/queue-sqs" alt="License"></a>
  <a href="https://github.com/kinetis-dev/kinetis/actions/workflows/ci.yml"><img src="https://github.com/kinetis-dev/kinetis/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
</p>

---

Part of [Kinetis](https://kinetis.dev/), a non-blocking PHP framework for
API-first applications, developed in the
[kinetis-dev/kinetis](https://github.com/kinetis-dev/kinetis) monorepo.

Adds Amazon SQS as a queue backend. `push()`/`pop()`/`ack()`/`release()`/`fail()`
work exactly like any other backend — only your configuration changes.

`SqsQueue` implements `Kinetis\Queue\QueueInterface` and not
`Kinetis\Queue\ClearableQueueInterface`: it has no `clear()`, and
`kinetis queue:clear` names the backend and stops. SQS offers no
operation that meets the clearing contract. `PurgeQueue` deletes the
messages a worker holds in flight along with the waiting ones, keeps
deleting messages sent during the up-to-60-second window it takes to
finish, reports no count, and is rate-limited to once per 60 seconds per
queue — so this package never calls it. `size()` could not report what
such a call destroyed either: it excludes in-flight work and is an
estimate. Empty an SQS queue the way you created it, with `aws sqs
purge-queue` or by recreating it.

`QueuedJob::$handle` is the message's `ReceiptHandle`, which SQS scopes
to the receive that produced it. This backend cannot tell SQS's answer
for a spent handle apart from any other API error, so it raises no
`Kinetis\Queue\Exception\StaleJobHandleException` and whatever SQS
returns propagates as itself. SQS can also redeliver a message
independently of anything this package does, so job handlers have to be
idempotent.

```php
use Kinetis\Config\Config;
use Kinetis\QueueSqs\SqsClientFactory;
use Kinetis\QueueSqs\SqsQueue;

$queue = new SqsQueue(SqsClientFactory::fromConfig($config));

$queue->push(new SendWelcomeEmail($email, $name), queue: 'default');
```

## Configuration

```
QUEUE_CONNECTION=sqs
QUEUE_SQS_REGION=us-east-1
```

| Key | Default | Purpose |
|---|---|---|
| `QUEUE_SQS_REGION` | *(required)* | AWS region. |
| `QUEUE_SQS_ENDPOINT` | — | SQS-compatible endpoint (e.g. LocalStack). One origin, nothing else. |
| `QUEUE_SQS_PLAINTEXT` | `false` | Allows an `http://` value for `QUEUE_SQS_ENDPOINT`. |
| `QUEUE_SQS_TIMEOUT` | `30` | Seconds bounding each SQS request and each credential lookup. |
| `QUEUE_SQS_QUEUE_PREFIX` | — | Prepended to every queue name — for shared AWS accounts. |

All five are scoped — `QUEUE_SQS_REGION` + `reports` →
`QUEUE_REPORTS_SQS_REGION`. [`kinetis/queue`](https://github.com/kinetis-dev/queue)'s own keys
(`QUEUE_CONNECTION`, `QUEUE_MAX_ATTEMPTS`, ...) are documented in that
package; full reference:
[kinetis.dev/docs/config.html](https://kinetis.dev/docs/config.html).

`QUEUE_SQS_ENDPOINT` is a scheme, a host and an optional port, with no
userinfo, path, query or fragment. Without it the destination is
AsyncAws's regional endpoint table, and an `AWS_ENDPOINT_URL` sitting in
the environment for some other tool is refused rather than quietly
redirecting signed requests. `QUEUE_SQS_TIMEOUT` bounds each request on
its own, not a whole `pop()`. Any positive value is accepted; set it
above the longest long poll the application issues — at most a
five-second slice, and shorter whenever a `pop()` deadline caps it —
which the default of `30` already covers.

Credentials are never read from Kinetis config — AsyncAws's standard
provider chain resolves them on its own, the usual AWS SDK convention.
Every provider in that chain that calls AWS uses the same Revolt
transport as the client, while the shared credentials and config files
and any token file are read with native blocking calls. Resolved
credentials are held only while unexpired, and a lookup that resolved
nothing is retried on the next queue operation rather than remembered.
Full detail:
[kinetis.dev/docs/queue-sqs.html](https://kinetis.dev/docs/queue-sqs.html).

A `push()`/`pop()` queue name resolves directly to an SQS queue of that
name — create it ahead of time; this package never creates one
automatically.

## Installation

```sh
composer require kinetis/queue-sqs
```

Requires PHP 8.4+, [`kinetis/framework`](https://github.com/kinetis-dev/framework), [`kinetis/queue`](https://github.com/kinetis-dev/queue), and
[`kinetis/revolt-http-client`](https://github.com/kinetis-dev/revolt-http-client). Full documentation:
[kinetis.dev/docs/queue-sqs.html](https://kinetis.dev/docs/queue-sqs.html).

## License

MIT — see [LICENSE](LICENSE).
