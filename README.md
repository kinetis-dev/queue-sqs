<p align="center">
  <img src="logo.svg" alt="Kinetis" width="420">
</p>

<p align="center">
  <strong>kinetis/queue-sqs</strong>
  <br>
  <strong>A Fiber-native, non-blocking Amazon SQS backend for kinetis/queue's <code>QueueInterface</code></strong>
</p>

<p align="center">
  <a href="https://packagist.org/packages/kinetis/queue-sqs"><img src="https://img.shields.io/packagist/v/kinetis/queue-sqs" alt="Packagist Version"></a>
  <a href="https://packagist.org/packages/kinetis/queue-sqs"><img src="https://img.shields.io/packagist/dt/kinetis/queue-sqs" alt="Packagist Downloads"></a>
  <a href="https://packagist.org/packages/kinetis/queue-sqs"><img src="https://img.shields.io/packagist/php-v/kinetis/queue-sqs" alt="PHP Version"></a>
  <a href="https://packagist.org/packages/kinetis/queue-sqs"><img src="https://img.shields.io/packagist/l/kinetis/queue-sqs" alt="License"></a>
  <a href="https://github.com/kinetis-dev/kinetis/actions/workflows/ci.yml"><img src="https://github.com/kinetis-dev/kinetis/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
</p>

---

Adds Amazon SQS as a queue backend. `push()`/`pop()`/`ack()`/`release()`/`fail()`
work exactly like any other backend — only your configuration changes.

```php
use Kinetis\Config\Config;
use Kinetis\QueueSqs\SqsClientFactory;
use Kinetis\QueueSqs\SqsQueue;

$queue = new SqsQueue(SqsClientFactory::fromConfig($config));

$queue->push(new SendWelcomeEmail($email, $name), queue: 'default');
```

## Configuring

```
QUEUE_CONNECTION=sqs
QUEUE_SQS_REGION=us-east-1
```

Credentials are never read from Kinetis config — AsyncAws's own default
credential provider chain (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, or
an IAM role) resolves them. A `push()`/`pop()` queue name resolves
directly to an SQS queue of that name — create it ahead of time; this
package never creates one automatically. Optional:
`QUEUE_SQS_ENDPOINT` (an SQS-compatible local service, e.g. LocalStack),
`QUEUE_SQS_QUEUE_PREFIX` (prepended to every queue name).

## Installation

```sh
composer require kinetis/queue-sqs
```

Requires PHP 8.4+, `kinetis/framework`, `kinetis/queue`, and
`kinetis/revolt-http-client`. Full documentation:
[docs.kinetis.dev/queue-sqs.html](https://docs.kinetis.dev/queue-sqs.html).

## License

MIT — see [LICENSE](../../LICENSE).
