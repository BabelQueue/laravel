# BabelQueue for Laravel

[![CI](https://github.com/babelqueue/laravel/actions/workflows/ci.yml/badge.svg)](https://github.com/babelqueue/laravel/actions/workflows/ci.yml)
[![Packagist](https://img.shields.io/packagist/v/babelqueue/laravel.svg)](https://packagist.org/packages/babelqueue/laravel)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

> **Polyglot Queues, Simplified.** A drop-in Laravel queue driver that produces a
> strict, language-agnostic JSON envelope — so Go, Python, Java, .NET and Node.js
> services can consume your jobs without PHP's `serialize()`.

Laravel's native queue serialises jobs with PHP `serialize()`, producing an
object graph only PHP can read. BabelQueue replaces just the **serialization
layer** with a frozen JSON envelope and **URN-based routing**, over the broker you
already run (Redis or RabbitMQ). No sidecar, no proxy, no broker plugin.

This is the PHP/Laravel SDK. The full cross-language standard lives at
**[babelqueue.com](https://babelqueue.com)**; the canonical contract is the
[`.ssot/contracts/`](../.ssot/contracts/) directory.

---

## Requirements

- PHP `^8.2`
- Laravel `^11.0 | ^12.0`
- Redis **or** RabbitMQ

## Installation

```bash
composer require babelqueue/laravel
php artisan vendor:publish --tag=babelqueue-config
```

Add a polyglot connection to `config/queue.php`:

```php
'connections' => [
    'bq-redis' => [
        'driver'      => 'babelqueue-redis',
        'connection'  => 'default',   // an illuminate/redis connection
        'queue'       => 'default',
        'retry_after' => 90,
    ],

    // RabbitMQ alternative
    'bq-rabbit' => [
        'driver'        => 'babelqueue-rabbitmq',
        'host'          => env('RABBITMQ_HOST', '127.0.0.1'),
        'port'          => env('RABBITMQ_PORT', 5672),
        'user'          => env('RABBITMQ_USER', 'guest'),
        'password'      => env('RABBITMQ_PASSWORD', 'guest'),
        'vhost'         => env('RABBITMQ_VHOST', '/'),
        'queue'         => 'default',
        'exchange'      => '',
        'exchange_type' => 'direct',
    ],
],
```

## The wire envelope

Every message is encoded as this frozen, `schema_version: 1` envelope
(full spec: [`.ssot/contracts/message-envelope.md`](../.ssot/contracts/message-envelope.md)):

```json
{
  "job": "urn:babel:orders:created",
  "trace_id": "7b3f9c2a-e41d-4f88-9b2a-1c0d5e6f7a8b",
  "data": { "order_id": 1042, "amount": 99.90 },
  "meta": { "id": "…", "queue": "default", "lang": "php", "schema_version": 1, "created_at": 1749132727000 },
  "attempts": 0
}
```

- **`job`** — the message URN, never a class name. Convention: `urn:babel:<context>:<event>`.
- **`trace_id`** — cross-service correlation id, preserved across every hop.
- **`data`** — your pure-JSON payload.

## Producing messages

**Typed job (primary):**

```php
use BabelQueue\Contracts\ShouldQueuePolyglot;

final class CreateOrder implements ShouldQueuePolyglot
{
    public function __construct(private int $orderId, private float $amount) {}

    public function getBabelUrn(): string
    {
        return 'urn:babel:orders:created';
    }

    public function toPayload(): array
    {
        return ['order_id' => $this->orderId, 'amount' => $this->amount];
    }
}

CreateOrder::dispatch(1042, 99.90)->onConnection('bq-redis');
```

**Facade (sugar):**

```php
use BabelQueue\Facades\BabelQueue;

BabelQueue::publish('urn:babel:orders:created', ['order_id' => 1042, 'amount' => 99.90]);
```

Continuing a trace from a handler? Implement `BabelQueue\Contracts\HasTraceId` on
the downstream job (or pass the `traceId` argument) and the inbound `trace_id`
is forwarded instead of a new one being minted.

## Consuming messages

Map URNs to handlers in `config/babelqueue.php`:

```php
'handlers' => [
    'urn:babel:orders:created' => \App\Consumers\OnOrderCreated::class,
],
```

```php
final class OnOrderCreated
{
    // $data, $meta, $traceId and $message are injected by name.
    public function handle(array $data, array $meta, string $traceId): void
    {
        logger()->info('order created', ['id' => $data['order_id'], 'trace' => $traceId]);
    }

    // Optional: called once when retries are exhausted.
    public function failed(array $data, ?\Throwable $e): void
    {
        report($e);
    }
}
```

Run a worker against the polyglot connection like any other:

```bash
php artisan queue:work bq-redis
```

### Unknown URNs

`config/babelqueue.php` → `on_unknown_urn`: `fail` (default) · `delete` ·
`release` · `dead_letter`.

### Dead-letter queue (cross-language)

Enable in `config/babelqueue.php`:

```php
'dead_letter' => [
    'enabled' => true,
    'suffix'  => '.dlq',   // failures from "orders" go to "orders.dlq"
],
```

Permanently-failed (and, with `on_unknown_urn => 'dead_letter'`, unroutable)
messages are republished to the DLQ as the **same** envelope plus an additive
`dead_letter` block (`reason`, `error`, `failed_at`, `original_queue`,
`attempts`, `lang`). Because the DLQ is an ordinary queue, any SDK can triage it.

## Testing

```bash
composer install
vendor/bin/phpunit
```

## Links

- Website & docs: <https://babelqueue.com>
- Canonical contract (SSOT): [`.ssot/contracts/`](../.ssot/contracts/)
- Changelog: [CHANGELOG.md](CHANGELOG.md)

## License

MIT © Muhammet Şafak. See [LICENSE](LICENSE).
