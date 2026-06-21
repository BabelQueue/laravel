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

This is the PHP/Laravel SDK. The full cross-language standard and the canonical
wire contract are documented at **[babelqueue.com](https://babelqueue.com)**.

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

    // Apache ActiveMQ Artemis over STOMP (the §7 path; requires stomp-php/stomp-php).
    // Consumes messages produced by the Java (JMS) / .NET / Node / Python / Go Artemis
    // SDKs — Artemis bridges STOMP ↔ AMQP 1.0 ↔ JMS on the same address.
    'bq-artemis' => [
        'driver'             => 'babelqueue-artemis',
        'host'               => env('ARTEMIS_HOST', '127.0.0.1'),
        'port'               => env('ARTEMIS_STOMP_PORT', 61613),
        'username'           => env('ARTEMIS_USER', 'artemis'),
        'password'           => env('ARTEMIS_PASSWORD', 'artemis'),
        'queue'              => 'default',
        'destination_prefix' => '',   // e.g. '/queue/' if your Artemis needs the anycast prefix
        'read_timeout'       => 1,    // seconds a pop() waits before returning empty
    ],
],
```

## The wire envelope

Every message is encoded as this frozen, `schema_version: 1` envelope
(full spec at [babelqueue.com](https://babelqueue.com)):

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

### Idempotent consumption (opt-in)

Brokers deliver **at least once**: a redelivery after a missed ack — or a
fan-out — can hand a consumer the *same* message twice. Enable idempotency to
dedupe deliveries on the envelope's canonical per-message identity, `meta.id`,
so a duplicate delivery is a no-op and the first delivery runs the handler
**exactly once**. It is off by default and never changes the wire envelope.

Enable in `config/babelqueue.php`:

```php
'idempotency' => [
    'enabled'    => true,
    'store'      => 'redis',   // 'redis' | 'database' | 'memory'
    'connection' => null,      // Redis/DB connection name (null = its default)
    'ttl'        => 3600,      // in-flight claim TTL (seconds) — crash backstop
],
```

The default backends are **Laravel-native** and reuse infrastructure you already
run:

- `redis` — over a Laravel Redis (predis) connection.
- `database` — over a Laravel DB connection (PostgreSQL / MySQL / SQLite). Create
  the table once at deploy time:
  ```php
  DB::connection(config('babelqueue.idempotency.connection'))
      ->getPdo()
      ->exec(\BabelQueue\Idempotency\PdoStore::ddl(config('babelqueue.idempotency.table')));
  ```
- `memory` — single-process only (tests / a lone worker); not shared, not
  persistent.

`redis` and `database` are **claiming** stores: when two workers receive the same
`meta.id` concurrently, exactly one runs the handler and the other parks for
redelivery — closing the in-flight window, not just deduping after success.

It binds the php-sdk `IdempotencyStore` in the container, so you can rebind it to
a custom store:

```php
$this->app->instance(\BabelQueue\Idempotency\IdempotencyStore::class, new MyStore());
```

A thrown handler leaves the id **unmarked**, so retry / DLQ still apply and a
later delivery re-runs it. A message with no usable `meta.id` always runs
(fail-open).

## Testing

```bash
composer install
vendor/bin/phpunit
```

## Links

- Website & docs (incl. the canonical wire contract): <https://babelqueue.com>
- Changelog: [CHANGELOG.md](CHANGELOG.md)

## License

MIT © Muhammet Şafak. See [LICENSE](LICENSE).
