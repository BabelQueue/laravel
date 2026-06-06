# Changelog

All notable changes to `babelqueue/laravel` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**); see [`.ssot/contracts/versioning-policy.md`](../.ssot/contracts/versioning-policy.md).

## [Unreleased]

### Added
- Polyglot Redis (`babelqueue-redis`) and RabbitMQ (`babelqueue-rabbitmq`) queue
  drivers that emit the canonical JSON envelope (`schema_version` 1).
- URN-based routing via `config/babelqueue.php` and `BabelQueueDispatcher`
  (`handle($data, $meta, $traceId, $message)`, optional `failed()`).
- Required top-level **`trace_id`** with propagation via the optional
  `BabelQueue\Contracts\HasTraceId` contract (ADR-0005). Carried in the AMQP
  `correlation_id` property on RabbitMQ.
- **`BabelQueue\Facades\BabelQueue::publish()`** producer facade — sugar over the
  `ShouldQueuePolyglot` interface, sharing the same encoder (ADR-0007).
- **Cross-language dead-letter queue** (`dead_letter` config + the `dead_letter`
  `on_unknown_urn` strategy): failed/unroutable messages are republished as the
  same envelope plus an additive `dead_letter` block (ADR-0009).
- `on_unknown_urn` strategies: `fail`, `delete`, `release`, `dead_letter`.

### Notes
- Pre-1.0: the public API may still change before the `1.0.0` tag.
- Requires PHP `^8.2` and Laravel `^11.0 | ^12.0`; Redis or RabbitMQ.

[Unreleased]: https://github.com/babelqueue/laravel/commits/main
