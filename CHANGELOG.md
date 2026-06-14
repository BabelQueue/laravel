# Changelog

All notable changes to `babelqueue/laravel` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the versioning policy at [babelqueue.com](https://babelqueue.com).

## [1.2.0] - 2026-06-14

### Added
- **Apache ActiveMQ Artemis drop-in driver** (`babelqueue-artemis`) — a polyglot Artemis
  queue over **STOMP**, the PHP path to [§7 of the broker-bindings
  contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-activemq-artemis)
  (ADR-0018). It is the **consume** half for PHP: Java (JMS) / .NET / Node / Python / Go
  produce on an Artemis address and a Laravel `queue:work babelqueue-artemis` worker
  consumes it — Artemis bridges STOMP ↔ AMQP 1.0 ↔ JMS on the same address.
  `BabelQueueArtemisQueue` (on the pure-PHP `stomp-php` client) sends `ShouldQueuePolyglot`
  jobs as the canonical envelope with the §7 STOMP headers (`content-type`,
  `correlation-id` = `trace_id`, and the string `bq_schema_version`/`bq_source_lang`/
  `bq_attempts`/`bq_app_id`); `pop()` subscribes with `client-individual` ack and wraps each
  frame as `BabelQueueArtemisJob`, where `delete()` ACKs and `release()` republishes with an
  incremented attempt counter then ACKs (at-least-once). **Routing is body-authoritative**
  (§7.8): the dispatcher routes on the envelope's `job` URN, since a STOMP frame cannot set
  the `x-opt-jms-type` annotation. `BabelQueueArtemisConnector` opens no socket eagerly (a
  lazy STOMP connection factory); native scheduled delivery via the `AMQ_SCHEDULED_DELAY`
  header. Registered as the `babelqueue-artemis` connector. Requires `stomp-php/stomp-php`
  (suggested). Proven live with a **Java(JMS) → Laravel(STOMP)** cross-protocol round-trip
  over a real Artemis. The envelope is unchanged (`schema_version: 1`); Artemis is purely
  additive.

## [1.1.0] - 2026-06-12

### Added
- **Amazon SQS drop-in driver** (`babelqueue-sqs`) — a polyglot Amazon SQS queue on
  Laravel's native `sqs` driver, implementing [§3 of the broker-bindings
  contract](https://babelqueue.com). `BabelQueueSqsQueue` (extends Illuminate
  `SqsQueue`) emits `ShouldQueuePolyglot` jobs as the canonical envelope and projects it
  onto native SQS `MessageAttributes` (`bq-job`/`bq-trace-id`/`bq-message-id`/
  `bq-schema-version`/`bq-source-lang`/`bq-created-at`) — so a Go/Python/… consumer can
  route and trace without decoding the body; standard Laravel jobs pass straight through
  (no attributes). `BabelQueueSqsConnector` mirrors the stock `SqsConnector` (credentials,
  prefix/suffix) but returns the polyglot queue; `BabelQueueSqsJob` re-wraps the reserved
  `SqsJob` so consumption routes by URN through the dispatcher. Registered as the
  `babelqueue-sqs` connector. Requires `aws/aws-sdk-php` (suggested). The envelope is
  unchanged (`schema_version: 1`); SQS is purely additive.

## [1.0.0] - 2026-06-07

**1.0.0 — the public API is now SemVer-stable**: breaking changes require a MAJOR,
following the deprecation policy. The wire envelope is unchanged
(`schema_version: 1`). Full reference at [babelqueue.com](https://babelqueue.com).

### Changed
- Require `babelqueue/php-sdk ^1.0`.

### Internal
- CI runs **Larastan (PHPStan level 6)** over `src` and enforces a **>=90%
  line-coverage gate** (`bin/check-coverage.php`). Type-safety fixes surfaced by the
  analysis (Redis `pop()` narrows to `RedisJob`; typed facade `@method` payloads) —
  no behaviour change.
- Coverage raised **40% → 90%**: the RabbitMQ driver/job/connector are tested
  against a mocked AMQP channel (no broker), and the Redis driver round-trips
  through a real Redis (`predis`, dev-only) in the coverage job's redis service.

## [0.3.0] - 2026-06-06

### Changed
- Raise the core dependency to `babelqueue/php-sdk ^0.3`. The framework-less core
  now also ships consumer-side validation and reference Redis/RabbitMQ transports;
  this adapter's own behaviour is unchanged.

### Notes
- The version jumps to **0.3.0** (skipping 0.2.0) to align the PHP packages —
  `php-sdk`, `laravel`, `symfony` — on a single version line.

## [0.1.0] - 2026-06-06

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

[Unreleased]: https://github.com/babelqueue/laravel/compare/v1.2.0...HEAD
[1.2.0]: https://github.com/babelqueue/laravel/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/babelqueue/laravel/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/babelqueue/laravel/compare/v0.3.0...v1.0.0
[0.3.0]: https://github.com/babelqueue/laravel/compare/v0.1.0...v0.3.0
[0.1.0]: https://github.com/babelqueue/laravel/releases/tag/v0.1.0
