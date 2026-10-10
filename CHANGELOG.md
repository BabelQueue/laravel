# Changelog

All notable changes to `babelqueue/laravel` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the versioning policy at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

MINOR (when released): new public API on `BabelQueueSqsJob` alongside the fix below. The envelope stays at
`schema_version: 1`.

### Added
- `BabelQueueSqsJob::UNSUPPORTED_SCHEMA_URN_PREFIX` (`babelqueue:unsupported-schema-version:`), the reserved routing
  URN prefix of a message rejected by the SQS schema-version gate.
- `BabelQueueSqsJob` takes an optional trailing `?string $rejectedSchemaVersion` constructor argument (the
  rejected `bq-schema-version` value; default `null`). Additive; existing callers are unaffected.

### Fixed
- **`babelqueue-sqs` now honours the `bq-schema-version` message attribute (broker-bindings §3.5 / §3.7).**
  The driver never asked SQS for message attributes, so the version gate could not run. `pop()` now makes its
  own `receiveMessage` call with `MessageAttributeNames = ['All']` and checks the attribute **before** the body
  is decoded: missing or blank (empty / ASCII whitespace only) takes the normal path, any other value that is
  not exactly `"1"` is rejected without decoding and goes through the dispatcher's unsupported-schema-version
  path (DLQ `poison` + ack, or `InvalidEnvelopeException`). Standard Laravel queues are unaffected.
  Needs no newer `babelqueue/php-sdk` than the current `^1.17.0` constraint.

## [1.4.0] - 2026-10-10

MINOR: the schema-version gate under Changed is a behaviour change for shared queues; read it before upgrading.

### Fixed
- **Absent or unsupported `meta.schema_version` is now rejected on consume.** The dispatcher used
  to hand an envelope with a `schema_version` other than `1` (or none) to the handler. Per the
  contract ("consumers MUST reject (or quarantine) versions they do not understand"; same rule as
  the php-sdk core and the other SDKs) it is now never handled, **independent of
  `on_unknown_urn`** (that setting is for URNs without a handler, not for versions): with the DLQ
  enabled it is quarantined (reason `poison`) and acked; otherwise it throws
  `InvalidEnvelopeException` (reason `unsupported_schema_version`) so the worker's retry /
  `failed_jobs` path records it. It is never deleted without a trace.

### Changed
- **Behaviour change: the schema-version gate runs before URN routing, so `on_unknown_urn`
  no longer sees messages with an absent or unsupported `meta.schema_version`.** Previously a
  message on a `babelqueue-*` connection whose `job` was not mapped to a handler was handled by
  `on_unknown_urn` regardless of its `meta`. Now, whenever `job` is non-empty and
  `meta.schema_version` is not exactly `1`, the message is rejected before the handler lookup.
  This affects shared queues: with `on_unknown_urn: delete` (or `release` / `dead_letter`),
  messages that used to be silently dropped for being "unrelated" — a stock Laravel job pushed
  onto the connection (`job: "Illuminate\\Queue\\CallQueuedHandler@call"`, no `meta`), or another
  team's `schema_version: 2` message under a URN this service has no handler for — now surface:
  with the DLQ enabled they are published to the configured DLQ (default `<queue>.dlq`) with reason `poison` (not
  `unknown_urn`) and acked; with the DLQ disabled they throw `InvalidEnvelopeException`
  (reason `unsupported_schema_version`) and follow the worker's retry / `failed_jobs` path.
  No message is lost, but operators who relied on `delete` to swallow such traffic will see new
  failures or DLQ entries. The default `fail` strategy already threw and is unchanged. To keep
  unrelated traffic out, give this service its own queue or connection. An empty `job` (a
  non-JSON body) still goes through `on_unknown_urn` as before.
- Raised the `babelqueue/php-sdk` floor to `^1.17.0` (forbidden-key warnings, list-shaped `data`
  rejection and `{}` encoding, K-15).

### Tests / CI
- **Cross-SDK conformance suite vendored and run through the adapter path.** The canonical
  suite (`CONFORMANCE_VERSION`, `manifest.json`, `fixtures/`, `schema/` — v1.0.0) now lives
  byte-for-byte under `tests/conformance/`. `tests/Feature/AdapterConformanceTest.php` feeds the
  fixtures through the RabbitMQ and Redis job classes, the URN dispatcher and the RabbitMQ
  producer/release path (decode, field preservation, re-emit, rejection of invalid cases).
  A new `conformance` CI job verifies the vendored copy has not drifted from the canonical
  repository and gates `ci-green`.

## [1.3.2] - 2026-10-03

### Changed
- **Release workflow: Packagist credentials moved out of the URL.** The optional
  "Notify Packagist" step now sends `Authorization: Bearer USERNAME:API_TOKEN`
  (Packagist's documented scheme) to `https://packagist.org/api/update-package`
  instead of passing `username` / `apiToken` as query parameters, so the token can
  no longer surface in request logs, proxies or curl error output. Secret names are
  unchanged (`PACKAGIST_USERNAME`, `PACKAGIST_TOKEN`).
- Added `.github/dependabot.yml` (weekly `composer` and `github-actions` updates).

### Tests
- `PolyglotConsumedMessageTest` no longer puts the forbidden `meta.attempts` key in
  its fixture, so it passes against `babelqueue/php-sdk` 1.17.0, which drops that key
  on decode (K-15). No runtime change; the `^1.15.0` constraint already admits 1.17.0.

## [1.3.1] - 2026-06-21

### Changed
- Lifted test coverage for the idempotency wiring back above the 90% gate
  (`PolyglotConsumedMessage` + the service-provider store factory) — tests only,
  no functional change.

## [1.3.0] - 2026-06-21

### Added
- **Opt-in idempotent consumption** — a `babelqueue-*` worker can now dedupe redelivered
  messages on the envelope's canonical `meta.id`, so a duplicate delivery is a no-op and the
  first delivery runs the handler exactly once (the consume-side half of "handlers SHOULD be
  idempotent", ADR-0022). It is **off by default** and **does not touch the frozen wire
  envelope** — dedupe is a consume-side concern, never an envelope change.
  - New `idempotency` block in `config/babelqueue.php`: `enabled` (default `false`), `store`
    (`redis` | `database` | `memory`), `connection`, `ttl`, `table`, `prefix`. Configure via
    `BABELQUEUE_IDEMPOTENCY_*` env vars.
  - Default backends are **Laravel-native**: `redis` maps onto a Laravel Redis (predis)
    connection and `database` onto a Laravel DB connection (PostgreSQL / MySQL / SQLite), both
    bound in the service provider as a php-sdk `IdempotencyStore` (rebindable to a custom store).
    Both are **claiming** stores, so two workers handed the same id never both run the handler —
    the loser parks for redelivery (`ClaimingDispatch`); `memory` is single-process only.
  - The `BabelQueueDispatcher` wraps each URN handler with the php-sdk
    `Idempotent::wrap` / `ClaimingDispatch::wrap` helpers (no logic duplicated). A thrown handler
    leaves the id unmarked, so retry / DLQ still apply.
- Bumped the `babelqueue/php-sdk` constraint to **^1.15.0** (the release that ships the persistent
  `RedisStore` / `PdoStore` and `ClaimingDispatch`).

## [1.2.1] - 2026-06-14

### Internal
- Restore the **≥90% coverage gate** after the v1.2.0 Artemis driver landed: split
  `BabelQueueArtemisConnector` into a unit-testable `configureClient()` (builds the configured
  STOMP client, no socket) and `makeConnection()` (connects), and add targeted tests for the
  remaining `BabelQueueArtemisQueue`/`BabelQueueArtemisJob` branches. **No runtime or API change** —
  the `babelqueue-artemis` driver behaves identically to v1.2.0; this is a tests + behaviour-
  preserving refactor that lifts overall coverage to ~92%.

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

[Unreleased]: https://github.com/babelqueue/laravel/compare/v1.2.1...HEAD
[1.2.1]: https://github.com/babelqueue/laravel/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/babelqueue/laravel/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/babelqueue/laravel/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/babelqueue/laravel/compare/v0.3.0...v1.0.0
[0.3.0]: https://github.com/babelqueue/laravel/compare/v0.1.0...v0.3.0
[0.1.0]: https://github.com/babelqueue/laravel/releases/tag/v0.1.0
