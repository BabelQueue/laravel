# Changelog

All notable changes to `babelqueue/laravel` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the versioning policy at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

Aligns the conformance report with ADR-0037 (conformance v1.8.0). **No behaviour changes**: nothing under `src/`
changes, and the envelope stays at `schema_version: 1`.

### Changed
- The vendored conformance suite and the workflow pin move to conformance v1.8.0.
- **Version-gate sections (ADR-0037 decision 4, the Laravel exception, GR-6).** The `babelqueue-sqs` and Artemis
  drivers keep handling a version-gated delivery in `BabelQueueDispatcher` (with the DLQ enabled an envelope with a
  `dead_letter` block, `reason: "poison"`, is published and the delivery acked; with it disabled the worker's retry and
  then `failed_jobs` take it; `on_unknown_urn` is ignored), so they do not follow the ADR-0037 settle rule (the raw
  body to `<queue>.dlq`). A DLQ reader must not assume a Laravel-written record of a version-gated delivery holds the
  original bytes. Every case of `version_gate_sqs` and `version_gate_artemis` is reported as `skip` with the reason
  `ADR-0037 decision 4` (`tests/Feature/VersionGateConformanceTest.php`); `version_gate_pulsar` and
  `version_gate_asb` are absent (no such binding in this package).
- **`failed_jobs` with the DLQ disabled is a recorded exception (ADR-0037 decision 4).** A poison body and an
  exhausted job on a `babelqueue-*` connection with the BabelQueue DLQ off are failed into Laravel's `failed_jobs`
  (`php artisan queue:failed` / `queue:retry` keep them inspectable); with the DLQ enabled they are dead-lettered as
  the ADR-0035 record, as before. The exception is now recorded in `conformance-known-gaps.json` (`exceptions`,
  informational: no conformance case exercises the DLQ-disabled path, so it is not a failing case).

## [1.9.0] - 2026-10-25

Implements ADR-0036 decisions 4 and 7 for the Laravel Redis driver (conformance v1.7.0, new core section
`identity_verdict`). The envelope stays at `schema_version: 1`. Only `babelqueue-redis` connections change
(Laravel RabbitMQ, SQS and Artemis behave as before; the standard `redis` connection is untouched). This is a
**MINOR** with behaviour changes.

Requires `babelqueue/php-sdk` ^1.26.0.

### Behaviour changes
- **An identity-less body is poison on Redis, whatever `on_unknown_urn` says (broker-bindings §1.1, decision 4).**
  With the dead-letter queue enabled, a body that has neither a valid `job` nor a valid `urn` (the conformance
  `identity_verdict` rule) is written to `<queue>.dlq` as the ADR-0035 record with `reason: "poison"` and removed
  from `:reserved`; it never reaches a handler or the unknown-URN strategies (`delete`, `release`, `fail` and
  `dead_letter` all behave the same for it). Before, it took the `on_unknown_urn` path (for `delete` it was dropped,
  for `fail` it was retried). A valid envelope whose URN has no handler is unchanged. With the DLQ disabled nothing
  changes (the existing `failed_jobs` known gap).
- **A failed dead-letter write no longer moves the message to `:delayed` (broker-bindings §1.7, decision 7).** When the
  DLQ write throws (a poison body, a version-gated body, `on_unknown_urn: dead_letter`, or retries exhausted), the
  reservation is neither acknowledged nor released: the member stays in `:reserved`, the failure is logged and the
  new `BabelQueue\Consumer\DeadLetterWriteFailed` is rethrown for the worker to report, and the `retry_after`
  recovery brings the message back (counting an attempt) to try the write again. Before, the exception reached
  Laravel's worker, which called `deleteAndRelease` and retried the message through `:delayed` (a poison body was
  re-run against a handler that cannot take it). With `retry_after: null` the member stays in `:reserved` until an
  operator acts.
- **The `failed` DLQ record is written before the reservation is deleted** (`BabelQueueJob::fail()`). Laravel deletes
  first, so a failed write used to lose the message; now it stays in `:reserved`. The handler's `failed()` hook and the
  `JobFailed` event are unchanged, and the record is written once.

### Added
- `BabelQueue\Consumer\DeadLetterWriteFailed`: thrown by `DeadLetterPublisher::publish()` when the write to the
  dead-letter queue fails (the original exception is `getPrevious()`). It extends the core's
  `BabelQueue\Consume\DeadLetterWriteFailed` (php-sdk 1.26), so a handler for the core type catches it too.
- `BabelQueueDispatcher::deadLetterFailure()` and a `$deadLettered` argument on `BabelQueueDispatcher::fail()`.
- `BabelQueueJob::hasIdentity()`, the identity rule `fire()` acts on. The `identity_verdict` conformance runner runs
  through it; 2 of its 97 cases (a wrong-typed `attempts` or `data` next to a valid identity) are listed in
  `conformance-known-gaps.json`: Laravel applies the identity rule only, not the strict decoder's typed-member
  pass (ADR-0036 decision 4 is scoped to identity-less bodies).
- A live-Redis integration test for both behaviours and the `identity_verdict` conformance runner; the vendored suite
  and the workflow pins move to conformance v1.7.0.

## [1.8.0] - 2026-10-24

Conformance v1.6.0 (ADR-0033 / ADR-0034). The envelope stays at `schema_version: 1`;
`conformance-known-gaps.json` is now empty.

Requires `babelqueue/php-sdk` ^1.25.0.

### Changed
- Poison DLQ record shape follows ADR-0035 (`PoisonCopy` already splices strict objects byte-for-byte and wraps the rest, base64 + `raw_encoding` only for invalid UTF-8; the consume-side invalid-UTF-8 poison detection is under Fixed). `AdapterConformanceTest` now runs the `poison_dlq` conformance section (43 cases).

### Fixed
- **Invalid UTF-8 is poison on the Redis consume path (broker-bindings §1.1, both layouts).** `BabelQueueJob::isPoison()` now also treats a raw body or reserved member that is not valid UTF-8 as poison. The reserve script (cjson) passes high bytes through, so such an envelope with a numeric `attempts` was reserved as a re-encoded member, missed the identity signal, fell into the unknown-URN path and could be written back to `:delayed` by a retry. It is now dead-lettered on first delivery with reason `poison` as the ADR-0035 base64 wrapper (`raw_encoding: "base64"`), never reaches a handler and is removed from `:reserved` by its exact reserved bytes.
- **Poison detection and DLQ copy (ADR-0034 §10).** `BabelQueueJob::isPoison()` now uses the reserve
  script's own signal (`getReservedJob() === getRawBody()`, broker-bindings.md §1.1) instead of re-reading the
  body with PHP `json_decode`, which disagrees with cjson on inputs such as `1e400`. The DLQ copy of a poison body
  is written byte-for-byte (`Consumer\PoisonCopy`): it never throws on invalid UTF-8 or `1e400`, keeps `{}` and
  big integers exact, and carries non-UTF-8 bytes as base64 with `raw_encoding: "base64"` (additive, ADR-0009).
- **Poison bodies no longer loop or crash.** A reserved `{job, job}` poison body (undecodable, or
  `attempts` not a number) is recognised by `BabelQueueJob`: `fire()` never reaches the handler, `attempts()`
  does not raise a `TypeError`, and the job is dead-lettered as `poison` (or failed when no dead-letter
  publisher is bound) and deleted from `:reserved` instead of being released forever. Standard Laravel jobs
  are unaffected (GR-6).
- **Release keeps the body byte-for-byte (ADR-0031).** `attempts` is rewritten in place at its literal span, so key
  order, escaping, `{}` / `[]`, floats and big integers survive; a body that cannot be rewritten safely falls
  back to the stock release.
- **Reserve Lua is `pcall`-hardened (§1.1).** The decode and the re-encode both run under `pcall`, and the raw
  job is always placed in `:reserved` first. A body that fails to decode, or whose `attempts` is not a number, is
  reserved raw and returned as `{job, job}` instead of failing after the `LPOP`.
- **Falsy reserved members.** A reserved member of `""` or `"0"` was dropped by the parent's truthiness test in
  `pop()`; it is now handed to the dispatcher so it can be dead-lettered.

### Changed
- **SQS / Artemis `attempts()` (broker-bindings §3.6 / §7.5).** `BabelQueueSqsJob::attempts()` is
  `max(body.attempts, ApproximateReceiveCount - 1)` and `BabelQueueArtemisJob::attempts()` is
  `max(body.attempts, delivery-count)`; an absent or non-numeric broker count is ignored. A first delivery
  reads 0. This applies to `babelqueue-sqs` / `babelqueue-artemis` connections only; the standard `sqs`
  driver and Laravel's own workers are untouched (GR-6). The Artemis republish still records `attempts + 1`.
- **Redis connector defaults.** `babelqueue-redis` falls back to `retry_after = 90` (was 60)
  when the connection omits it.
- **Redis delay score.** Numeric delays are rounded up (`ceil`) so the delayed score is
  `floor(now) + ceil(delay)` (§1.6).
- **`pushRaw()` return value.** `BabelQueueRabbitQueue` and `BabelQueueArtemisQueue` now return `null` instead of
  the id when `meta.id` is not a string.

### CI
- The `test` job (`ci.yml`) and the `validate` job (`release.yml`) run a `redis:7` service with
  `BABELQUEUE_TEST_REDIS` and `BQ_REQUIRE_REDIS=1`, so the live Redis conformance blocks can no longer be
  skipped silently in the jobs that produce `results.json`.
- `BQ_CONFORMANCE_VERSION` is 1.6.0 in `ci.yml` and `release.yml`.

### Tests
- New `tests/Feature/BabelQueuePoisonJobTest.php` (poison path) and extended pop / release tests.
- Vendored conformance v1.6.0; the `redis_delay` and `redis_layout` `delayed_score` cases run offline.
- New `tests/Integration/RedisLayoutConformanceTest.php` runs the `redis_layout` `migrate` / `reserve` /
  `pcall_poison` / `release` blocks against a real Redis (`BABELQUEUE_TEST_REDIS`). Without Redis the blocks
  are skipped with an ADR-0033 reason; set `BQ_REQUIRE_REDIS` to make an unreachable Redis a failure. The
  legacy `:processing` blocks are skipped as not applicable (Laravel's own reserved-set layout).

## [1.7.0] - 2026-10-19

Requires `babelqueue/php-sdk` ^1.24.0 (ASCII trim and the non-string `job` rule, ADR-0032).

### Added
- **SQS delay limits (section 3.4).** `BabelQueueSqsQueue::later()` throws `BabelQueueException` for a polyglot
  job whose delay exceeds 900 seconds or that carries a per-message delay on a `.fifo` queue.
- **Artemis `bq_schema_version` gate (section 7.7).** A frame whose `bq_schema_version` is not blank or `1` is
  rejected without decoding the body (shared `Support\SchemaVersionGate`, also used by the SQS driver).
- **Conformance known-gaps gate (tests/CI only).** `conformance-known-gaps.json` lists the SQS section 3.6 and
  Artemis section 7.5 `attempts` reconciliation cases (the drivers keep Laravel's own `attempts()`); they are
  reported as honest `fail`. CI and release fail on a stale gap, an unlisted `fail`, or a gap reason without an
  `ADR-NNNN` / `.ssot/plans/` reference.

### Changed
- **Internal:** the schema-version stub envelope used by the SQS and Artemis jobs now lives in
  `Support\SchemaVersionGate::stubEnvelope()` instead of being duplicated in both job classes (no behaviour change).

## [1.6.0] - 2026-10-17

MINOR: retry/dead-letter behaviour change and a raised core floor (ADR-0031). The envelope stays at
`schema_version: 1`.

### Changed
- Rabbit/Artemis `release()` republishes the body byte for byte; only an inbound `urn` alias is moved into `job`
  (ADR-0031 (c)) and the section 10 forbidden keys (`timestamp`, `meta.ts`/`max_retries`/`attempts`/`source`) are
  dropped (ADR-0031 (b)). Unknown top-level and `meta` keys, nested `{}` and big integers are never re-serialised.
- The dead-letter publisher keeps a poison body (e.g. a list `data`) as the original JSON object verbatim, else
  `{"raw": "<body>"}` — the same shape as php-sdk's `Consume\DeadLetterPublisher`.
- Requires `babelqueue/php-sdk` `^1.23.0`.

### Known limits
- The Redis driver's atomic pop/release goes through Laravel's Lua script (`cjson`), which rewrites the body on every
  retry: integers above ~14 significant digits lose precision, `[]` becomes `{}`, an inbound `urn` alias is not moved
  into `job` and the section 10 forbidden keys are not removed. It predates 1.23 and deviates from ADR-0031 (b)/(c); a
  BabelQueue-owned pop/release script is a follow-up. See the README "Known limits" section.

## [1.5.1] - 2026-10-11

### Fixed
- `babelqueue-sqs` `pop()` now requests `ApproximateReceiveCount` via `MessageSystemAttributeNames` instead of
  `AttributeNames`. It is a message system attribute, not a queue attribute; the current AWS SDK types reject it
  under `AttributeNames` (Larastan failure). The wire/SQS behaviour is unchanged — `Attributes['ApproximateReceiveCount']`
  still comes back on each message.
- `composer.json` now declares `"conflict": {"aws/aws-sdk-php": "<3.306.2"}`. Requires `aws/aws-sdk-php` >= 3.306.2
  (where `MessageSystemAttributeNames` was introduced): an older SDK silently drops the unknown parameter, so SQS
  would never return `ApproximateReceiveCount` and attempt counting would break (an `Undefined array key` error at `attempts()`, so the job is never failed or dead-lettered).

## [1.5.0] - 2026-10-10

MINOR: new public API on `BabelQueueSqsJob` alongside the fix below. The envelope stays at
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
