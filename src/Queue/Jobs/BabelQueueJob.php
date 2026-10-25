<?php

declare(strict_types=1);

namespace BabelQueue\Queue\Jobs;

use BabelQueue\Consumer\BabelQueueDispatcher;
use BabelQueue\Consumer\DeadLetterWriteFailed;
use BabelQueue\Contracts\PolyglotMessage;
use BabelQueue\Exceptions\InvalidEnvelopeException;
use BabelQueue\Queue\Concerns\ParsesPolyglotEnvelope;
use Illuminate\Queue\Jobs\RedisJob;
use Throwable;

/**
 * A Redis-backed consume job for polyglot messages.
 *
 * It keeps all of {@see RedisJob}'s reservation/ack/release machinery (so retry
 * semantics, the reserved sorted set and attempt counting all work exactly as
 * in stock Laravel) and layers on top:
 *
 *   - URN / data / meta parsing of the raw JSON body (via the shared trait);
 *   - fire()/failed() routed through the {@see \BabelQueue\Consumer\BabelQueueDispatcher}
 *     instead of resolving payload['job'] as a PHP class;
 *   - poison handling (broker-bindings.md section 1.1, ADR-0033): the reserve script hands over a body that
 *     does not decode to an object with a numeric `attempts` untouched, and such a body is quarantined
 *     (DLQ `poison`, else the failed path) and removed from `:reserved` instead of being retried forever;
 *   - an identity-less body (neither a valid `job` nor a valid `urn`) is poison too, with the DLQ enabled, whatever
 *     `on_unknown_urn` says (broker-bindings.md section 1.1, ADR-0036 decision 4);
 *   - a failed dead-letter write never reaches the worker's release (ADR-0036 decision 7, broker-bindings.md
 *     section 1.7): the reservation stays in `:reserved` and the `retry_after` recovery brings it back.
 */
class BabelQueueJob extends RedisJob implements PolyglotMessage
{
    use ParsesPolyglotEnvelope;

    /** Whether {@see fail()} already wrote the `failed` DLQ record, so {@see failed()} does not repeat it. */
    private bool $deadLettered = false;

    /**
     * Prefer the polyglot meta.id as the job identifier, falling back to the
     * Laravel-style top-level "id" that the parent reads.
     */
    public function getJobId()
    {
        return $this->getMeta()['id'] ?? parent::getJobId();
    }

    /**
     * Whether the body is poison. The reserve script is the authority (broker-bindings.md section 1.2): it hands a
     * poison element over untouched, so the reserved member is the raw body. A decodable envelope is reserved as
     * the re-encoded member with `attempts` incremented, which never equals the raw body. The body is not
     * re-interpreted in PHP: `json_decode` and cjson disagree on some inputs (e.g. `1e400`).
     *
     * One exception is made for invalid UTF-8 (broker-bindings.md section 1.1, both layouts): cjson passes high
     * bytes through unvalidated, so the script reserves such an envelope as a re-encoded member and the
     * identity signal above misses it. A raw body or a reserved member that is not valid UTF-8 is always poison,
     * so it is never handed to a handler, never released and never written back with its bytes altered.
     */
    public function isPoison(): bool
    {
        return $this->getReservedJob() === $this->getRawBody()
            || preg_match('//u', $this->getRawBody()) !== 1
            || preg_match('//u', $this->getReservedJob()) !== 1;
    }

    /**
     * Whether the body carries a valid identity (a valid `job`, else a valid `urn` alias; ADR-0032 section 6).
     * A body without one is poison on Redis with the DLQ enabled (ADR-0036 decision 4); see {@see fire()}.
     */
    public function hasIdentity(): bool
    {
        return $this->getUrn() !== '';
    }

    /**
     * Run the job. A poison body is quarantined, never dispatched to a handler and never released.
     *
     * An identity-less body (no valid `job` and no valid `urn`) is poison as well when the DLQ is enabled: it
     * is written to the DLQ with reason `poison` (the ADR-0035 record) whatever `on_unknown_urn` is, and never
     * reaches the handler / unknown-URN path. With the DLQ disabled nothing changes (the existing known gap).
     *
     * If the dead-letter write itself fails, the reservation is left in `:reserved` (see {@see keepReserved()}).
     */
    public function fire()
    {
        $dispatcher = $this->container->make(BabelQueueDispatcher::class);

        try {
            if ($this->isPoison()) {
                $this->quarantine();

                return;
            }

            if (! $this->hasIdentity() && $dispatcher->quarantinePoison($this)) {
                return;
            }

            $dispatcher->dispatch($this);
        } catch (DeadLetterWriteFailed $e) {
            $this->keepReserved($e);

            throw $e;
        }
    }

    /**
     * Permanent failure (the worker exhausted the retries, or the handler failed the job). The `failed`
     * dead-letter record is written BEFORE the reservation is deleted: Laravel's own order deletes first, so a
     * failed write would lose the message. If the write fails the reservation stays in `:reserved` and the
     * exception is rethrown (ADR-0036 decision 7).
     *
     * @param  \Throwable|string|null  $e
     */
    public function fail($e = null)
    {
        if (! $this->isDeleted()) {
            try {
                $this->container->make(BabelQueueDispatcher::class)
                    ->deadLetterFailure($this, $e instanceof Throwable ? $e : null);
                $this->deadLettered = true;
            } catch (DeadLetterWriteFailed $writeFailed) {
                $this->keepReserved($writeFailed, $e instanceof Throwable ? $e : null);

                throw $writeFailed;
            }
        }

        parent::fail($e);
    }

    /**
     * Run the handler's `failed()` hook; the DLQ record was already written by {@see fail()}.
     *
     * @param  \Throwable|null  $e
     */
    protected function failed($e)
    {
        $this->container->make(BabelQueueDispatcher::class)
            ->fail($this, $e instanceof Throwable ? $e : null, $this->deadLettered);
    }

    /**
     * A dead-letter write failed: neither acknowledge nor release the reservation. Marking the job failed makes
     * Laravel's worker skip its `release()` (which would `deleteAndRelease` it to `:delayed`), and nothing deletes
     * it, so the member stays in `:reserved` until the `retry_after` recovery moves it back (counting an attempt)
     * and the write is tried again. The failure is logged here and the caller rethrows it, so the worker reports it.
     */
    private function keepReserved(DeadLetterWriteFailed $writeFailed, ?Throwable $cause = null): void
    {
        $this->markAsFailed();

        if (! $this->container->bound('log')) {
            return;
        }

        try {
            $this->container->make('log')->error(
                'BabelQueue: the dead-letter write failed; the message stays in :reserved until the retry_after recovery',
                [
                    'queue' => $this->getQueue(),
                    'job_id' => $this->getJobId(),
                    'urn' => $this->getUrn(),
                    'error' => $writeFailed->getMessage(),
                    'cause' => $cause?->getMessage(),
                ],
            );
        } catch (Throwable) {
            // Logging must never turn a kept reservation into a lost one.
        }
    }

    /**
     * Stock `RedisJob::attempts()` adds 1 to whatever `attempts` holds: a missing / null value yields 1
     * for ever (the worker's max-tries check never fires) and a string or array value throws a TypeError.
     * A non-numeric `attempts` counts as 0 here.
     */
    public function attempts()
    {
        $attempts = $this->decoded['attempts'] ?? null;

        return is_int($attempts) || is_float($attempts) ? (int) $attempts + 1 : 1;
    }

    /**
     * The job name for logs and events. The parent reads `payload()['job']`, which is an array-offset
     * access on null (an ErrorException under Laravel's error handler) for a body that is not an object.
     */
    public function getName()
    {
        return $this->getUrn();
    }

    /**
     * Quarantine a poison body: DLQ with reason `poison` and ack when the dead-letter queue is enabled,
     * otherwise mark it failed so it reaches `failed_jobs` (and `:reserved` is cleared) rather than loop.
     */
    private function quarantine(): void
    {
        if ($this->container->make(BabelQueueDispatcher::class)->quarantinePoison($this)) {
            return;
        }

        $this->fail(InvalidEnvelopeException::because('poison', []));
    }
}
