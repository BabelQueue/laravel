<?php

declare(strict_types=1);

namespace BabelQueue\Queue\Jobs;

use BabelQueue\Consumer\BabelQueueDispatcher;
use BabelQueue\Contracts\PolyglotMessage;
use BabelQueue\Exceptions\InvalidEnvelopeException;
use BabelQueue\Queue\Concerns\ParsesPolyglotEnvelope;
use Illuminate\Queue\Jobs\RedisJob;

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
 *     (DLQ `poison`, else the failed path) and removed from `:reserved` instead of being retried forever.
 */
class BabelQueueJob extends RedisJob implements PolyglotMessage
{
    use ParsesPolyglotEnvelope;

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
     */
    public function isPoison(): bool
    {
        return $this->getReservedJob() === $this->getRawBody();
    }

    /**
     * Run the job. A poison body is quarantined, never dispatched to a handler and never released.
     */
    public function fire()
    {
        if ($this->isPoison()) {
            $this->quarantine();

            return;
        }

        $this->container->make(BabelQueueDispatcher::class)->dispatch($this);
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
