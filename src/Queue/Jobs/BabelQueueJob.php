<?php

declare(strict_types=1);

namespace BabelQueue\Queue\Jobs;

use BabelQueue\Contracts\PolyglotMessage;
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
 *     instead of resolving payload['job'] as a PHP class.
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
}
