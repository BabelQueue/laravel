<?php

declare(strict_types=1);

namespace BabelQueue\Queue\Jobs;

use BabelQueue\Contracts\PolyglotMessage;
use BabelQueue\Queue\Concerns\ParsesPolyglotEnvelope;
use Illuminate\Queue\Jobs\SqsJob;

/**
 * An SQS-backed consume job for polyglot messages.
 *
 * It keeps all of {@see SqsJob}'s reservation/ack/release machinery (so retry
 * semantics and the visibility-timeout / ApproximateReceiveCount attempt counting
 * work exactly as in stock Laravel) and layers on top:
 *
 *   - URN / data / meta parsing of the raw JSON body (via the shared trait);
 *   - fire()/failed() routed through the {@see \BabelQueue\Consumer\BabelQueueDispatcher}
 *     instead of resolving payload['job'] as a PHP class.
 */
class BabelQueueSqsJob extends SqsJob implements PolyglotMessage
{
    use ParsesPolyglotEnvelope;

    /**
     * Prefer the polyglot meta.id as the job identifier, falling back to the
     * SQS-assigned id the parent reads.
     */
    public function getJobId()
    {
        return $this->getMeta()['id'] ?? parent::getJobId();
    }
}
