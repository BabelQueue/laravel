<?php

declare(strict_types=1);

namespace BabelQueue\Queue\Jobs;

use BabelQueue\Contracts\PolyglotMessage;
use BabelQueue\Queue\BabelQueueArtemisQueue;
use BabelQueue\Queue\Concerns\ParsesPolyglotEnvelope;
use Illuminate\Container\Container;
use Illuminate\Queue\Jobs\Job;
use Stomp\Transport\Frame;

/**
 * A read STOMP frame from Apache Artemis wrapped as a polyglot consume job.
 *
 * Acknowledgement is manual (`client-individual`): delete() ACKs the delivery and release()
 * republishes a fresh copy (with an incremented attempt counter) before ACKing the original,
 * giving at-least-once semantics.
 *
 * The shared trait supplies URN/data/meta parsing and routes fire()/failed() through the
 * dispatcher (by the body's `job` URN — §7.8 body-authoritative routing), so Artemis consumption
 * behaves identically to Redis/RabbitMQ.
 *
 * Note: signatures intentionally mirror the untyped parent/contract methods so the class stays
 * compatible across Laravel versions; types are documented.
 */
class BabelQueueArtemisJob extends Job implements PolyglotMessage
{
    use ParsesPolyglotEnvelope;

    public function __construct(
        Container $container,
        protected BabelQueueArtemisQueue $artemis,
        protected Frame $frame,
        string $connectionName,
        string $queue,
    ) {
        $this->container = $container;
        $this->connectionName = $connectionName;
        $this->queue = $queue;
    }

    /**
     * Get the raw, encoded payload (the STOMP frame body).
     */
    public function getRawBody(): string
    {
        return (string) $this->frame->body;
    }

    /**
     * Get the unique identifier for the job: the envelope's meta.id (cross-language id), else the
     * STOMP message-id header.
     */
    public function getJobId(): string
    {
        $metaId = $this->getMeta()['id'] ?? null;

        if ($metaId !== null && $metaId !== '') {
            return (string) $metaId;
        }

        return (string) ($this->frame['message-id'] ?? '');
    }

    /**
     * Number of times this job has been attempted (this delivery included).
     */
    public function attempts(): int
    {
        return $this->priorAttempts() + 1;
    }

    /**
     * Delete the job from the queue by acknowledging its delivery.
     */
    public function delete(): void
    {
        parent::delete();

        $this->artemis->ackFrame($this->frame);
    }

    /**
     * Release the job back onto the queue, optionally after a delay.
     *
     * @param  int  $delay
     */
    public function release($delay = 0): void
    {
        parent::release($delay);

        // Republish first (at-least-once), then ack the original delivery.
        $this->artemis->republish($this->frame, $this->queue, $this->attempts(), (int) $delay);

        $this->artemis->ackFrame($this->frame);
    }

    /**
     * Attempts already recorded before this delivery. The STOMP `bq_attempts` header is
     * authoritative (we maintain it on republish); otherwise we fall back to the envelope's
     * top-level "attempts" for externally produced messages that only set it in the body.
     */
    protected function priorAttempts(): int
    {
        $attempts = $this->frame['bq_attempts'] ?? null;

        if ($attempts !== null && $attempts !== '') {
            return (int) $attempts;
        }

        return (int) ($this->envelope()['attempts'] ?? 0);
    }
}
