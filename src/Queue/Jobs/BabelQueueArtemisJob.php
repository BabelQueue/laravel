<?php

declare(strict_types=1);

namespace BabelQueue\Queue\Jobs;

use BabelQueue\Contracts\PolyglotMessage;
use BabelQueue\Queue\BabelQueueArtemisQueue;
use BabelQueue\Queue\Concerns\ParsesPolyglotEnvelope;
use BabelQueue\Support\SchemaVersionGate;
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
    use ParsesPolyglotEnvelope {
        envelope as private decodeEnvelope;
    }

    /**
     * Reserved routing URN prefix for a message rejected by the §7.7 version gate; the rejected
     * `bq_schema_version` value is appended. No handler is ever mapped to it.
     */
    public const UNSUPPORTED_SCHEMA_URN_PREFIX = SchemaVersionGate::UNSUPPORTED_URN_PREFIX;

    /**
     * @param  string|null  $rejectedSchemaVersion  the `bq_schema_version` header value when the §7.7
     *                                              gate rejected the message, else null
     */
    public function __construct(
        Container $container,
        protected BabelQueueArtemisQueue $artemis,
        protected Frame $frame,
        string $connectionName,
        string $queue,
        private readonly ?string $rejectedSchemaVersion = null,
    ) {
        $this->container = $container;
        $this->connectionName = $connectionName;
        $this->queue = $queue;
    }

    /**
     * The decoded envelope, or — for a gate-rejected message — a stub that never touches the body: a
     * reserved sentinel URN and the rejected, non-integer `meta.schema_version`, so the dispatcher
     * takes its unsupported-schema-version path (DLQ quarantine or fail) and no handler runs.
     *
     * @return array<string, mixed>
     */
    protected function envelope(): array
    {
        if ($this->rejectedSchemaVersion !== null) {
            return SchemaVersionGate::stubEnvelope($this->rejectedSchemaVersion);
        }

        return $this->decodeEnvelope();
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
