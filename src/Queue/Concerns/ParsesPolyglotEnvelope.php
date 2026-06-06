<?php

declare(strict_types=1);

namespace BabelQueue\Queue\Concerns;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Consumer\BabelQueueDispatcher;
use Throwable;

/**
 * Shared consume-side behaviour for the Redis and RabbitMQ polyglot jobs.
 *
 * It decodes the { job, trace_id, data, meta } envelope once and, crucially,
 * replaces the base job's fire()/failed() — which would try to resolve
 * payload['job'] as a PHP class — with a hand-off to the
 * {@see BabelQueueDispatcher}, which routes by URN instead.
 *
 * Host classes must extend a Laravel {@see \Illuminate\Queue\Jobs\Job} (for
 * $this->container and getRawBody()) and implement
 * {@see \BabelQueue\Contracts\PolyglotMessage}.
 */
trait ParsesPolyglotEnvelope
{
    /** @var array<string, mixed>|null Memoised decode of the raw body. */
    private ?array $polyglotEnvelope = null;

    /**
     * The message URN. Canonical wire field is "job"; "urn" is accepted as an
     * alias so producers that named the field literally still interoperate.
     */
    public function getUrn(): string
    {
        $envelope = $this->envelope();

        return (string) ($envelope['job'] ?? $envelope['urn'] ?? '');
    }

    /**
     * The cross-service correlation id (trace_id). Returns an empty string when
     * the producer omitted it (e.g. a non-conformant or legacy message).
     */
    public function getTraceId(): string
    {
        return (string) ($this->envelope()['trace_id'] ?? '');
    }

    /**
     * @return array<string, mixed>
     */
    public function getData(): array
    {
        return (array) ($this->envelope()['data'] ?? []);
    }

    /**
     * @return array<string, mixed>
     */
    public function getMeta(): array
    {
        return (array) ($this->envelope()['meta'] ?? []);
    }

    /**
     * Run the job by dispatching it to the URN-mapped handler.
     */
    public function fire()
    {
        $this->container->make(BabelQueueDispatcher::class)->dispatch($this);
    }

    /**
     * Forward permanent failure to the URN-mapped handler's failed() hook,
     * instead of the base behaviour that resolves payload['job'] as a class.
     *
     * @param  \Throwable|null  $e
     */
    protected function failed($e)
    {
        $this->container->make(BabelQueueDispatcher::class)
            ->fail($this, $e instanceof Throwable ? $e : null);
    }

    /**
     * Decode the raw JSON body once.
     *
     * @return array<string, mixed>
     */
    protected function envelope(): array
    {
        if ($this->polyglotEnvelope === null) {
            $this->polyglotEnvelope = EnvelopeCodec::decode($this->getRawBody());
        }

        return $this->polyglotEnvelope;
    }
}
