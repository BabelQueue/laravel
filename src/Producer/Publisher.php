<?php

declare(strict_types=1);

namespace BabelQueue\Producer;

use BabelQueue\Support\PolyglotEnvelopeJob;
use DateInterval;
use DateTimeInterface;
use Illuminate\Contracts\Queue\Factory as QueueFactory;

/**
 * Ergonomic producer facade-service for one-off polyglot publishes.
 *
 * It is sugar over the primary, typed API ({@see \BabelQueue\Contracts\ShouldQueuePolyglot}):
 * instead of declaring a job class for a fire-and-forget message, you can call
 *
 *     BabelQueue::publish('urn:babel:orders:created', ['order_id' => 1042]);
 *
 * Under the hood this wraps the (urn, data) pair in a {@see PolyglotEnvelopeJob}
 * and pushes it onto a `babelqueue-*` connection, so it runs the *exact same*
 * {@see \BabelQueue\Codec\EnvelopeCodec} encoder — same canonical envelope,
 * same trace_id handling, same broker bindings. There is no second wire format.
 *
 * The target connection MUST use a BabelQueue driver (`babelqueue-redis` or
 * `babelqueue-rabbitmq`); otherwise the job is serialised the standard Laravel
 * way and will not be a polyglot envelope.
 */
final class Publisher
{
    /**
     * @param  string|null  $connection  Default BabelQueue connection name, or null for the app default.
     */
    public function __construct(
        private QueueFactory $queue,
        private ?string $connection = null,
    ) {
    }

    /**
     * Publish a message and return its id (envelope meta.id).
     *
     * @param  string  $urn  The message URN (e.g. "urn:babel:orders:created").
     * @param  array<string, mixed>  $data  The pure JSON payload.
     * @param  string|null  $queue  Logical queue name, or null for the connection default.
     * @param  string|null  $traceId  Inherited trace id to continue, or null to mint a new one.
     */
    public function publish(string $urn, array $data, ?string $queue = null, ?string $traceId = null): string
    {
        return (string) $this->queue->connection($this->connection)->push(
            new PolyglotEnvelopeJob($urn, $data, $traceId),
            '',
            $queue,
        );
    }

    /**
     * Publish a message after a delay, returning its id.
     *
     * @param  DateTimeInterface|DateInterval|int  $delay
     * @param  array<string, mixed>  $data
     */
    public function later($delay, string $urn, array $data, ?string $queue = null, ?string $traceId = null): string
    {
        return (string) $this->queue->connection($this->connection)->later(
            $delay,
            new PolyglotEnvelopeJob($urn, $data, $traceId),
            '',
            $queue,
        );
    }

    /**
     * Return a Publisher bound to a specific connection (fluent override).
     */
    public function onConnection(?string $connection): self
    {
        return new self($this->queue, $connection);
    }
}
