<?php

declare(strict_types=1);

namespace BabelQueue\Queue;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\ShouldQueuePolyglot;
use BabelQueue\Queue\Jobs\BabelQueueArtemisJob;
use Closure;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Stomp\StatefulStomp;
use Stomp\Transport\Frame;
use Stomp\Transport\Message;

/**
 * A polyglot-first Apache ActiveMQ **Artemis** queue built directly on the pure-PHP `stomp-php`
 * client — the PHP path to the §7 binding (ADR-0018, §7.8).
 *
 * Produce path: polyglot jobs are sent as the strict { job, trace_id, data, meta } JSON envelope
 * (the STOMP frame body), with the §7 STOMP headers (`content-type`, `correlation-id` = trace_id,
 * and the string `bq_schema_version`/`bq_source_lang`/`bq_attempts`/`bq_app_id`). Artemis bridges
 * STOMP ↔ AMQP 1.0 ↔ JMS on the same address, so Java/.NET/Node/Python/Go consume it natively.
 *
 * Consume path: pop() subscribes once (ack mode `client-individual`) and reads the next frame,
 * wrapping it as a {@see BabelQueueArtemisJob}; delete() ACKs, release() republishes a fresh copy
 * (incremented attempt counter) then ACKs the original (at-least-once). Routing is
 * **body-authoritative**: the dispatcher routes on the envelope `job` URN (§7.8) — a STOMP frame
 * cannot set the `x-opt-jms-type` annotation, but the URN is always in the body.
 *
 * STOMP has no queue-depth primitive, so size() returns 0 (use the broker's management API for
 * depth); the worker drives pop()/delete()/release() as usual.
 */
class BabelQueueArtemisQueue extends Queue implements QueueContract
{
    /** @var Closure(): StatefulStomp */
    protected Closure $connectionFactory;

    protected ?StatefulStomp $stomp = null;

    /** Default (logical) queue name. */
    protected string $default;

    /** Prepended to the queue name to form the STOMP destination (e.g. "/queue/" for Artemis anycast). */
    protected string $destinationPrefix;

    /**
     * Destinations already subscribed on the current connection.
     *
     * @var array<string, true>
     */
    protected array $subscribed = [];

    /**
     * @param  Closure(): StatefulStomp  $connectionFactory
     * @param  array<string, mixed>  $config
     */
    public function __construct(Closure $connectionFactory, array $config)
    {
        $this->connectionFactory = $connectionFactory;
        $this->default = (string) ($config['queue'] ?? 'default');
        $this->destinationPrefix = (string) ($config['destination_prefix'] ?? '');
    }

    /**
     * STOMP exposes no queue-depth primitive; depth is a broker-management concern.
     *
     * @param  string|null  $queue
     */
    public function size($queue = null): int
    {
        return 0;
    }

    /**
     * Push a new job onto the queue.
     *
     * @param  \BabelQueue\Contracts\ShouldQueuePolyglot|object|string  $job
     * @param  mixed  $data
     * @param  string|null  $queue
     * @return string|null The published message id (meta.id) for polyglot jobs.
     */
    public function push($job, $data = '', $queue = null)
    {
        if ($job instanceof ShouldQueuePolyglot) {
            return $this->enqueuePolyglot($job, $queue, 0);
        }

        return $this->pushRaw($this->createPayload($job, $this->getQueue($queue), $data), $queue);
    }

    /**
     * Push a raw, already-encoded payload onto the queue.
     *
     * @param  string  $payload
     * @param  string|null  $queue
     * @param  array<string, mixed>  $options
     * @return string|null
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        $this->send(
            $payload,
            $this->getQueue($queue),
            ['content-type' => 'application/json', 'bq_attempts' => (string) ((int) ($options['attempts'] ?? 0))],
            0,
        );

        return $this->extractId($payload);
    }

    /**
     * Push a new job onto the queue after a delay (Artemis native scheduled delivery).
     *
     * @param  \DateTimeInterface|\DateInterval|int  $delay
     * @param  \BabelQueue\Contracts\ShouldQueuePolyglot|object|string  $job
     * @param  mixed  $data
     * @param  string|null  $queue
     * @return string|null
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        $delayMs = $this->secondsUntil($delay) * 1000;

        if ($job instanceof ShouldQueuePolyglot) {
            return $this->enqueuePolyglot($job, $queue, $delayMs);
        }

        $payload = $this->createPayload($job, $this->getQueue($queue), $data);
        $this->send($payload, $this->getQueue($queue), ['content-type' => 'application/json'], $delayMs);

        return $this->extractId($payload);
    }

    /**
     * Pop the next job off of the queue.
     *
     * @param  string|null  $queue
     */
    public function pop($queue = null): ?JobContract
    {
        $queue = $this->getQueue($queue);
        $this->subscribe($queue);

        $frame = $this->stomp()->read();

        if (! $frame instanceof Frame) {
            return null;
        }

        return new BabelQueueArtemisJob(
            $this->container,
            $this,
            $frame,
            $this->connectionName,
            $queue,
        );
    }

    /**
     * ACK a delivery (the job's delete()).
     */
    public function ackFrame(Frame $frame): void
    {
        $this->stomp()->ack($frame);
    }

    /**
     * Republish a released message with an incremented attempt counter (and an optional Artemis
     * scheduled delay), preserving the trace id. Used by the job's release() so retry semantics
     * live in one place; the caller ACKs the original delivery afterwards (at-least-once).
     */
    public function republish(Frame $frame, string $queue, int $attempts, int $delaySeconds): void
    {
        $headers = [
            'content-type' => 'application/json',
            'bq_attempts' => (string) $attempts,
        ];

        $traceId = $frame['correlation-id'] ?? null;
        if (is_string($traceId) && $traceId !== '') {
            $headers['correlation-id'] = $traceId;
        }

        $this->send((string) $frame->body, $queue, $headers, $delaySeconds * 1000);
    }

    /**
     * Resolve the logical queue name, falling back to the connection default.
     *
     * @param  string|null  $queue
     */
    public function getQueue($queue): string
    {
        return $queue ?: $this->default;
    }

    /**
     * Build and send the strict polyglot envelope for a job.
     *
     * @param  string|null  $queue
     * @return string The sent message id (meta.id).
     */
    protected function enqueuePolyglot(ShouldQueuePolyglot $job, $queue, int $delayMs): string
    {
        $resolved = $this->getQueue($queue);
        $payload = EnvelopeCodec::fromJob($job, $resolved);

        $headers = [
            'content-type' => 'application/json',
            'bq_app_id' => 'babelqueue',
            'bq_schema_version' => (string) EnvelopeCodec::SCHEMA_VERSION,
            'bq_source_lang' => (string) EnvelopeCodec::SOURCE_LANG,
            'bq_attempts' => (string) $payload['attempts'],
        ];

        if ($payload['trace_id'] !== '') {
            // STOMP "correlation-id" ↔ AMQP correlation-id (native), so a consumer correlates
            // without decoding the body; it survives the STOMP↔AMQP↔JMS bridge.
            $headers['correlation-id'] = (string) $payload['trace_id'];
        }

        $this->send(EnvelopeCodec::encode($payload), $resolved, $headers, $delayMs);

        return $payload['meta']['id'];
    }

    /**
     * Low-level send: subscribe-agnostic SEND to the queue's destination, with the Artemis native
     * scheduled-delay header when a delay is requested.
     *
     * @param  array<string, string>  $headers
     */
    protected function send(string $body, string $queue, array $headers, int $delayMs): void
    {
        if ($delayMs > 0) {
            // Artemis honours scheduled delivery natively via this header (ms).
            $headers['AMQ_SCHEDULED_DELAY'] = (string) $delayMs;
        }

        $this->stomp()->send($this->destination($queue), new Message($body, $headers));
    }

    /**
     * Idempotently subscribe to a queue's destination with manual (client-individual) ack.
     */
    protected function subscribe(string $queue): void
    {
        if (isset($this->subscribed[$queue])) {
            return;
        }

        $this->stomp()->subscribe($this->destination($queue), null, 'client-individual');
        $this->subscribed[$queue] = true;
    }

    /**
     * The STOMP destination for a logical queue (prefix + name).
     */
    protected function destination(string $queue): string
    {
        return $this->destinationPrefix . $queue;
    }

    /**
     * Best-effort extraction of a message id from an encoded payload.
     */
    protected function extractId(string $payload): ?string
    {
        $decoded = json_decode($payload, true);

        if (! is_array($decoded)) {
            return null;
        }

        return $decoded['meta']['id'] ?? $decoded['id'] ?? null;
    }

    /** Lazily resolve a live STOMP client, rebuilding it (and its subscriptions) if it dropped. */
    protected function stomp(): StatefulStomp
    {
        if (! $this->stomp instanceof StatefulStomp) {
            $this->stomp = ($this->connectionFactory)();
            $this->subscribed = [];
        }

        return $this->stomp;
    }
}
