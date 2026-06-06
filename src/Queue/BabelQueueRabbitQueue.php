<?php

declare(strict_types=1);

namespace BabelQueue\Queue;

use BabelQueue\Contracts\ShouldQueuePolyglot;
use BabelQueue\Exceptions\BabelQueueException;
use BabelQueue\Queue\Jobs\BabelQueueRabbitJob;
use BabelQueue\Codec\EnvelopeCodec;
use Closure;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

/**
 * A minimal, polyglot-first RabbitMQ queue built directly on php-amqplib.
 *
 * Produce path: polyglot jobs are published as the strict { job, data, meta }
 * JSON envelope, with AMQP properties (content_type, type, message_id,
 * delivery_mode) that are part of the cross-language contract. Standard Laravel
 * jobs still work as a drop-in fallback.
 *
 * Consume path: pop() uses basic_get + manual ack so a PHP worker can run
 * standard jobs and round-trip tests stay simple. Polyglot envelopes are meant
 * to be consumed by Go/Java services, not by PHP.
 */
class BabelQueueRabbitQueue extends Queue implements QueueContract
{
    /** @var Closure(): AbstractConnection */
    protected Closure $connectionFactory;

    protected ?AbstractConnection $connection = null;

    protected ?AMQPChannel $channel = null;

    /** Default (logical) queue name. */
    protected string $default;

    /** Exchange to publish to; '' means the AMQP default exchange. */
    protected string $exchange;

    /** Exchange type: direct | topic | fanout | x-delayed-message. */
    protected string $exchangeType;

    /** @var array<string, mixed> */
    protected array $connectionConfig;

    /**
     * Queues whose topology has already been declared on the current channel.
     *
     * @var array<string, true>
     */
    protected array $declared = [];

    /**
     * @param  Closure(): AbstractConnection  $connectionFactory
     * @param  array<string, mixed>  $config
     */
    public function __construct(Closure $connectionFactory, array $config)
    {
        $this->connectionFactory = $connectionFactory;
        $this->connectionConfig = $config;
        $this->default = (string) ($config['queue'] ?? 'default');
        $this->exchange = (string) ($config['exchange'] ?? '');
        $this->exchangeType = (string) ($config['exchange_type'] ?? 'direct');
    }

    /**
     * Get the number of ready messages in the given queue.
     *
     * @param  string|null  $queue
     */
    public function size($queue = null): int
    {
        $queue = $this->getQueue($queue);
        $this->declareTopology($queue);

        // Passive declare returns [name, messageCount, consumerCount].
        [, $messageCount] = $this->channel()->queue_declare($queue, true);

        return (int) $messageCount;
    }

    /**
     * Push a new job onto the queue.
     *
     * @param  \BabelQueue\Contracts\ShouldQueuePolyglot|object|string  $job
     * @param  mixed  $data
     * @param  string|null  $queue
     * @return string|null The published message id.
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
        return $this->enqueueRaw($payload, $this->getQueue($queue), (int) ($options['attempts'] ?? 0), 0);
    }

    /**
     * Push a new job onto the queue after a delay.
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

        return $this->enqueueRaw(
            $this->createPayload($job, $this->getQueue($queue), $data),
            $this->getQueue($queue),
            0,
            $delayMs,
        );
    }

    /**
     * Pop the next job off of the queue.
     *
     * @param  string|null  $queue
     */
    public function pop($queue = null): ?JobContract
    {
        $queue = $this->getQueue($queue);
        $this->declareTopology($queue);

        $message = $this->channel()->basic_get($queue);

        if ($message === null) {
            return null;
        }

        return new BabelQueueRabbitJob(
            $this->container,
            $this,
            $message,
            $this->connectionName,
            $queue,
        );
    }

    /**
     * Republish a message that is being released back onto the queue, with an
     * incremented attempt counter and an optional delay. Used by the job's
     * release() so retry semantics live in one place.
     */
    public function republish(AMQPMessage $message, string $queue, int $attempts, int $delaySeconds): void
    {
        $properties = $message->get_properties();

        $headers = isset($properties['application_headers'])
            ? $properties['application_headers']->getNativeData()
            : [];

        $headers['x-attempts'] = $attempts;

        // Best-effort delay: honoured only when a delayed-message exchange is in
        // use; otherwise the job is requeued immediately so it is never lost.
        if ($delaySeconds > 0 && $this->supportsDelay()) {
            $headers['x-delay'] = $delaySeconds * 1000;
        }

        $properties['application_headers'] = new AMQPTable($headers);

        $this->declareTopology($queue);
        $this->channel()->basic_publish(
            new AMQPMessage($message->getBody(), $properties),
            $this->exchange,
            $this->routingKey($queue),
        );
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
     * Build and publish the strict polyglot envelope for a job.
     *
     * @param  string|null  $queue
     * @return string The published message id (meta.id).
     */
    protected function enqueuePolyglot(ShouldQueuePolyglot $job, $queue, int $delayMs): string
    {
        $resolved = $this->getQueue($queue);
        $payload = EnvelopeCodec::fromJob($job, $resolved);

        $this->publish(
            EnvelopeCodec::encode($payload),
            $resolved,
            [
                'content_type' => 'application/json',
                'content_encoding' => 'utf-8',
                'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
                'message_id' => $payload['meta']['id'],
                // AMQP "correlation_id" carries the cross-service trace_id, so a
                // consumer can correlate without decoding the body. republish()
                // copies message properties verbatim, so it survives retries.
                'correlation_id' => $payload['trace_id'],
                'timestamp' => intdiv($payload['meta']['created_at'], 1000),
                // AMQP "type" carries the message URN (never a PHP class name),
                // so consumers can route purely on properties.type.
                'type' => $payload['job'],
                'app_id' => 'babelqueue',
            ],
            [
                'x-schema-version' => EnvelopeCodec::SCHEMA_VERSION,
                'x-source-lang' => EnvelopeCodec::SOURCE_LANG,
                'x-attempts' => $payload['attempts'] ?? 0,
            ],
            $delayMs,
        );

        return $payload['meta']['id'];
    }

    /**
     * Publish an already-encoded (standard Laravel) payload.
     *
     * @return string|null The payload's own id, if any.
     */
    protected function enqueueRaw(string $payload, string $queue, int $attempts, int $delayMs): ?string
    {
        $this->publish(
            $payload,
            $queue,
            [
                'content_type' => 'application/json',
                'content_encoding' => 'utf-8',
                'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            ],
            ['x-attempts' => $attempts],
            $delayMs,
        );

        return $this->extractId($payload);
    }

    /**
     * Low-level publish: declare topology, attach headers, basic_publish.
     *
     * @param  array<string, mixed>  $properties
     * @param  array<string, mixed>  $headers
     */
    protected function publish(string $body, string $queue, array $properties, array $headers, int $delayMs = 0): void
    {
        if ($delayMs > 0) {
            $this->assertDelaySupported();
            $headers['x-delay'] = $delayMs;
        }

        $this->declareTopology($queue);

        $properties['application_headers'] = new AMQPTable($headers);

        $this->channel()->basic_publish(
            new AMQPMessage($body, $properties),
            $this->exchange,
            $this->routingKey($queue),
        );
    }

    /**
     * Idempotently declare the queue (and, for a named exchange, the exchange
     * and its binding). Results are cached per channel for throughput.
     */
    protected function declareTopology(string $queue): void
    {
        if (isset($this->declared[$queue])) {
            return;
        }

        $channel = $this->channel();

        // durable queue, not exclusive, not auto-delete.
        $channel->queue_declare($queue, false, true, false, false);

        if ($this->exchange !== '') {
            $arguments = [];

            if ($this->supportsDelay()) {
                $arguments['x-delayed-type'] = (string) ($this->connectionConfig['delayed_type'] ?? 'direct');
            }

            $channel->exchange_declare(
                $this->exchange,
                $this->exchangeType,
                false,
                true,
                false,
                false,
                false,
                new AMQPTable($arguments),
            );

            $channel->queue_bind($queue, $this->exchange, $this->routingKey($queue));
        }

        $this->declared[$queue] = true;
    }

    /**
     * Routing key for a queue. With the default exchange this is the queue name
     * itself, which is exactly how AMQP routes to a same-named queue.
     */
    protected function routingKey(string $queue): string
    {
        return $queue;
    }

    protected function supportsDelay(): bool
    {
        return $this->exchangeType === 'x-delayed-message';
    }

    protected function assertDelaySupported(): void
    {
        if (! $this->supportsDelay()) {
            throw new BabelQueueException(
                'Delayed dispatch requires a RabbitMQ "x-delayed-message" exchange. '
                . 'Configure "exchange" with "exchange_type" => "x-delayed-message" and enable the '
                . 'rabbitmq_delayed_message_exchange plugin on the broker.',
            );
        }
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

    /** Lazily resolve (and memoise) an open channel, reconnecting if needed. */
    protected function channel(): AMQPChannel
    {
        if ($this->channel instanceof AMQPChannel && $this->channel->is_open()) {
            return $this->channel;
        }

        return $this->channel = $this->connection()->channel();
    }

    /** Lazily resolve a live connection, rebuilding it if it has dropped. */
    protected function connection(): AbstractConnection
    {
        if (! $this->connection instanceof AbstractConnection || ! $this->connection->isConnected()) {
            $this->connection = ($this->connectionFactory)();

            // A fresh connection/channel has declared nothing yet.
            $this->declared = [];
        }

        return $this->connection;
    }

    /** Close the channel and connection cleanly when the queue is discarded. */
    public function __destruct()
    {
        try {
            if ($this->channel instanceof AMQPChannel && $this->channel->is_open()) {
                $this->channel->close();
            }

            if ($this->connection instanceof AbstractConnection && $this->connection->isConnected()) {
                $this->connection->close();
            }
        } catch (\Throwable) {
            // Never let teardown noise surface during shutdown.
        }
    }
}
