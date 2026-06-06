<?php

declare(strict_types=1);

namespace BabelQueue\Queue\Jobs;

use BabelQueue\Contracts\PolyglotMessage;
use BabelQueue\Queue\BabelQueueRabbitQueue;
use BabelQueue\Queue\Concerns\ParsesPolyglotEnvelope;
use Illuminate\Container\Container;
use Illuminate\Queue\Jobs\Job;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * A popped RabbitMQ message wrapped as a polyglot consume job.
 *
 * Acknowledgement is manual: delete() acks the delivery and release()
 * republishes a fresh copy (with an incremented attempt counter) before acking
 * the original, giving at-least-once semantics. The job acks on its own
 * delivery channel, as AMQP requires.
 *
 * The shared trait supplies URN/data/meta parsing and routes fire()/failed()
 * through the dispatcher, so RabbitMQ consumption behaves identically to Redis.
 *
 * Note: signatures intentionally mirror the untyped parent/contract methods so
 * the class stays compatible across Laravel versions; types are documented.
 */
class BabelQueueRabbitJob extends Job implements PolyglotMessage
{
    use ParsesPolyglotEnvelope;

    public function __construct(
        Container $container,
        protected BabelQueueRabbitQueue $rabbit,
        protected AMQPMessage $message,
        string $connectionName,
        string $queue,
    ) {
        $this->container = $container;
        $this->connectionName = $connectionName;
        $this->queue = $queue;
    }

    /**
     * Get the raw, encoded payload (the AMQP message body).
     */
    public function getRawBody(): string
    {
        return $this->message->getBody();
    }

    /**
     * Get the unique identifier for the job: the AMQP message id, else the
     * envelope's meta.id, else the delivery tag.
     */
    public function getJobId(): string
    {
        if ($this->message->has('message_id')) {
            return (string) $this->message->get('message_id');
        }

        $metaId = $this->getMeta()['id'] ?? null;

        if ($metaId !== null && $metaId !== '') {
            return (string) $metaId;
        }

        return (string) $this->message->getDeliveryTag();
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

        $this->message->getChannel()->basic_ack($this->message->getDeliveryTag());
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
        $this->rabbit->republish($this->message, $this->queue, $this->attempts(), (int) $delay);

        $this->message->getChannel()->basic_ack($this->message->getDeliveryTag());
    }

    /**
     * Attempts already recorded before this delivery. The AMQP "x-attempts"
     * header is authoritative (we maintain it on republish); otherwise we fall
     * back to the envelope's top-level "attempts" for externally produced
     * messages that only set it in the body.
     */
    protected function priorAttempts(): int
    {
        if ($this->message->has('application_headers')) {
            $headers = $this->message->get('application_headers')->getNativeData();

            if (isset($headers['x-attempts'])) {
                return (int) $headers['x-attempts'];
            }
        }

        return (int) ($this->envelope()['attempts'] ?? 0);
    }
}
