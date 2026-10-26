<?php

declare(strict_types=1);

namespace BabelQueue\Consumer;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\PolyglotMessage;
use BabelQueue\DeadLetter\DeadLetter;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Queue\SqsQueue;
use Throwable;

/**
 * Routes a failed/undeliverable message to a cross-language dead-letter queue.
 *
 * The DLQ is just another broker queue holding canonical envelopes, so any SDK
 * (PHP, Go, Python, ...) can consume and inspect it. The original envelope is
 * preserved verbatim — same trace_id, same meta.id — and an additive, OPTIONAL
 * top-level "dead_letter" block is attached describing why it failed. Because the
 * field is additive and optional, the envelope stays at schema_version 1
 * (normal consumers ignore it; see https://babelqueue.com).
 *
 * This is opt-in: when disabled (the default) every call is a no-op, so a worker
 * with no DLQ configured behaves exactly as before.
 */
final class DeadLetterPublisher
{
    /**
     * @param  array<string, mixed>  $config  The "dead_letter" config block:
     *   enabled (bool), connection (?string), queue (?string), suffix (?string). A null / absent suffix
     *   is the driver default: "-dlq" on an Amazon SQS connection (`babelqueue-sqs` or the stock `sqs`, ADR-0037 decision 7), ".dlq" elsewhere.
     */
    public function __construct(
        private QueueFactory $queue,
        private array $config = [],
    ) {
    }

    public function enabled(): bool
    {
        return (bool) ($this->config['enabled'] ?? false);
    }

    /**
     * Publish a copy of the message onto the dead-letter queue, annotated with a
     * "dead_letter" block. No-op when DLQ is disabled.
     *
     * @param  string  $reason  Machine-readable cause: failed | unknown_urn | poison.
     *
     * @throws DeadLetterWriteFailed When the write to the dead-letter queue fails.
     */
    public function publish(PolyglotMessage $message, string $reason, ?Throwable $e = null): void
    {
        if (! $this->enabled()) {
            return;
        }

        $rawBody = $message->getRawBody();
        $queue = (string) $message->getQueue();

        // A poison copy (reason `poison`, which the reserve script flags even for a body PHP can decode, e.g. 1e400)
        // is written byte-for-byte by PoisonCopy (it cannot fail on the body: invalid UTF-8,
        // 1e400, ...), like php-sdk's Consume\DeadLetterPublisher; encode() would reshape a rejected list
        // "data" into an object or refuse a forbidden key.
        $body = $reason === 'poison' || $this->isPoison($rawBody)
            ? PoisonCopy::annotate($rawBody, $reason, $e, $queue, $message->attempts(), EnvelopeCodec::SOURCE_LANG)
            : EnvelopeCodec::encode(DeadLetter::annotate(
                EnvelopeCodec::decode($rawBody),
                $reason,
                $e,
                $queue,
                $message->attempts(),
                EnvelopeCodec::SOURCE_LANG,
            ));

        $target = null;

        try {
            $connection = $this->config['connection'] ?? $message->getConnectionName();

            $destination = $this->queue->connection($connection);
            $target = $this->target($message, $destination instanceof SqsQueue);

            $destination->pushRaw($body, $target);
        } catch (Throwable $writeError) {
            // A failed write is its own failure type: the Redis consume job leaves the reservation in
            // `:reserved` for it instead of releasing the message (ADR-0036 decision 7).
            throw DeadLetterWriteFailed::forQueue($target ?? $this->target($message, false), $reason, $writeError);
        }
    }

    /**
     * Resolve the dead-letter queue name: an explicit configured queue, else the original queue with the
     * configured suffix. An unset suffix is the default of the destination: ".dlq", except on Amazon SQS, whose
     * queue names cannot hold a dot, where it is "-dlq" (ADR-0037 decision 7). On SQS a FIFO queue keeps `.fifo`
     * last (`orders.fifo` -> `orders-dlq.fifo`); the original queue may be a name or a queue URL, the suffix is
     * appended to its last segment either way.
     */
    private function target(PolyglotMessage $message, bool $sqs): string
    {
        if (! empty($this->config['queue'])) {
            return (string) $this->config['queue'];
        }

        $queue = (string) $message->getQueue();
        $suffix = $this->config['suffix'] ?? null;
        $suffix = $suffix === null ? ($sqs ? '-dlq' : '.dlq') : (string) $suffix;

        if ($sqs && str_ends_with($queue, '.fifo')) {
            return substr($queue, 0, -strlen('.fifo')) . $suffix . '.fifo';
        }

        return $queue . $suffix;
    }

    /**
     * Whether the body is poison: the codec decodes it to the empty envelope (e.g. malformed JSON or a
     * list `data`). Such a body is dead-lettered as-is ({@see PoisonCopy}); a readable envelope is re-encoded.
     */
    private function isPoison(string $rawBody): bool
    {
        return EnvelopeCodec::decode($rawBody, static function (): void {}) === [];
    }
}
