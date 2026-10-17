<?php

declare(strict_types=1);

namespace BabelQueue\Consumer;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\PolyglotMessage;
use BabelQueue\DeadLetter\DeadLetter;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
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
     *   enabled (bool), connection (?string), queue (?string), suffix (string).
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
     */
    public function publish(PolyglotMessage $message, string $reason, ?Throwable $e = null): void
    {
        if (! $this->enabled()) {
            return;
        }

        $rawBody = $message->getRawBody();
        $poison = $this->poisonCopy($rawBody);

        $envelope = DeadLetter::annotate(
            $poison ?? EnvelopeCodec::decode($rawBody),
            $reason,
            $e,
            (string) $message->getQueue(),
            $message->attempts(),
            EnvelopeCodec::SOURCE_LANG,
        );

        $connection = $this->config['connection'] ?? $message->getConnectionName();

        // A poison copy is written as-is (like php-sdk's Consume\DeadLetterPublisher): encode() would
        // reshape a rejected list "data" into an object or refuse a forbidden key.
        $body = $poison === null
            ? EnvelopeCodec::encode($envelope)
            : json_encode($envelope, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);

        $this->queue->connection($connection)->pushRaw($body, $this->target($message));
    }

    /**
     * Resolve the dead-letter queue name: an explicit configured queue, else the
     * original queue with the configured suffix (default ".dlq").
     */
    private function target(PolyglotMessage $message): string
    {
        if (! empty($this->config['queue'])) {
            return (string) $this->config['queue'];
        }

        return $message->getQueue() . ((string) ($this->config['suffix'] ?? '.dlq'));
    }

    /**
     * The inspectable copy of a poison body (one the codec decodes to the empty envelope, e.g.
     * malformed JSON or a list `data`): the original JSON object verbatim, else `{"raw": "<body>"}`.
     * Null when the body is a readable envelope.
     *
     * @return array<string, mixed>|null
     */
    private function poisonCopy(string $rawBody): ?array
    {
        if (EnvelopeCodec::decode($rawBody, static function (): void {}) !== []) {
            return null;
        }

        $decoded = json_decode($rawBody, true);

        return is_array($decoded) ? $decoded : ['raw' => $rawBody];
    }
}
