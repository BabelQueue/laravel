<?php

declare(strict_types=1);

namespace BabelQueue\Consumer;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\PolyglotMessage;
use BabelQueue\DeadLetter\DeadLetter;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use JsonException;
use Throwable;

/**
 * Routes a failed/undeliverable message to a cross-language dead-letter queue.
 *
 * The DLQ is just another broker queue holding canonical envelopes, so any SDK
 * (PHP, Go, Python, ...) can consume and inspect it. The original envelope is
 * preserved verbatim — same trace_id, same meta.id — and an additive, OPTIONAL
 * top-level "dead_letter" block is attached describing why it failed. Because the
 * field is additive and optional, the envelope stays at schema_version 1
 * (see ../../.ssot/contracts/error-handling.md and ADR-0009).
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

        $envelope = DeadLetter::annotate(
            $this->decode($message->getRawBody()),
            $reason,
            $e,
            (string) $message->getQueue(),
            $message->attempts(),
            EnvelopeCodec::SOURCE_LANG,
        );

        $connection = $this->config['connection'] ?? $message->getConnectionName();

        $this->queue->connection($connection)->pushRaw(
            EnvelopeCodec::encode($envelope),
            $this->target($message),
        );
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
     * Decode the raw body; a poison (non-JSON) body still gets dead-lettered as
     * an empty envelope so it can be inspected rather than lost.
     *
     * @return array<string, mixed>
     */
    private function decode(string $rawBody): array
    {
        try {
            $decoded = json_decode($rawBody, true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException) {
            return ['raw' => $rawBody];
        }

        return is_array($decoded) ? $decoded : ['raw' => $rawBody];
    }
}
