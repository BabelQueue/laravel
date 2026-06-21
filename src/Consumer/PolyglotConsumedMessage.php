<?php

declare(strict_types=1);

namespace BabelQueue\Consumer;

use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Contracts\PolyglotMessage;

/**
 * Adapts a Laravel {@see PolyglotMessage} to the framework-agnostic core
 * {@see ConsumedMessage} view, so the php-sdk idempotency helpers
 * ({@see \BabelQueue\Idempotency\Idempotent::wrap()} and
 * {@see \BabelQueue\Idempotency\ClaimingDispatch::wrap()}) — which type-hint
 * {@see ConsumedMessage} — can dedupe a Laravel delivery on its canonical
 * "meta.id" without the frozen {@see PolyglotMessage} contract having to extend
 * {@see ConsumedMessage} (GR-6: standard Laravel jobs stay untouched).
 *
 * It is a thin read-only decorator: every method delegates to the underlying
 * job. The helpers only read the decoded envelope view (URN, trace id, data,
 * meta) to key dedupe on "meta.id"; the original {@see PolyglotMessage} — not
 * this adapter — is what the actual handler runs against, so the job's
 * ack/release lifecycle is never funnelled through here.
 */
final class PolyglotConsumedMessage implements ConsumedMessage
{
    public function __construct(private readonly PolyglotMessage $message)
    {
    }

    public function getUrn(): string
    {
        return $this->message->getUrn();
    }

    public function getTraceId(): string
    {
        return $this->message->getTraceId();
    }

    /**
     * @return array<string, mixed>
     */
    public function getData(): array
    {
        return $this->message->getData();
    }

    /**
     * @return array<string, mixed>
     */
    public function getMeta(): array
    {
        return $this->message->getMeta();
    }

    public function attempts(): int
    {
        return (int) $this->message->attempts();
    }

    /**
     * @return array<string, mixed>
     */
    public function envelope(): array
    {
        return [
            'job' => $this->message->getUrn(),
            'trace_id' => $this->message->getTraceId(),
            'data' => $this->message->getData(),
            'meta' => $this->message->getMeta(),
        ];
    }
}
