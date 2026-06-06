<?php

declare(strict_types=1);

namespace BabelQueue\Support;

use BabelQueue\Contracts\HasTraceId;
use BabelQueue\Contracts\ShouldQueuePolyglot;

/**
 * A lightweight, ad-hoc polyglot job built from a raw (urn, data) pair.
 *
 * It exists so the {@see \BabelQueue\Producer\Publisher} facade can publish a
 * message without forcing the caller to declare a dedicated job class. Because it
 * implements {@see ShouldQueuePolyglot} (and optionally carries an inherited
 * {@see HasTraceId trace id}), pushing it onto a `babelqueue-*` connection runs
 * the exact same encode pipeline as a normal job — {@see \BabelQueue\Codec\EnvelopeCodec}
 * builds the identical canonical envelope, so there is no second code path or format.
 *
 * It is a pure data carrier: no Dispatchable/InteractsWithQueue traits, no
 * behaviour. The queue driver detects the contract and serialises it.
 */
final class PolyglotEnvelopeJob implements ShouldQueuePolyglot, HasTraceId
{
    /**
     * @param  string  $urn  The message URN (envelope "job").
     * @param  array<string, mixed>  $data  The pure JSON payload (envelope "data").
     * @param  string|null  $traceId  An inherited trace id to continue, or null to mint a new one.
     */
    public function __construct(
        private string $urn,
        private array $data,
        private ?string $traceId = null,
    ) {
    }

    public function getBabelUrn(): string
    {
        return $this->urn;
    }

    /**
     * @return array<string, mixed>
     */
    public function toPayload(): array
    {
        return $this->data;
    }

    public function getBabelTraceId(): ?string
    {
        return $this->traceId;
    }
}
