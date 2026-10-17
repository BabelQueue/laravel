<?php

declare(strict_types=1);

namespace BabelQueue\Support;

use BabelQueue\Codec\EnvelopeCodec;
use JsonException;

/**
 * Normalises a raw envelope body through the core codec (decode -> encode) before it is
 * written back to a broker (release/republish), so the re-emit keeps unknown top-level and
 * `meta` keys, preserves empty objects as `{}` and resolves a `urn`-only body to `job`
 * (ADR-0031). A body the codec cannot read (poison) is returned unchanged, so a retry never
 * loses a message it could not parse.
 */
final class EnvelopeBody
{
    public static function normalize(string $rawBody): string
    {
        $envelope = EnvelopeCodec::decode($rawBody);

        if ($envelope === []) {
            return $rawBody;
        }

        try {
            return EnvelopeCodec::encode($envelope);
        } catch (JsonException) {
            return $rawBody;
        }
    }
}
