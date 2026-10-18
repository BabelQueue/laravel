<?php

declare(strict_types=1);

namespace BabelQueue\Support;

use BabelQueue\Codec\EnvelopeCodec;

/**
 * The transport-header schema-version gate shared by the SQS (§3.7, `bq-schema-version`) and
 * Artemis (§7.7, `bq_schema_version`) drivers.
 *
 * The gate runs BEFORE the body is decoded. A blank value (the empty string or only ASCII
 * whitespace: space, `\t`, `\n`, `\v`, `\f`, `\r`) or an absent / non-string one falls through to
 * the normal decode path; any other value is compared as-is, never trimmed, and only exactly the
 * supported version decodes. The blank set is the same in every SDK (GR-5); it is not `trim()`,
 * whose default set contains `\0` and lacks `\f`, and not a Unicode-aware check.
 */
final class SchemaVersionGate
{
    /**
     * Reserved routing URN prefix for a message rejected by the gate; the rejected header value is
     * appended. No handler is ever mapped to it.
     */
    public const UNSUPPORTED_URN_PREFIX = 'babelqueue:unsupported-schema-version:';

    /**
     * The stand-in envelope a gate-rejected message presents instead of its (never decoded) body: the
     * reserved sentinel URN and the rejected, non-integer `meta.schema_version`, so the dispatcher
     * takes its unsupported-schema-version path (DLQ quarantine or fail) and no handler runs.
     *
     * @return array{job: string, meta: array{schema_version: string}}
     */
    public static function stubEnvelope(string $rejectedVersion): array
    {
        return [
            'job' => self::UNSUPPORTED_URN_PREFIX . $rejectedVersion,
            'meta' => ['schema_version' => $rejectedVersion],
        ];
    }

    /**
     * The header value when the message must be rejected without decoding; null when the body may be
     * decoded.
     */
    public static function rejected(mixed $value): ?string
    {
        if (! is_string($value) || self::isBlankASCII($value)) {
            return null;
        }

        return $value === (string) EnvelopeCodec::SCHEMA_VERSION ? null : $value;
    }

    private static function isBlankASCII(string $value): bool
    {
        return preg_match('/\A[ \t\n\x0B\f\r]*\z/', $value) === 1;
    }
}
