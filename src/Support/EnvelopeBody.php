<?php

declare(strict_types=1);

namespace BabelQueue\Support;

use BabelQueue\Codec\EnvelopeCodec;

/**
 * Prepares a raw envelope body for being written back to a broker (release/republish)
 * without changing anything the broker did not already hold.
 *
 * A decode -> encode round trip through PHP arrays is lossy for the data channel (a nested
 * `{}` becomes `[]`, an integer above PHP_INT_MAX becomes a float), so the body is NOT
 * re-serialised. It is scanned member by member and every value is kept byte for byte. The only
 * edits are the ones ADR-0031 asks for on re-emit, i.e. what `decode` -> `encode` would change
 * in the members that are not `data`:
 *
 *  - a body that carries the inbound `urn` alias is rewritten so the URN lives in `job` and
 *    `urn` is gone (ADR-0031 (c));
 *  - the keys message-envelope.md section 10 forbids are removed: `timestamp` at the top level and
 *    `max_retries` / `attempts` / `source` / `ts` inside `meta`, so a retry never puts them back on
 *    the wire (ADR-0031 (b), GR-8). The key lists are read from the core codec, not copied.
 *
 * A body the scanner cannot make sense of (poison, non-object JSON, a non-string/blank `urn`, a
 * duplicated `job`/`urn` key for the alias edit) is left alone for that edit, so a retry never
 * loses a message. A body with nothing to edit is returned unchanged, byte for byte.
 *
 * Unknown top-level and `meta` keys, empty objects and big integers therefore survive as-is.
 */
final class EnvelopeBody
{
    /** The ASCII blank set shared with the core codec (`EnvelopeCodec::BLANK_CHARS`). */
    private const BLANK_CHARS = " \t\n\v\f\r";

    public static function normalize(string $rawBody): string
    {
        if (! is_array(json_decode($rawBody, true))) {
            return $rawBody;
        }

        $members = self::members($rawBody);
        if ($members === null) {
            return $rawBody;
        }

        $changed = false;

        // Forbidden top-level keys, and forbidden keys inside "meta".
        $kept = [];
        foreach ($members as $member) {
            if (in_array($member['key'], EnvelopeCodec::FORBIDDEN_TOP_LEVEL_KEYS, true)) {
                $changed = true;

                continue;
            }

            if ($member['key'] === 'meta') {
                $cleaned = self::withoutForbiddenMeta($member['value']);
                if ($cleaned !== $member['value']) {
                    $changed = true;
                    $member['value'] = $cleaned;
                    $member['raw'] = $member['rawKey'] . ':' . $cleaned;
                }
            }

            $kept[] = $member;
        }
        $members = $kept;

        // The inbound "urn" alias.
        $jobIndex = null;
        $urnIndex = null;
        $ambiguous = false;
        foreach ($members as $i => $member) {
            if ($member['key'] === 'job') {
                $ambiguous = $ambiguous || $jobIndex !== null;
                $jobIndex = $i;
            } elseif ($member['key'] === 'urn') {
                $ambiguous = $ambiguous || $urnIndex !== null;
                $urnIndex = $i;
            }
        }

        $parts = array_map(static fn (array $m): string => $m['raw'], $members);

        if ($urnIndex !== null && ! $ambiguous) {
            if ($jobIndex !== null) {
                $job = json_decode($members[$jobIndex]['value']);
                $jobBlank = $job === null || (is_string($job) && trim($job, self::BLANK_CHARS) === '');

                if ($jobBlank) {
                    $urn = json_decode($members[$urnIndex]['value']);
                    if (is_string($urn) && trim($urn, self::BLANK_CHARS) !== '') {
                        $parts[$jobIndex] = $members[$jobIndex]['rawKey'] . ':' . $members[$urnIndex]['value'];
                        unset($parts[$urnIndex]);
                        $changed = true;
                    }
                } else {
                    unset($parts[$urnIndex]);
                    $changed = true;
                }
            } else {
                $urn = json_decode($members[$urnIndex]['value']);
                if (is_string($urn) && trim($urn, self::BLANK_CHARS) !== '') {
                    $parts[$urnIndex] = '"job":' . $members[$urnIndex]['value'];
                    $changed = true;
                }
            }
        }

        return $changed ? '{' . implode(',', $parts) . '}' : $rawBody;
    }

    /**
     * The raw `meta` value without the keys section 10 forbids; the value itself when it is not an
     * object, cannot be scanned, or holds none of them.
     */
    private static function withoutForbiddenMeta(string $rawMeta): string
    {
        $members = self::members($rawMeta);
        if ($members === null) {
            return $rawMeta;
        }

        $parts = [];
        $dropped = false;
        foreach ($members as $member) {
            if (in_array($member['key'], EnvelopeCodec::FORBIDDEN_META_KEYS, true)) {
                $dropped = true;

                continue;
            }
            $parts[] = $member['raw'];
        }

        return $dropped ? '{' . implode(',', $parts) . '}' : $rawMeta;
    }

    /**
     * Split a JSON object body into its top-level members, keeping the raw text of each key and value.
     * The body is already known to be valid JSON, so the scan only has to track strings and nesting.
     *
     * @return list<array{key: string, rawKey: string, value: string, raw: string}>|null
     */
    private static function members(string $body): ?array
    {
        $length = strlen($body);
        $pos = self::skipWhitespace($body, 0);
        if ($pos >= $length || $body[$pos] !== '{') {
            return null;
        }
        $pos = self::skipWhitespace($body, $pos + 1);

        $members = [];
        while ($pos < $length && $body[$pos] !== '}') {
            if ($body[$pos] !== '"') {
                return null;
            }
            $keyEnd = self::stringEnd($body, $pos);
            $rawKey = substr($body, $pos, $keyEnd - $pos);
            $key = json_decode($rawKey);
            if (! is_string($key)) {
                return null;
            }

            $pos = self::skipWhitespace($body, $keyEnd);
            if ($pos >= $length || $body[$pos] !== ':') {
                return null;
            }
            $valueStart = self::skipWhitespace($body, $pos + 1);
            $valueEnd = self::valueEnd($body, $valueStart);
            if ($valueEnd === null) {
                return null;
            }

            $value = substr($body, $valueStart, $valueEnd - $valueStart);
            $members[] = ['key' => $key, 'rawKey' => $rawKey, 'value' => $value, 'raw' => $rawKey . ':' . $value];

            $pos = self::skipWhitespace($body, $valueEnd);
            if ($pos < $length && $body[$pos] === ',') {
                $pos = self::skipWhitespace($body, $pos + 1);
            }
        }

        return $pos < $length && $members !== [] ? $members : null;
    }

    private static function skipWhitespace(string $body, int $pos): int
    {
        $length = strlen($body);
        while ($pos < $length && ($body[$pos] === ' ' || $body[$pos] === "\t" || $body[$pos] === "\n" || $body[$pos] === "\r")) {
            $pos++;
        }

        return $pos;
    }

    /** Offset just past the closing quote of the string starting at $start. */
    private static function stringEnd(string $body, int $start): int
    {
        $length = strlen($body);
        for ($i = $start + 1; $i < $length; $i++) {
            if ($body[$i] === '\\') {
                $i++;
            } elseif ($body[$i] === '"') {
                return $i + 1;
            }
        }

        return $length;
    }

    /** Offset just past the JSON value starting at $start (no trailing whitespace). */
    private static function valueEnd(string $body, int $start): ?int
    {
        $length = strlen($body);
        if ($start >= $length) {
            return null;
        }

        if ($body[$start] === '"') {
            return self::stringEnd($body, $start);
        }

        if ($body[$start] === '{' || $body[$start] === '[') {
            $depth = 0;
            for ($i = $start; $i < $length; $i++) {
                $c = $body[$i];
                if ($c === '"') {
                    $i = self::stringEnd($body, $i) - 1;
                } elseif ($c === '{' || $c === '[') {
                    $depth++;
                } elseif ($c === '}' || $c === ']') {
                    if (--$depth === 0) {
                        return $i + 1;
                    }
                }
            }

            return null;
        }

        // number, true, false, null: runs until a delimiter
        $i = $start;
        while ($i < $length && strpos(",} \t\n\r", $body[$i]) === false) {
            $i++;
        }

        return $i;
    }
}
