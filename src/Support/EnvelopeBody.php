<?php

declare(strict_types=1);

namespace BabelQueue\Support;

/**
 * Prepares a raw envelope body for being written back to a broker (release/republish)
 * without changing anything the broker did not already hold.
 *
 * A decode -> encode round trip through PHP arrays is lossy for the data channel (a nested
 * `{}` becomes `[]`, an integer above PHP_INT_MAX becomes a float), so the body is NOT
 * re-serialised. It is scanned member by member and every value is kept byte for byte; the only
 * edit is the one ADR-0031 (c) asks for on re-emit: a body that carries the inbound `urn` alias
 * is rewritten so the URN lives in `job` and `urn` is gone. A body without `urn`, a body the
 * scanner cannot make sense of (poison, non-object JSON, a non-string/blank `urn`, a duplicated
 * `job`/`urn` key) is returned unchanged, so a retry never loses a message.
 *
 * Unknown top-level and `meta` keys, empty objects and big integers therefore survive as-is.
 */
final class EnvelopeBody
{
    /** The ASCII blank set shared with the core codec (`EnvelopeCodec::BLANK_CHARS`). */
    private const BLANK_CHARS = " \t\n\v\f\r";

    public static function normalize(string $rawBody): string
    {
        // Cheap exit: no member can be named "urn" without the substring being present.
        if (! str_contains($rawBody, 'urn')) {
            return $rawBody;
        }

        if (! is_array(json_decode($rawBody, true))) {
            return $rawBody;
        }

        $members = self::members($rawBody);
        if ($members === null) {
            return $rawBody;
        }

        $jobIndex = null;
        $urnIndex = null;
        foreach ($members as $i => $member) {
            if ($member['key'] === 'job') {
                if ($jobIndex !== null) {
                    return $rawBody;
                }
                $jobIndex = $i;
            } elseif ($member['key'] === 'urn') {
                if ($urnIndex !== null) {
                    return $rawBody;
                }
                $urnIndex = $i;
            }
        }

        if ($urnIndex === null) {
            return $rawBody;
        }

        $parts = array_map(static fn (array $m): string => $m['raw'], $members);

        if ($jobIndex !== null) {
            $job = json_decode($members[$jobIndex]['value']);
            $jobBlank = $job === null || (is_string($job) && trim($job, self::BLANK_CHARS) === '');

            if ($jobBlank) {
                $urn = json_decode($members[$urnIndex]['value']);
                if (! is_string($urn) || trim($urn, self::BLANK_CHARS) === '') {
                    return $rawBody;
                }
                $parts[$jobIndex] = $members[$jobIndex]['rawKey'] . ':' . $members[$urnIndex]['value'];
            }

            unset($parts[$urnIndex]);
        } else {
            $urn = json_decode($members[$urnIndex]['value']);
            if (! is_string($urn) || trim($urn, self::BLANK_CHARS) === '') {
                return $rawBody;
            }
            $parts[$urnIndex] = '"job":' . $members[$urnIndex]['value'];
        }

        return '{' . implode(',', $parts) . '}';
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
