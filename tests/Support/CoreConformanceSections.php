<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Support;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\DeadLetter\DeadLetter;
use BabelQueue\Exceptions\BabelQueueException;
use BabelQueue\Exceptions\InvalidEnvelopeException;
use BabelQueue\Idempotency\Idempotent;
use BabelQueue\Idempotency\InMemoryStore;
use BabelQueue\Schema\PayloadValidator;

/**
 * The broker-free conformance sections this adapter proves by delegating to the `babelqueue/php-sdk`
 * core (it adds no codec of its own): `extension_write`, `encode_reject`, `payload_schema`,
 * `payload_schema_unicode`, `trim`, `job_type` and `idempotency`. Each case is recorded for
 * `results.json` (ADR-0032) through {@see ConformanceResults::each()}.
 *
 * Shared by the test class that also runs the adapter-specific sections.
 */
trait CoreConformanceSections
{
    /** ADR-0024: this validator must agree with every other SDK on each case's `valid` flag. */
    public function test_payload_schema_cases(): void
    {
        /** @var array<string, mixed> $schema */
        $schema = ConformanceResults::manifest()['payload_schema']['schema'];

        ConformanceResults::each('payload_schema', static function (array $case) use ($schema): void {
            /** @var array<string, mixed> $data */
            $data = is_array($case['data'] ?? null) ? $case['data'] : [];
            $valid = PayloadValidator::check($schema, $data) === null;
            self::assertSame((bool) $case['valid'], $valid, 'case ' . (string) $case['name']);
        });
    }

    public function test_extension_write_cases(): void
    {
        ConformanceResults::each('extension_write', function (\stdClass $case): void {
            $name = (string) $case->name;
            /** @var array<string, mixed> $meta */
            $meta = get_object_vars($case->meta);
            /** @var array<string, mixed> $data */
            $data = (array) $case->data;

            $envelope = EnvelopeCodec::make((string) $case->urn, $data, (string) $case->queue, meta: $meta);
            $out = self::parse(EnvelopeCodec::encode($envelope));
            self::assertSame([], self::pointerViolations($out, $case->expect_envelope, $name . ' (encode)'), $name);

            // Re-emit chain: decode -> attempts+1 -> encode.
            $warnings = [];
            $decoded = EnvelopeCodec::decode(EnvelopeCodec::encode($envelope), self::collector($warnings));
            self::assertSame([], $warnings, $name . ': decode must not warn');
            self::assertIsInt($decoded['attempts']);
            $decoded['attempts']++;
            $again = self::parse(EnvelopeCodec::encode($decoded));
            self::assertSame([], self::pointerViolations($again, $case->expect_envelope, $name . ' (re-emit)'), $name);
            self::assertSame(1, self::pointer($again, '/attempts', $name), $name . ': attempts');
        }, objects: true);
    }

    public function test_encode_reject_cases(): void
    {
        ConformanceResults::each('encode_reject', function (\stdClass $case): void {
            $name = (string) $case->name;
            self::assertSame('reject', $case->expect, $name);

            // A repeated key cannot be expressed through make(meta: [...]) (one map per call): ADR-0031
            // lets a write API that takes one map per call skip the `meta_entries` form.
            if (isset($case->meta_entries)) {
                ConformanceResults::skipCase('ADR-0031: make(meta: [...]) takes one map per call, so a repeated meta key cannot be expressed');
            }

            $produced = null;

            try {
                /** @var array<string, mixed> $meta */
                $meta = isset($case->meta) ? get_object_vars($case->meta) : [];
                /** @var array<string, mixed> $data */
                $data = (array) $case->data;
                $envelope = EnvelopeCodec::make((string) $case->urn, $data, (string) $case->queue, meta: $meta);

                foreach (isset($case->extras_top) ? get_object_vars($case->extras_top) : [] as $key => $value) {
                    $envelope[(string) $key] = $value;
                }
                foreach (isset($case->extras_meta) ? get_object_vars($case->extras_meta) : [] as $key => $value) {
                    $envelope['meta'][(string) $key] = $value;
                }

                $produced = EnvelopeCodec::encode($envelope);
            } catch (BabelQueueException $e) {
                self::assertStringContainsString((string) $case->offending_key, $e->getMessage(), $name . ': message names the key');

                return;
            }

            self::fail($name . ': must be rejected, but produced ' . (string) $produced);
        }, objects: true);
    }

    public function test_payload_schema_unicode_cases(): void
    {
        /** @var array<string, mixed> $schema */
        $schema = ConformanceResults::manifest()['payload_schema_unicode']['schema'];

        ConformanceResults::each('payload_schema_unicode', static function (array $case) use ($schema): void {
            /** @var array<string, mixed> $data */
            $data = (array) $case['data'];
            $valid = PayloadValidator::check($schema, $data) === null;
            self::assertSame((bool) $case['valid'], $valid, 'case ' . (string) $case['name']);
        });
    }

    /**
     * ADR-0032: the ASCII trim set on `make` (urn, trace id), the blank-trace-id verdict of
     * `accepts`, and the rule that a re-emit never trims what it forwards.
     */
    public function test_trim_cases(): void
    {
        ConformanceResults::each('trim', function (\stdClass $case): void {
            $name = (string) $case->name;

            switch ($case->mode) {
                case 'make':
                    if ($case->expect === 'reject') {
                        try {
                            EnvelopeCodec::make((string) $case->urn);
                        } catch (BabelQueueException) {
                            return;
                        }
                        self::fail($name . ': make() must reject the blank urn');
                    }

                    $envelope = EnvelopeCodec::make((string) $case->urn, [], 'orders', $case->trace_id ?? null);
                    self::assertSame($case->expect->job, $envelope['job'], $name . ': job');
                    if (isset($case->expect->trace_id)) {
                        self::assertSame($case->expect->trace_id, $envelope['trace_id'], $name . ': trace_id');
                    }

                    return;

                case 'decode':
                    $this->assertDecodeVerdict($case);

                    return;

                case 'accepts':
                    $envelope = EnvelopeCodec::decode($this->fixture($case));
                    self::assertSame((bool) $case->expect_decode_ok, $envelope !== [], $name . ': decode');
                    self::assertSame((bool) $case->expect_accepts, EnvelopeCodec::accepts($envelope), $name . ': accepts');

                    return;

                case 'reemit':
                    $envelope = EnvelopeCodec::decode($this->fixture($case));
                    self::assertNotSame([], $envelope, $name . ': decode');
                    self::assertIsInt($envelope['attempts']);
                    $envelope['attempts']++;
                    $out = self::parse(EnvelopeCodec::encode($envelope));
                    self::assertSame([], self::pointerViolations($out, $case->expect_preserved, $name), $name);
                    self::assertSame($case->expect_attempts, self::pointer($out, '/attempts', $name), $name . ': attempts');

                    return;
            }

            self::fail($name . ': unknown trim mode ' . (string) $case->mode);
        }, objects: true);
    }

    /**
     * The installed `babelqueue/php-sdk` must carry the ADR-0032 codec rules (ASCII trim set, `job`
     * type rule). A core that predates them cannot satisfy the `trim` / `job_type` sections, so say
     * so plainly instead of failing on a confusing per-case diff; the composer floor must name a
     * core release that has them.
     */
    public function test_installed_core_carries_the_adr_0032_codec_rules(): void
    {
        $nonStringJob = EnvelopeCodec::decode('{"job":5,"data":{},"attempts":0,"meta":{"id":"x","trace_id":"t","schema_version":1}}');
        self::assertSame([], $nonStringJob, 'the installed babelqueue/php-sdk predates ADR-0032 (non-string `job` accepted): raise the composer constraint to a core release that carries it');

        self::assertSame(
            "\u{00A0}urn:acme:a",
            EnvelopeCodec::make(" \f\u{00A0}urn:acme:a\v")['job'],
            'the installed babelqueue/php-sdk predates ADR-0032 (trim set is not the ASCII set): raise the composer constraint',
        );
    }

    /** ADR-0032: trimming is linear; a pathological run of padding must not cost quadratic time. */
    public function test_trim_is_linear_on_huge_whitespace_runs(): void
    {
        $pad = str_repeat(" \t\n\v\f\r", 33334);
        $start = hrtime(true);

        try {
            EnvelopeCodec::make($pad);
            self::fail('an all-whitespace urn must be rejected');
        } catch (BabelQueueException) {
            // expected
        }
        self::assertSame('urn:acme:a', EnvelopeCodec::make($pad . 'urn:acme:a' . $pad)['job']);

        $body = '{"job":"urn:acme:a","data":{},"attempts":0,"meta":{"id":"x","trace_id":' . json_encode($pad) . ',"schema_version":1}}';
        EnvelopeCodec::accepts(EnvelopeCodec::decode($body));

        self::assertLessThan(1.0, (hrtime(true) - $start) / 1e9, 'whitespace handling must stay well under a second');
    }

    /** Unit test for the ASCII trim set: Unicode whitespace is data, not padding (ADR-0032). */
    public function test_trim_keeps_unicode_whitespace_and_trims_the_ascii_set(): void
    {
        $padded = "\u{00A0}urn:acme:a\u{3000}";
        self::assertSame($padded, EnvelopeCodec::make($padded)['job'], 'U+00A0 and U+3000 are preserved');
        self::assertSame($padded, EnvelopeCodec::make(" \f" . $padded . "\v\t")['job'], 'ASCII padding stops at the first non-ASCII-whitespace byte');
        self::assertSame("\u{00A0}", EnvelopeCodec::make("\u{00A0}")['job'], 'a URN of only U+00A0 is not blank');
        self::assertSame("\0urn:acme:a", EnvelopeCodec::make("\0urn:acme:a")['job'], 'NUL is not in the trim set');

        $this->expectException(BabelQueueException::class);
        EnvelopeCodec::make("\f\v \t");
    }

    /** ADR-0032 section 6: `job` must be a string; a non-string `job` falls back to `urn` with a warning. */
    public function test_job_type_cases(): void
    {
        ConformanceResults::each('job_type', function (\stdClass $case): void {
            $this->assertDecodeVerdict($case);
        }, objects: true);
    }

    /**
     * `retry_backoff` (ADR-0034 §9) is pinned on the SDK's worker-side backoff helper (`Consume\RetryBackoff` in
     * `babelqueue/php-sdk`, used by its own `Dispatcher`). This adapter does not use that path:
     * Laravel retries through the job's own `backoff()` / `retryUntil()` (GR-6), so there is no helper of the adapter's own to run the formula cases against.
     */
    public function test_retry_backoff_cases(): void
    {
        ConformanceResults::skipAll(
            'retry_backoff',
            'ADR-0034 §9: the worker-side backoff helper is the php-sdk Dispatcher\'s; Laravel retries through the job\'s own `backoff()` / `retryUntil()` (GR-6) and applies no BabelQueue backoff parameters',
        );
        self::assertTrue(true);
    }

    /** ADR-0022: the seen-set contract of `Idempotent::wrap`, keyed on `meta.id` verbatim. */
    public function test_idempotency_dedup_key(): void
    {
        ConformanceResults::each('idempotency', function (array $case, string $id): void {
            self::assertSame('dedup_key', $id);
            self::assertSame('meta.id', $case['key_field']);

            $envelope = EnvelopeCodec::decode($this->fixture(['file' => $case['envelope_file']]));
            self::assertSame($case['expected_key'], $envelope['meta']['id'] ?? null, 'meta.id is the dedupe key');

            $store = new InMemoryStore();
            $runs = 0;
            $handler = Idempotent::wrap($store, static function () use (&$runs): void {
                $runs++;
            });
            $handler(new EnvelopeMessage($envelope));
            $handler(new EnvelopeMessage($envelope));

            self::assertSame(1, $runs, 'a byte-identical redelivery is the same message');
            self::assertTrue($store->seen($case['expected_key']), 'the seen-set is keyed on meta.id verbatim');
        }, 'dedup_key');
    }

    public function test_idempotency_sequences(): void
    {
        ConformanceResults::each('idempotency', function (array $case): void {
            $name = (string) $case['name'];
            $store = new InMemoryStore();
            $effects = 0;

            /** @var array<string, mixed> $delivery */
            foreach ($case['deliveries'] as $index => $delivery) {
                $metaId = (string) $delivery['meta_id'];
                if (! empty($delivery['forget_before'])) {
                    $store->forget($metaId);
                }

                $ran = false;
                $handler = Idempotent::wrap($store, static function () use (&$ran, $delivery): void {
                    $ran = true;
                    if (($delivery['outcome'] ?? 'ok') === 'throw') {
                        throw new \RuntimeException('handler failed');
                    }
                });

                $envelope = EnvelopeCodec::make('urn:acme:a');
                $envelope['meta']['id'] = $metaId;

                try {
                    $handler(new EnvelopeMessage($envelope));
                } catch (\RuntimeException) {
                    // A throwing handler leaves the id unmarked; the runtime would redeliver.
                }

                self::assertSame($delivery['expect_effect'] === 'run', $ran, sprintf('%s: delivery %d effect', $name, $index));
                $effects += $ran ? 1 : 0;
            }

            self::assertSame($case['expected_effects'], $effects, $name . ': total effects');
        }, 'sequences');
    }

    public function test_the_runner_reports_violations_of_the_optional_fields(): void
    {
        $body = '{"job":"urn:acme:a","trace_id":"t","data":{"order_id":1},"meta":{"id":"i","queue":"q","lang":"php","schema_version":1,"created_at":1,"vendor_flag":true},"extra_top":1,"attempts":0}';

        $good = (object) [
            'name' => 'synthetic',
            'expect_attempts' => 1,
            'expect_preserved' => (object) ['/meta/vendor_flag' => true, '/extra_top' => 1],
            'expect_absent_after_reencode' => ['/meta/missing'],
        ];
        self::assertSame([], $this->roundtripViolations($good, $body), 'a satisfied case reports nothing');

        $absent = clone $good;
        $absent->expect_absent_after_reencode = ['/extra_top'];
        self::assertNotSame([], $this->roundtripViolations($absent, $body), 'expect_absent_after_reencode: a present pointer');

        $preserved = clone $good;
        $preserved->expect_preserved = (object) ['/meta/vendor_flag' => false];
        self::assertNotSame([], $this->roundtripViolations($preserved, $body), 'expect_preserved: a different value');

        $retyped = clone $good;
        $retyped->expect_preserved = (object) ['/data' => []];
        self::assertNotSame([], $this->roundtripViolations($retyped, $body), 'expect_preserved: {} is not []');

        $deadLetter = clone $good;
        $deadLetter->apply_dead_letter = (object) ['reason' => 'failed', 'original_queue' => 'orders'];
        $deadLetter->expect_dead_letter = (object) ['reason' => 'failed', 'original_queue' => 'orders'];
        self::assertSame([], $this->roundtripViolations($deadLetter, $body), 'a satisfied dead-letter case reports nothing');

        $deadLetter->expect_dead_letter = (object) ['original_queue' => 'elsewhere'];
        self::assertNotSame([], $this->roundtripViolations($deadLetter, $body), 'expect_dead_letter: a different value');

        $forbiddenBody = '{"job":"urn:acme:a","trace_id":"t","data":{},"meta":{"schema_version":1},"attempts":0,"timestamp":1}';
        $forbidden = (object) [
            'name' => 'synthetic-forbidden',
            'forbidden_key' => '/timestamp',
            'expect' => 'warn',
            'expect_absent_after_reencode' => ['/timestamp'],
            'expect_preserved' => (object) ['/job' => 'urn:acme:a'],
        ];
        self::assertSame([], $this->forbiddenKeyViolations($forbidden, $forbiddenBody), 'a satisfied forbidden case reports nothing');

        $forbidden->expect_preserved = (object) ['/job' => 'urn:acme:other'];
        self::assertNotSame([], $this->forbiddenKeyViolations($forbidden, $forbiddenBody), 'forbidden_keys expect_preserved: a different value');

        $forbidden->expect_preserved = (object) [];
        $forbidden->expect_absent_after_reencode = ['/job'];
        self::assertNotSame([], $this->forbiddenKeyViolations($forbidden, $forbiddenBody), 'forbidden_keys: a present pointer');

        $forbidden->expect_absent_after_reencode = ['/timestamp'];
        $forbidden->forbidden_key = '/meta/ts';
        self::assertNotSame([], $this->forbiddenKeyViolations($forbidden, $forbiddenBody), 'forbidden_keys: no warning names the key');
    }

    public function test_encoder_never_emits_forbidden_keys(): void
    {
        $envelope = EnvelopeCodec::make('urn:babel:orders:created', ['order_id' => 1], 'orders');
        $envelope['timestamp'] = 1;
        $envelope['meta']['ts'] = 1;
        $envelope['meta']['source'] = 'php';
        $envelope['meta']['max_retries'] = 3;
        $envelope['meta']['attempts'] = 2;

        $this->expectException(InvalidEnvelopeException::class);
        $this->expectExceptionMessageMatches('#/timestamp, /meta/max_retries, /meta/attempts, /meta/source, /meta/ts#');

        EnvelopeCodec::encode($envelope);
    }

    /** Shared by `trim` (decode mode) and `job_type`: a decode verdict plus the urn and warning expectation. */
    private function assertDecodeVerdict(\stdClass $case): void
    {
        $name = (string) $case->name;
        $warnings = [];
        $envelope = EnvelopeCodec::decode($this->fixture($case), self::collector($warnings));

        if (! $case->valid) {
            self::assertSame([], $envelope, $name . ': decode must reject');

            return;
        }

        self::assertNotSame([], $envelope, $name . ': decode must accept');
        self::assertSame($case->expect->urn, EnvelopeCodec::urn($envelope), $name . ': urn');
        self::assertSame((bool) ($case->expect_warning ?? false), $warnings !== [], $name . ': warning');
    }

    /** Key-sorted JSON, so object key order is ignored while types stay strict. */
    private static function canonical(mixed $value): string
    {
        $sort = static function (mixed $v) use (&$sort): mixed {
            if (! is_array($v)) {
                return $v;
            }
            if (! array_is_list($v)) {
                ksort($v);
            }

            return array_map($sort, $v);
        };

        return json_encode($sort($value), JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    }

    /**
     * Decode -> attempts+1 [-> dead-letter] -> encode, then every assertion of the case.
     *
     * @return list<string> the violations; empty when the case holds
     */
    private function roundtripViolations(\stdClass $case, string $body): array
    {
        $name = (string) $case->name;
        $violations = [];

        $warnings = [];
        $envelope = EnvelopeCodec::decode($body, self::collector($warnings));
        if ($warnings !== []) {
            $violations[] = $name . ': decode must emit no warning, got ' . $warnings[0][0];
        }
        if (! EnvelopeCodec::accepts($envelope)) {
            return [...$violations, $name . ': decode must accept'];
        }

        self::assertIsInt($envelope['attempts']);
        $envelope['attempts']++;

        $deadLettered = isset($case->apply_dead_letter);
        if ($deadLettered) {
            $envelope = DeadLetter::annotate(
                $envelope,
                (string) $case->apply_dead_letter->reason,
                new \RuntimeException('conformance runner'),
                (string) $case->apply_dead_letter->original_queue,
                $envelope['attempts'],
            );
        }

        $out = self::parse(EnvelopeCodec::encode($envelope));

        if (isset($case->expect_attempts) && (! self::has($out, '/attempts') || self::pointer($out, '/attempts', $name) !== $case->expect_attempts)) {
            $violations[] = $name . ': /attempts must be ' . $case->expect_attempts;
        }

        $violations = [...$violations, ...self::pointerViolations($out, $case->expect_preserved ?? (object) [], $name)];
        $violations = [...$violations, ...self::absentViolations($out, (array) ($case->expect_absent_after_reencode ?? []), $name)];

        if ($deadLettered && isset($case->expect_dead_letter)) {
            if (! self::has($out, '/dead_letter')) {
                $violations[] = $name . ': /dead_letter must exist';
            } else {
                foreach (get_object_vars($case->expect_dead_letter) as $field => $expected) {
                    $pointer = '/dead_letter/' . $field;
                    if (! self::has($out, $pointer) || ! self::deepEqual($expected, self::pointer($out, $pointer, $name))) {
                        $violations[] = sprintf('%s: %s must be %s', $name, $pointer, json_encode($expected));
                    }
                }
            }
        }

        return $violations;
    }

    /**
     * Decode a forbidden-key fixture (must warn, naming the key), re-encode it, then the optional
     * `expect_preserved` / `expect_absent_after_reencode` assertions.
     *
     * @return list<string>
     */
    private function forbiddenKeyViolations(\stdClass $case, string $body): array
    {
        $name = (string) $case->name;
        $forbidden = (string) $case->forbidden_key;
        $violations = [];

        /** @var list<array{string, string}> $warnings */
        $warnings = [];
        $envelope = EnvelopeCodec::decode($body, self::collector($warnings));
        if (! EnvelopeCodec::accepts($envelope)) {
            return [$name . ': decode must succeed'];
        }

        $named = array_filter($warnings, static fn (array $w): bool => str_contains($w[0], $forbidden));
        if ($named === []) {
            $violations[] = $name . ': a warning must name ' . $forbidden;
        }

        $out = self::parse(EnvelopeCodec::encode($envelope));

        $violations = [...$violations, ...self::absentViolations($out, (array) ($case->expect_absent_after_reencode ?? []), $name)];
        $violations = [...$violations, ...self::pointerViolations($out, $case->expect_preserved ?? (object) [], $name)];

        $before = self::parse($body);
        if (self::pointer($before, '/attempts', $name) !== self::pointer($out, '/attempts', $name)) {
            $violations[] = $name . ': top-level attempts unchanged';
        }

        return $violations;
    }

    /**
     * Every pointer of `$expected` must hold the given value (type-strict) in `$doc`.
     *
     * @return list<string>
     */
    private static function pointerViolations(mixed $doc, \stdClass $expected, string $name): array
    {
        $violations = [];
        foreach (get_object_vars($expected) as $pointer => $value) {
            $pointer = (string) $pointer;
            if (! self::has($doc, $pointer)) {
                $violations[] = sprintf('%s: %s missing, expected %s', $name, $pointer, json_encode($value));
            } elseif (! self::deepEqual($value, self::pointer($doc, $pointer, $name))) {
                $violations[] = sprintf('%s: %s must be %s, got %s', $name, $pointer, json_encode($value), json_encode(self::pointer($doc, $pointer, $name)));
            }
        }

        return $violations;
    }

    /**
     * None of the pointers may exist in `$doc`.
     *
     * @param  list<mixed>  $pointers
     * @return list<string>
     */
    private static function absentViolations(mixed $doc, array $pointers, string $name): array
    {
        $violations = [];
        foreach ($pointers as $pointer) {
            if (self::has($doc, (string) $pointer)) {
                $violations[] = sprintf('%s: %s must be absent after re-encode', $name, $pointer);
            }
        }

        return $violations;
    }

    /** @param  array<string, mixed>|\stdClass  $case */
    private function fixture(array|\stdClass $case): string
    {
        $file = $case instanceof \stdClass ? $case->file : $case['file'];
        $path = dirname(__DIR__) . '/conformance/' . (string) $file;
        self::assertFileExists($path);

        return (string) file_get_contents($path);
    }

    /**
     * @param  array<int, array{string, string}>  $sink
     * @return callable(string, string): void
     */
    private static function collector(array &$sink): callable
    {
        return static function (string $message, string $pointer) use (&$sink): void {
            $sink[] = [$message, $pointer];
        };
    }

    private static function parse(string $json): mixed
    {
        return json_decode($json, false, 512, JSON_THROW_ON_ERROR);
    }

    /** @return list<string> */
    private static function tokens(string $pointer): array
    {
        if ($pointer === '') {
            return [];
        }

        return array_map(
            static fn (string $t): string => str_replace(['~1', '~0'], ['/', '~'], $t),
            array_slice(explode('/', $pointer), 1),
        );
    }

    private static function has(mixed $doc, string $pointer): bool
    {
        foreach (self::tokens($pointer) as $token) {
            if ($doc instanceof \stdClass && property_exists($doc, $token)) {
                $doc = $doc->{$token};
            } elseif (is_array($doc) && ctype_digit($token) && array_key_exists((int) $token, $doc)) {
                $doc = $doc[(int) $token];
            } else {
                return false;
            }
        }

        return true;
    }

    private static function pointer(mixed $doc, string $pointer, string $name): mixed
    {
        self::assertTrue(self::has($doc, $pointer), $name . ': ' . $pointer . ' missing');

        foreach (self::tokens($pointer) as $token) {
            $doc = $doc instanceof \stdClass ? $doc->{$token} : $doc[(int) $token];
        }

        return $doc;
    }

    private static function deepEqual(mixed $a, mixed $b): bool
    {
        if ($a instanceof \stdClass || $b instanceof \stdClass) {
            if (! $a instanceof \stdClass || ! $b instanceof \stdClass) {
                return false;
            }
            $av = get_object_vars($a);
            $bv = get_object_vars($b);
            if (count($av) !== count($bv)) {
                return false;
            }
            foreach ($av as $k => $v) {
                if (! array_key_exists($k, $bv) || ! self::deepEqual($v, $bv[$k])) {
                    return false;
                }
            }

            return true;
        }

        if (is_array($a) || is_array($b)) {
            if (! is_array($a) || ! is_array($b) || count($a) !== count($b)) {
                return false;
            }
            foreach (array_values($a) as $i => $v) {
                if (! self::deepEqual($v, array_values($b)[$i])) {
                    return false;
                }
            }

            return true;
        }

        return $a === $b;
    }
}
