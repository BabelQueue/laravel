<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Feature;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Consumer\BabelQueueDispatcher;
use BabelQueue\Contracts\ShouldQueuePolyglot;
use BabelQueue\Exceptions\InvalidEnvelopeException;
use BabelQueue\Exceptions\UnknownUrnException;
use BabelQueue\Queue\BabelQueueRabbitQueue;
use BabelQueue\Queue\Jobs\BabelQueueJob;
use BabelQueue\Queue\Jobs\BabelQueueRabbitJob;
use BabelQueue\Tests\TestCase;
use BabelQueue\Validation\EnvelopeValidator;
use Illuminate\Queue\RedisQueue;
use Mockery;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

/**
 * Runs the vendored cross-SDK conformance suite (tests/conformance, a byte-for-byte copy
 * of the canonical conformance/ directory) through the Laravel adapter's own paths:
 *
 *   - consume: raw body -> the Redis / RabbitMQ polyglot job -> BabelQueueDispatcher;
 *   - produce: ShouldQueuePolyglot -> BabelQueueRabbitQueue -> the AMQP publish;
 *   - retry:   pop -> release() -> republish (the body must travel unchanged).
 *
 * Objects are parsed as stdClass so {} is never confused with [] and key order is ignored.
 */
final class AdapterConformanceTest extends TestCase
{
    private const DIR = __DIR__ . '/../conformance';

    /** @var array<int, array{string, string}> */
    private array $warnings = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->warnings = [];
        EnvelopeCodec::setWarningHandler(function (string $message, string $pointer): void {
            $this->warnings[] = [$message, $pointer];
        });
        ConformanceHandler::reset();
    }

    protected function tearDown(): void
    {
        EnvelopeCodec::setWarningHandler(null);
        ConformanceHandler::reset();

        parent::tearDown();
    }

    // ----------------------------------------------------------------- consume

    public function test_valid_cases_are_decoded_by_the_rabbit_job(): void
    {
        foreach ($this->cases('cases', true) as $case) {
            $job = $this->rabbitJob($this->fixture($case));
            $this->assertConsumed($case, $job);
        }
    }

    public function test_valid_cases_are_decoded_by_the_redis_job(): void
    {
        foreach ($this->cases('cases', true) as $case) {
            $raw = $this->fixture($case);
            $job = new BabelQueueJob($this->app, Mockery::mock(RedisQueue::class), $raw, $raw, 'babelqueue-redis', 'orders');
            $this->assertConsumed($case, $job);
        }
    }

    public function test_valid_cases_run_the_urn_mapped_handler_and_ack(): void
    {
        foreach ($this->cases('cases', true) as $case) {
            ConformanceHandler::reset();
            $expect = (array) $case['expect'];
            $dispatcher = new BabelQueueDispatcher($this->app, [(string) $expect['urn'] => ConformanceHandler::class]);

            $message = Mockery::mock(AMQPMessage::class);
            $message->shouldReceive('getBody')->andReturn($this->fixture($case));
            $message->shouldReceive('getDeliveryTag')->andReturn(1);
            $message->shouldReceive('has')->andReturn(false);
            $channel = Mockery::mock(AMQPChannel::class);
            $channel->shouldReceive('basic_ack')->once()->with(1);
            $message->shouldReceive('getChannel')->andReturn($channel);

            $job = new BabelQueueRabbitJob($this->app, Mockery::mock(BabelQueueRabbitQueue::class), $message, 'babelqueue-rabbitmq', 'orders');
            $dispatcher->dispatch($job);

            $name = (string) $case['name'];
            self::assertSame(1, ConformanceHandler::$calls, $name . ': handler ran once');
            self::assertTrue(self::deepEqual(self::toObject($expect['data']), self::toObject(ConformanceHandler::$data)), $name . ': data');
            self::assertSame((string) $expect['urn'], ConformanceHandler::$urn, $name . ': urn');
            self::assertSame(self::parse($this->fixture($case))->trace_id, ConformanceHandler::$traceId, $name . ': trace_id forwarded');
        }
    }

    public function test_missing_urn_is_rejected(): void
    {
        $case = $this->caseNamed('cases', 'invalid-missing-urn');
        self::assertFalse((bool) $case['valid']);

        $job = $this->rabbitJob($this->fixture($case));
        self::assertSame('', $job->getUrn());

        $this->expectException(UnknownUrnException::class);
        (new BabelQueueDispatcher($this->app, ['urn:babel:orders:created' => ConformanceHandler::class]))->dispatch($job);
    }

    public function test_list_shaped_data_is_rejected(): void
    {
        $case = $this->caseNamed('data_shape', 'data-array-rejected');
        self::assertFalse((bool) $case['valid']);

        $job = $this->rabbitJob($this->fixture($case));
        self::assertSame('', $job->getUrn(), 'a list-shaped data block leaves the message without an identity');
        self::assertNotEmpty($this->warnings, 'the rejection is surfaced as a warning');

        $this->expectException(UnknownUrnException::class);
        (new BabelQueueDispatcher($this->app, ['urn:babel:orders:created' => ConformanceHandler::class]))->dispatch($job);
    }

    /**
     * Contract: "Consumers MUST reject (or quarantine) versions they do not understand"
     * (message-envelope.md, `invalid-unknown-schema-version`). The dispatcher rejects the
     * message (default `fail` strategy) before any handler runs.
     */
    public function test_unsupported_schema_version_is_rejected(): void
    {
        $case = $this->caseNamed('cases', 'invalid-unknown-schema-version');
        self::assertFalse((bool) $case['valid']);

        $job = $this->rabbitJob($this->fixture($case));
        $dispatcher = new BabelQueueDispatcher($this->app, [
            (string) $job->getUrn() => ConformanceHandler::class,
        ]);

        try {
            $dispatcher->dispatch($job);
            self::fail('an unsupported meta.schema_version must be rejected');
        } catch (InvalidEnvelopeException $e) {
            self::assertSame(EnvelopeValidator::REASON_UNSUPPORTED_SCHEMA_VERSION, $e->reason());
        }

        self::assertSame(0, ConformanceHandler::$calls, 'the handler must never run for a rejected envelope');
    }

    public function test_forbidden_keys_warn_and_are_not_forwarded(): void
    {
        foreach ($this->cases('forbidden_keys') as $case) {
            $name = (string) $case['name'];
            $forbidden = (string) $case['forbidden_key'];
            self::assertSame('warn', $case['expect'], $name);

            $this->warnings = [];
            $job = $this->rabbitJob($this->fixture($case));
            self::assertNotSame('', $job->getUrn(), $name . ': decode must succeed');

            $named = array_filter($this->warnings, static fn (array $w): bool => str_contains($w[0], $forbidden));
            self::assertNotEmpty($named, $name . ': a warning must name ' . $forbidden);

            // The handler never sees the forbidden key (decode strips it).
            foreach ((array) $case['expect_absent_after_reencode'] as $pointer) {
                $tokens = self::tokens((string) $pointer);
                if (count($tokens) === 2 && $tokens[0] === 'meta') {
                    self::assertArrayNotHasKey($tokens[1], $job->getMeta(), $name . ': ' . $pointer);
                } elseif (count($tokens) === 1) {
                    self::assertArrayNotHasKey($tokens[0], EnvelopeCodec::decode($this->fixture($case), static fn () => null), $name . ': ' . $pointer);
                }
            }
        }
    }

    // ----------------------------------------------------------------- produce

    public function test_data_shape_encode_cases_are_emitted_through_the_rabbit_producer(): void
    {
        foreach ($this->cases('data_shape') as $case) {
            if ($case['mode'] !== 'encode') {
                continue;
            }
            $name = (string) $case['name'];

            $published = [];
            $channel = $this->channel();
            $channel->shouldReceive('basic_publish')->once()->andReturnUsing(
                static function (AMQPMessage $message) use (&$published): void {
                    $published[] = $message;
                },
            );

            $job = new ConformanceProducerJob((string) $case['urn'], (array) $case['data']);
            $this->makeQueue($channel, ['queue' => (string) $case['queue']])->push($job);

            self::assertCount(1, $published, $name);
            $out = self::parse($published[0]->getBody());
            $raw = json_encode($out->data, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
            self::assertSame($case['expect_encoded_data_json'], $raw, $name . ': encoded data');
            self::assertSame((string) $case['urn'], $out->job, $name . ': job carries the URN');
            self::assertSame(1, $out->meta->schema_version, $name);
            self::assertTrue(EnvelopeCodec::accepts(EnvelopeCodec::decode($published[0]->getBody())), $name . ': own output is accepted');
        }
    }

    public function test_the_producer_never_emits_forbidden_keys(): void
    {
        $published = [];
        $channel = $this->channel();
        $channel->shouldReceive('basic_publish')->once()->andReturnUsing(
            static function (AMQPMessage $message) use (&$published): void {
                $published[] = $message;
            },
        );

        $this->makeQueue($channel)->push(new ConformanceProducerJob('urn:babel:orders:created', ['order_id' => 1]));

        $out = self::parse($published[0]->getBody());
        foreach (['/timestamp', '/meta/ts', '/meta/source', '/meta/max_retries', '/meta/attempts'] as $pointer) {
            self::assertFalse(self::has($out, $pointer), $pointer);
        }
        self::assertSame(0, $out->attempts);
    }

    // ------------------------------------------------------------------- retry

    /**
     * On retry the RabbitMQ driver republishes the body normalised through the core codec
     * (decode -> encode, ADR-0031) and carries the attempt counter in the `x-attempts` header
     * (broker-bindings.md §2), so every unknown key, the `{}` data block and an unknown
     * `meta.lang` survive, and a `urn`-only body is re-emitted with `job`.
     */
    public function test_roundtrip_cases_survive_a_release_republish(): void
    {
        foreach ($this->cases('roundtrip') as $case) {
            $name = (string) $case['name'];
            $body = $this->fixture($case);

            $published = [];
            $channel = $this->channel();
            $channel->shouldReceive('basic_publish')->once()->andReturnUsing(
                static function (AMQPMessage $message) use (&$published): void {
                    $published[] = $message;
                },
            );
            $channel->shouldReceive('basic_ack')->once()->with(1);

            $message = Mockery::mock(AMQPMessage::class);
            $message->shouldReceive('getBody')->andReturn($body);
            $message->shouldReceive('getChannel')->andReturn($channel);
            $message->shouldReceive('getDeliveryTag')->andReturn(1);
            $message->shouldReceive('has')->andReturn(false);
            $message->shouldReceive('get_properties')->andReturn([]);
            $channel->shouldReceive('basic_get')->once()->andReturn($message);

            $job = $this->makeQueue($channel)->pop('orders');
            self::assertInstanceOf(BabelQueueRabbitJob::class, $job, $name);
            $job->release();

            self::assertCount(1, $published, $name);
            // Expectations come from the fixture, not from the implementation under test: a retry
            // re-emits the very same bytes, except that an inbound `urn` alias moves into `job`.
            if (! isset($case['expect_absent_after_reencode'])) {
                self::assertSame($body, $published[0]->getBody(), $name . ': a release republishes the body byte for byte');
            }

            /** @var AMQPTable $table */
            $table = $published[0]->get('application_headers');
            self::assertSame($case['expect_attempts'], $table->getNativeData()['x-attempts'], $name . ': attempts + 1 in x-attempts');

            $out = self::parse($published[0]->getBody());
            foreach ((array) ($case['expect_absent_after_reencode'] ?? []) as $pointer) {
                self::assertFalse(self::has($out, (string) $pointer), sprintf('%s: %s must be gone after the re-emit', $name, $pointer));
            }
            foreach ($this->expectedPreserved($name) as $pointer => $expected) {
                self::assertTrue(
                    self::deepEqual($expected, self::pointer($out, $pointer, $name)),
                    sprintf('%s: %s must be preserved', $name, $pointer),
                );
            }
        }
    }

    // ----------------------------------------------------------------- helpers

    /** @param array<string, mixed> $case */
    private function assertConsumed(array $case, \BabelQueue\Contracts\PolyglotMessage $job): void
    {
        $name = (string) $case['name'];
        $expect = (array) $case['expect'];
        $source = self::parse($this->fixture($case));

        self::assertSame($expect['urn'], $job->getUrn(), $name . ': urn');
        self::assertTrue(self::deepEqual(self::toObject($expect['data']), self::toObject($job->getData())), $name . ': data');
        self::assertSame($source->trace_id, $job->getTraceId(), $name . ': trace_id preserved');
        self::assertSame($expect['lang'], $job->getMeta()['lang'], $name . ': lang');
        self::assertSame($expect['schema_version'], $job->getMeta()['schema_version'], $name . ': schema_version');
        self::assertSame($source->meta->id, $job->getMeta()['id'], $name . ': meta.id');
        self::assertSame($expect['attempts'], $source->attempts, $name . ': fixture attempts');
    }

    private function rabbitJob(string $body): BabelQueueRabbitJob
    {
        $message = Mockery::mock(AMQPMessage::class);
        $message->shouldReceive('getBody')->andReturn($body);
        $message->shouldReceive('getDeliveryTag')->andReturn(1);
        $message->shouldReceive('has')->andReturn(false);
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('basic_ack')->with(1)->zeroOrMoreTimes();
        $message->shouldReceive('getChannel')->andReturn($channel);

        return new BabelQueueRabbitJob($this->app, Mockery::mock(BabelQueueRabbitQueue::class), $message, 'babelqueue-rabbitmq', 'orders');
    }

    /** @param array<string,mixed> $config */
    private function makeQueue(AMQPChannel $channel, array $config = []): BabelQueueRabbitQueue
    {
        $connection = Mockery::mock(AbstractConnection::class);
        $connection->shouldReceive('isConnected')->andReturn(true);
        $connection->shouldReceive('channel')->andReturn($channel);
        $connection->shouldReceive('close')->andReturnNull();
        $connection->shouldReceive('isWriting')->andReturn(false);

        $queue = new BabelQueueRabbitQueue(static fn () => $connection, array_merge(['queue' => 'orders'], $config));
        $queue->setContainer($this->app);
        $queue->setConnectionName('babelqueue-rabbitmq');

        return $queue;
    }

    private function channel(): Mockery\MockInterface
    {
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('is_open')->andReturn(true);
        $channel->shouldReceive('queue_declare')->andReturn(['orders', 0, 0]);
        $channel->shouldReceive('exchange_declare')->andReturnNull();
        $channel->shouldReceive('queue_bind')->andReturnNull();
        $channel->shouldReceive('close')->andReturnNull();

        return $channel;
    }

    /** @return array<string, mixed> */
    private function manifest(): array
    {
        $path = self::DIR . '/manifest.json';
        self::assertFileExists($path, 'vendored conformance suite is missing');

        /** @var array<string, mixed> */
        return (array) json_decode((string) file_get_contents($path), true, 512, JSON_THROW_ON_ERROR);
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function cases(string $section, ?bool $valid = null): array
    {
        $block = $this->manifest()[$section] ?? null;
        $cases = is_array($block) ? ($block['cases'] ?? $block) : [];
        self::assertNotEmpty($cases, 'manifest has no ' . $section . ' cases');

        $cases = array_values(array_filter(
            $cases,
            static fn (array $c): bool => $valid === null || (bool) ($c['valid'] ?? true) === $valid,
        ));
        self::assertNotEmpty($cases, $section . ': no matching cases');

        /** @var list<array<string, mixed>> */
        return $cases;
    }

    /** @return array<string, mixed> */
    private function caseNamed(string $section, string $name): array
    {
        foreach ($this->cases($section) as $case) {
            if ($case['name'] === $name) {
                return $case;
            }
        }

        self::fail($section . ' case not found: ' . $name);
    }

    /**
     * `expect_preserved` re-read with objects kept as stdClass, so {} stays {}.
     *
     * @return array<string, mixed>
     */
    private function expectedPreserved(string $name): array
    {
        $raw = json_decode((string) file_get_contents(self::DIR . '/manifest.json'), false, 512, JSON_THROW_ON_ERROR);
        foreach ($raw->roundtrip->cases as $case) {
            if ($case->name === $name) {
                return get_object_vars($case->expect_preserved);
            }
        }

        self::fail('roundtrip case not found: ' . $name);
    }

    /** @param array<string, mixed> $case */
    private function fixture(array $case): string
    {
        $path = self::DIR . '/' . (string) $case['file'];
        self::assertFileExists($path);

        return (string) file_get_contents($path);
    }

    private static function parse(string $json): mixed
    {
        return json_decode($json, false, 512, JSON_THROW_ON_ERROR);
    }

    /** Normalise a decoded (assoc) value to the stdClass form used for strict comparison. */
    private static function toObject(mixed $value): mixed
    {
        return json_decode(json_encode($value, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION), false, 512, JSON_THROW_ON_ERROR);
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

/** Records what the dispatcher hands to a URN-mapped handler. */
final class ConformanceHandler
{
    public static int $calls = 0;

    /** @var array<string, mixed> */
    public static array $data = [];

    public static string $traceId = '';

    public static string $urn = '';

    public static function reset(): void
    {
        self::$calls = 0;
        self::$data = [];
        self::$traceId = '';
        self::$urn = '';
    }

    /**
     * @param  array<string, mixed>  $data
     * @param  array<string, mixed>  $meta
     */
    public function handle(array $data, array $meta, string $traceId, \BabelQueue\Contracts\PolyglotMessage $message): void
    {
        self::$calls++;
        self::$data = $data;
        self::$traceId = $traceId;
        self::$urn = $message->getUrn();
    }
}

final class ConformanceProducerJob implements ShouldQueuePolyglot
{
    /** @param array<string, mixed> $payload */
    public function __construct(private string $urn, private array $payload)
    {
    }

    public function getBabelUrn(): string
    {
        return $this->urn;
    }

    /** @return array<string, mixed> */
    public function toPayload(): array
    {
        return $this->payload;
    }
}
