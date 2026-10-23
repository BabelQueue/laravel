<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Feature;

use Aws\Result;
use Aws\Sqs\SqsClient;
use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Consumer\BabelQueueDispatcher;
use BabelQueue\Consumer\DeadLetterPublisher;
use BabelQueue\Contracts\HasTraceId;
use BabelQueue\Contracts\PolyglotMessage;
use BabelQueue\Contracts\ShouldQueuePolyglot;
use BabelQueue\Exceptions\BabelQueueException;
use BabelQueue\Exceptions\InvalidEnvelopeException;
use BabelQueue\Exceptions\UnknownUrnException;
use BabelQueue\Queue\BabelQueueArtemisQueue;
use BabelQueue\Queue\BabelQueueRabbitQueue;
use BabelQueue\Queue\BabelQueueRedisQueue;
use BabelQueue\Queue\BabelQueueSqsQueue;
use BabelQueue\Queue\Jobs\BabelQueueArtemisJob;
use BabelQueue\Queue\Jobs\BabelQueueJob;
use BabelQueue\Queue\Jobs\BabelQueueRabbitJob;
use BabelQueue\Queue\Jobs\BabelQueueSqsJob;
use BabelQueue\Tests\Support\ConformanceResults;
use BabelQueue\Tests\Support\CoreConformanceSections;
use BabelQueue\Tests\Support\SqsAttempts;
use BabelQueue\Tests\TestCase;
use BabelQueue\Validation\EnvelopeValidator;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Contracts\Redis\Factory as RedisFactory;
use Illuminate\Queue\LuaScripts;
use Illuminate\Queue\RedisQueue;
use Illuminate\Support\Carbon;
use Illuminate\Redis\Connections\Connection as RedisConnection;
use Mockery;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Stomp\StatefulStomp;
use Stomp\Transport\Frame;
use Stomp\Transport\Message;

/**
 * Runs the vendored cross-SDK conformance suite (tests/conformance, a byte-for-byte copy
 * of the canonical conformance/ directory) through the Laravel adapter's own paths:
 *
 *   - consume: raw body -> the Redis / RabbitMQ / SQS / Artemis polyglot job -> BabelQueueDispatcher;
 *   - produce: ShouldQueuePolyglot -> the driver -> the broker call (AMQP publish, Redis EVAL,
 *              SQS SendMessage, STOMP SEND);
 *   - retry:   pop -> release() -> republish (the body must travel unchanged).
 *
 * The sections the adapter shares with the core codec (payload_schema, trim, job_type, ...) run
 * through {@see CoreConformanceSections}. Every section x case outcome is recorded by
 * {@see ConformanceResults}, which writes `results.json` when `BQ_CONFORMANCE_RESULTS` is set.
 *
 * Objects are parsed as stdClass so {} is never confused with [] and key order is ignored.
 */
final class AdapterConformanceTest extends TestCase
{
    use CoreConformanceSections;

    private const SQS_PREFIX = 'https://sqs.eu-central-1.amazonaws.com/123456789012';

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

    /**
     * Valid cases are decoded by the RabbitMQ and Redis jobs and run the URN-mapped handler (then
     * ack); invalid ones are rejected before any handler runs.
     */
    public function test_cases_section(): void
    {
        ConformanceResults::each('cases', function (array $case): void {
            ConformanceHandler::reset();
            $body = $this->fixture($case);

            if ((bool) $case['valid']) {
                $this->assertConsumed($case, $this->rabbitJob($body));
                $this->assertConsumed($case, new BabelQueueJob($this->app, Mockery::mock(RedisQueue::class), $body, $body . ' ', 'babelqueue-redis', 'orders'));

                $expect = (array) $case['expect'];
                $dispatcher = new BabelQueueDispatcher($this->app, [(string) $expect['urn'] => ConformanceHandler::class]);

                $message = Mockery::mock(AMQPMessage::class);
                $message->shouldReceive('getBody')->andReturn($body);
                $message->shouldReceive('getDeliveryTag')->andReturn(1);
                $message->shouldReceive('has')->andReturn(false);
                $channel = Mockery::mock(AMQPChannel::class);
                $channel->shouldReceive('basic_ack')->once()->with(1);
                $message->shouldReceive('getChannel')->andReturn($channel);

                $dispatcher->dispatch(new BabelQueueRabbitJob($this->app, Mockery::mock(BabelQueueRabbitQueue::class), $message, 'babelqueue-rabbitmq', 'orders'));

                self::assertSame(1, ConformanceHandler::$calls, 'handler ran once');
                self::assertTrue(self::deepEqual(self::toObject($expect['data']), self::toObject(ConformanceHandler::$data)), 'data');
                self::assertSame((string) $expect['urn'], ConformanceHandler::$urn, 'urn');
                self::assertSame(self::parse($body)->trace_id, ConformanceHandler::$traceId, 'trace_id forwarded');

                return;
            }

            // Rejected: no identity (missing urn, wrong-case `JOB`) is an unknown URN; an identity with
            // an unsupported meta.schema_version is an InvalidEnvelopeException. Never a handler call.
            $job = $this->rabbitJob($body);
            if ($job->getUrn() === '') {
                $this->expectUnknownUrn($job);
            } else {
                $dispatcher = new BabelQueueDispatcher($this->app, [$job->getUrn() => ConformanceHandler::class]);
                try {
                    $dispatcher->dispatch($job);
                    self::fail('an unsupported meta.schema_version must be rejected');
                } catch (InvalidEnvelopeException $e) {
                    self::assertSame(EnvelopeValidator::REASON_UNSUPPORTED_SCHEMA_VERSION, $e->reason());
                }
            }
            self::assertSame(0, ConformanceHandler::$calls, 'the handler must never run for a rejected envelope');
        });
    }

    /** Both data_shape modes: `encode` runs the rabbit producer, `decode` the consume rejection. */
    public function test_data_shape_section(): void
    {
        ConformanceResults::each('data_shape', function (array $case): void {
            if ($case['mode'] === 'encode') {
                $published = [];
                $channel = $this->channel();
                $channel->shouldReceive('basic_publish')->once()->andReturnUsing(
                    static function (AMQPMessage $message) use (&$published): void {
                        $published[] = $message;
                    },
                );

                $this->makeQueue($channel, ['queue' => (string) $case['queue']])->push(new ConformanceProducerJob((string) $case['urn'], (array) $case['data']));

                self::assertCount(1, $published);
                $out = self::parse($published[0]->getBody());
                $raw = json_encode($out->data, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
                self::assertSame($case['expect_encoded_data_json'], $raw, 'encoded data');
                self::assertSame((string) $case['urn'], $out->job, 'job carries the URN');
                self::assertSame(1, $out->meta->schema_version);
                self::assertTrue(EnvelopeCodec::accepts(EnvelopeCodec::decode($published[0]->getBody())), 'own output is accepted');

                return;
            }

            self::assertFalse((bool) $case['valid']);
            $job = $this->rabbitJob($this->fixture($case));
            self::assertSame('', $job->getUrn(), 'a list-shaped data block leaves the message without an identity');
            self::assertNotEmpty($this->warnings, 'the rejection is surfaced as a warning');
            $this->expectUnknownUrn($job);
        });
    }

    /**
     * Forbidden keys warn and are not forwarded on decode, and a retry (release -> republish) never
     * puts them back on the wire.
     */
    public function test_forbidden_keys_section(): void
    {
        ConformanceResults::each('forbidden_keys', function (array $case): void {
            $forbidden = (string) $case['forbidden_key'];
            self::assertSame('warn', $case['expect']);
            $body = $this->fixture($case);

            $this->warnings = [];
            $job = $this->rabbitJob($body);
            self::assertNotSame('', $job->getUrn(), 'decode must succeed');

            $named = array_filter($this->warnings, static fn (array $w): bool => str_contains($w[0], $forbidden));
            self::assertNotEmpty($named, 'a warning must name ' . $forbidden);

            self::assertNotEmpty((array) $case['expect_absent_after_reencode']);
            foreach ((array) $case['expect_absent_after_reencode'] as $pointer) {
                $tokens = self::tokens((string) $pointer);
                if (count($tokens) === 2 && $tokens[0] === 'meta') {
                    self::assertArrayNotHasKey($tokens[1], $job->getMeta(), (string) $pointer);
                } elseif (count($tokens) === 1) {
                    self::assertArrayNotHasKey($tokens[0], EnvelopeCodec::decode($body, static fn () => null), (string) $pointer);
                }
            }

            $out = self::parse($this->releaseAndCapture($body)->getBody());
            foreach ((array) $case['expect_absent_after_reencode'] as $pointer) {
                self::assertFalse(self::has($out, (string) $pointer), sprintf('%s must not be republished', $pointer));
            }
        });
    }

    // ------------------------------------------------------------------- retry

    /**
     * On retry the RabbitMQ driver republishes the body normalised through the core codec
     * (decode -> encode, ADR-0031) and carries the attempt counter in the `x-attempts` header
     * (broker-bindings.md §2), so every unknown key, the `{}` data block and an unknown
     * `meta.lang` survive, and a `urn`-only body is re-emitted with `job`. A dead-letter case also
     * runs the DLQ move through the adapter's {@see DeadLetterPublisher}.
     */
    public function test_roundtrip_section(): void
    {
        ConformanceResults::each('roundtrip', function (\stdClass $case): void {
            $name = (string) $case->name;
            $body = $this->fixture($case);

            $message = $this->releaseAndCapture($body);
            // Expectations come from the fixture, not from the implementation under test: a retry
            // re-emits the very same bytes, except that an inbound `urn` alias moves into `job`.
            if (! isset($case->expect_absent_after_reencode)) {
                self::assertSame($body, $message->getBody(), 'a release republishes the body byte for byte');
            }

            /** @var AMQPTable $table */
            $table = $message->get('application_headers');
            self::assertSame($case->expect_attempts, $table->getNativeData()['x-attempts'], 'attempts + 1 in x-attempts');

            $this->assertRoundtripBody($case, self::parse($message->getBody()), $name);

            if (isset($case->apply_dead_letter)) {
                $this->assertRoundtripBody($case, self::parse($this->deadLetterBody($case, $body)), $name . ' (dead-lettered)', true);
            }
        }, null, true);
    }

    // ----------------------------------------------------------------- rabbitmq

    /**
     * §2.2 property projection. The driver generates `meta.id` itself, so the fixture id cannot be
     * forced through a job: `message_id` is compared with the published body's own `meta.id`, and
     * the trace id is pinned through {@see HasTraceId}.
     */
    public function test_rabbitmq_property_projection(): void
    {
        ConformanceResults::each('rabbitmq', function (array $case): void {
            $expect = (array) $case['properties'];
            $headers = (array) $case['headers'];

            $published = [];
            $channel = $this->channel();
            $channel->shouldReceive('basic_publish')->once()->andReturnUsing(
                static function (AMQPMessage $message) use (&$published): void {
                    $published[] = $message;
                },
            );

            $fixture = self::parse($this->fixture(['file' => $case['envelope_file']]));
            $this->makeQueue($channel)->push(new ConformanceTracedProducerJob((string) $fixture->job, (array) json_decode(json_encode($fixture->data), true), (string) $fixture->trace_id));

            self::assertCount(1, $published);
            $message = $published[0];
            $out = self::parse($message->getBody());

            self::assertSame($expect['type'], $message->get('type'), 'type = job');
            self::assertSame($expect['correlation_id'], $message->get('correlation_id'), 'correlation_id = trace_id');
            self::assertSame($out->meta->id, $message->get('message_id'), 'message_id = meta.id');
            self::assertSame($expect['app_id'], $message->get('app_id'), 'app_id');
            self::assertSame($expect['content_type'], $message->get('content_type'), 'content_type');

            /** @var AMQPTable $table */
            $table = $message->get('application_headers');
            $native = $table->getNativeData();
            foreach ($headers as $name => $value) {
                self::assertArrayHasKey($name, $native, $name);
                self::assertSame($value, $native[$name], $name . ' (native-typed)');
            }
        }, 'property_projection');
    }

    // -------------------------------------------------------------------- redis

    /** §1: the queue element is the envelope, byte for byte, in and out. */
    public function test_redis_payload_identity(): void
    {
        ConformanceResults::each('redis', function (array $case): void {
            $body = $this->fixture(['file' => $case['envelope_file']]);

            $pushed = [];
            $queue = $this->redisQueue(function (array $args) use (&$pushed, $body): mixed {
                if ($args[0] === LuaScripts::push()) {
                    $pushed[] = $args[4];
                }
                if ($args[0] === BabelQueueRedisQueue::popScript()) {
                    return [$body, $body];
                }

                return [];
            });

            $queue->pushRaw($body, 'orders');
            self::assertSame([$body], $pushed, 'the element stored is the envelope, byte for byte');

            $job = $queue->pop('orders');
            self::assertInstanceOf(BabelQueueJob::class, $job);
            self::assertSame($body, $job->getRawBody(), 'reserve returns the element unchanged');
        }, 'payload_identity');
    }

    /** §1.6 / ADR-0033: the delayed score is `floor(now) + ceil(delay)`; pure, clock pinned. */
    public function test_redis_delay_score(): void
    {
        $queue = $this->redisQueue(static fn (array $args): mixed => []);
        $availableAt = new \ReflectionMethod($queue, 'availableAt');

        ConformanceResults::each('redis_delay', static function (array $case) use ($queue, $availableAt): void {
            Carbon::setTestNow(Carbon::createFromTimestamp((int) floor((float) $case['now'])));
            try {
                self::assertSame($case['expect_score'], $availableAt->invoke($queue, $case['delay']), (string) $case['name']);
            } finally {
                Carbon::setTestNow();
            }
        });
    }

    /**
     * The `delayed_score` behavioural block against the real `later` path on a recording client: one write
     * to `:delayed`, the member the body byte for byte, the integer score, no notify token. The live
     * Redis blocks run in `tests/Integration/RedisLayoutConformanceTest.php`.
     */
    public function test_redis_layout_delayed_score(): void
    {
        ConformanceResults::each('redis_layout', function (array $case): void {
            $queueName = (string) $case['queue'];
            self::assertCount(1, $case['steps']);
            $step = $case['steps'][0];
            self::assertSame('push_delayed', $step['op']);

            /** @var list<array<int, mixed>> $writes */
            $writes = [];
            $queue = $this->redisQueue(function (array $args) use (&$writes): mixed {
                if ($args[0] === LuaScripts::later() || $args[0] === 'zadd') {
                    $writes[] = $args;
                }

                return [];
            });

            Carbon::setTestNow(Carbon::createFromTimestamp((int) floor((float) $step['now'])));
            try {
                (new \ReflectionMethod($queue, 'laterRaw'))->invoke($queue, $step['delay'], $step['body'], $queueName);
            } finally {
                Carbon::setTestNow();
            }

            self::assertCount(1, $writes, 'one write to the delayed set');
            self::assertSame('queues:' . $queueName . ':delayed', $writes[0][2], 'the delayed key');
            self::assertSame($step['expect']['delayed'][0]['member'], $writes[0][4], 'the member is the body byte for byte');
            self::assertSame($step['expect']['delayed'][0]['score'], (int) $writes[0][3], 'the score is floor(now) + ceil(delay)');
            self::assertSame(0, (int) ($case['state']['notify'] ?? 0), 'a delayed push writes no notify token');
        }, 'delayed_score');
    }

    /** §1.1: the Laravel-compatible key layout (`queues:<q>` + `:delayed` / `:reserved` / `:notify`). */
    public function test_redis_layout_cases(): void
    {
        ConformanceResults::each('redis_layout', function (array $case): void {
            $queueName = (string) $case['queue'];
            $expect = (array) $case['expect'];
            $prefix = (string) $case['key_prefix'];

            /** @var list<array<int, mixed>> $evals */
            $evals = [];
            $queue = $this->redisQueue(function (array $args) use (&$evals): mixed {
                $evals[] = $args;

                return [];
            });

            $queue->push(new ConformanceProducerJob('urn:babel:orders:created', ['order_id' => 1]), '', $queueName);
            $queue->later(30, new ConformanceProducerJob('urn:babel:orders:created', ['order_id' => 1]), '', $queueName);
            $queue->pop($queueName);

            $byScript = static function (string $script) use (&$evals): array {
                foreach ($evals as $args) {
                    if ($args[0] === $script) {
                        return $args;
                    }
                }

                return [];
            };

            $push = $byScript(LuaScripts::push());
            // LuaScripts::later() only exists from Laravel 12; Laravel 11 delays with a plain ZADD.
            $later = method_exists(LuaScripts::class, 'later') ? $byScript(LuaScripts::later()) : $byScript('zadd');
            $pop = $byScript(BabelQueueRedisQueue::popScript());
            self::assertNotSame([], $push, 'push reached Redis');
            self::assertNotSame([], $later, 'later reached Redis');
            self::assertNotSame([], $pop, 'pop reached Redis');

            self::assertStringStartsWith($prefix, $expect['ready']);
            self::assertSame($expect['ready'], $push[2], 'ready list');
            self::assertSame($expect['notify'], $push[3], 'notify list');
            self::assertSame($expect['delayed'], $later[2], 'delayed zset');
            self::assertSame($expect['ready'], $pop[2], 'pop reads the ready list');
            self::assertSame($expect['reserved'], $pop[3], 'reserved zset');
            self::assertSame($expect['notify'], $pop[4], 'pop notify list');
        });
    }

    // ---------------------------------------------------------------------- sqs

    /** §3.5 MessageAttributes projection on SendMessage. */
    public function test_sqs_attribute_projection(): void
    {
        ConformanceResults::each('sqs', function (array $case): void {
            $body = $this->fixture(['file' => $case['envelope_file']]);

            $captured = null;
            $sqs = Mockery::mock(SqsClient::class);
            $sqs->shouldReceive('sendMessage')->once()->with(
                Mockery::on(function (array $args) use (&$captured): bool {
                    $captured = $args;

                    return true;
                }),
            )->andReturn(new Result(['MessageId' => 'sqs-1']));

            $this->sqsQueue($sqs)->pushRaw($body, 'orders');

            self::assertSame($body, $captured['MessageBody'], 'the body is the envelope, byte for byte');
            foreach ((array) $case['message_attributes'] as $name => $expected) {
                self::assertArrayHasKey($name, $captured['MessageAttributes'], $name);
                self::assertSame($expected, $captured['MessageAttributes'][$name], $name);
            }
        }, 'attribute_projection');

        // §3.6: attempts = max(body.attempts, ApproximateReceiveCount - 1). The drop-in driver surfaces the
        // native count unreconciled, so the cases it cannot satisfy are listed in conformance-known-gaps.json.
        ConformanceResults::each('sqs', function (array $case): void {
            $body = $this->fixture(['file' => 'fixtures/order-created.json']);
            $decoded = json_decode($body, true, 512, JSON_THROW_ON_ERROR);
            $decoded['attempts'] = (int) $case['body_attempts'];
            $body = json_encode($decoded, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);

            $job = $this->sqsPop($body, null, $case['approximate_receive_count'] ?? null);
            self::assertNotNull($job);

            // Illuminate's SqsJob reads Attributes.ApproximateReceiveCount unguarded, so a message that carries no
            // count crashes instead of falling back to body.attempts: that one crash is this case's conformance
            // failure (reported as an assertion); any other ErrorException is rethrown (see SqsAttempts).
            $attempts = SqsAttempts::read(static fn (): int => $job->attempts(), ! isset($case['approximate_receive_count']));

            self::assertSame((int) $case['expected_attempts'], $attempts, 'attempts');
        }, 'attempts_reconciliation');

        ConformanceResults::each('sqs', function (array $case): void {
            $absent = (bool) ($case['absent'] ?? false);
            $value = $absent ? null : (string) $case['value'];
            $body = $this->fixture(['file' => 'fixtures/order-created.json']);

            $job = $this->sqsPop($body, $value);
            self::assertNotNull($job);

            if ($case['expect'] === 'decode') {
                self::assertSame('urn:babel:orders:created', $job->getUrn(), 'decoded');
            } else {
                self::assertSame(BabelQueueSqsJob::UNSUPPORTED_SCHEMA_URN_PREFIX . $value, $job->getUrn(), 'rejected without decoding');
                self::assertSame($body, $job->getRawBody());
            }
        }, 'schema_version_gate');
    }

    /** §3.4 DelaySeconds on a delayed publish and §3.5 ChangeMessageVisibility on release. */
    public function test_delay_section(): void
    {
        ConformanceResults::each('delay', function (array $case): void {
            $fifo = $case['queue_type'] === 'fifo';
            $queueName = $fifo ? 'orders.fifo' : 'orders';

            $captured = null;
            $sqs = Mockery::mock(SqsClient::class);
            $sqs->shouldReceive('sendMessage')->zeroOrMoreTimes()->with(
                Mockery::on(function (array $args) use (&$captured): bool {
                    $captured = $args;

                    return true;
                }),
            )->andReturn(new Result(['MessageId' => 'sqs-1']));

            $job = new ConformanceProducerJob('urn:babel:orders:created', ['order_id' => 1]);

            if ($case['expect'] === 'reject') {
                try {
                    $this->sqsQueue($sqs)->later((int) $case['delay_seconds'], $job, '', $queueName);
                } catch (BabelQueueException) {
                    self::assertNull($captured, 'a rejected delay must not reach SendMessage');

                    return;
                }

                self::fail('later() must raise BabelQueueException for a rejected delay (broker-bindings.md §3.4)');
            }

            $this->sqsQueue($sqs)->later((int) $case['delay_seconds'], $job, '', $queueName);
            self::assertNotNull($captured, 'SendMessage was called');
            self::assertSame((int) $case['expect']['delay_seconds_attribute'], $captured['DelaySeconds'], 'DelaySeconds');
        });

        ConformanceResults::each('delay', function (array $case): void {
            $body = $this->fixture(['file' => 'fixtures/order-created.json']);

            $sqs = Mockery::mock(SqsClient::class);
            $sqs->shouldReceive('receiveMessage')->once()->andReturn(new Result(['Messages' => [[
                'Body' => $body,
                'ReceiptHandle' => 'rh-1',
                'MessageId' => 'sqs-1',
                'Attributes' => ['ApproximateReceiveCount' => '1'],
            ]]]));
            $visibility = null;
            $sqs->shouldReceive('changeMessageVisibility')->once()->with(
                Mockery::on(function (array $args) use (&$visibility): bool {
                    $visibility = $args;

                    return true;
                }),
            )->andReturnNull();

            $job = $this->sqsQueue($sqs)->pop();
            self::assertInstanceOf(BabelQueueSqsJob::class, $job);
            $job->release((int) $case['delay_seconds']);

            self::assertSame('rh-1', $visibility['ReceiptHandle']);
            self::assertSame((int) $case['expect']['visibility_timeout'], $visibility['VisibilityTimeout'], 'VisibilityTimeout');
        }, 'release_visibility');
    }

    // ------------------------------------------------------------------ artemis

    /**
     * §7.2 / §7.3 projection over STOMP. The `x-opt-jms-type` annotation and `creation-time` cannot
     * be set from STOMP (ADR-0018: routing is body-authoritative on this path), so the runner pins the
     * rest: correlation-id = trace_id and the string-valued `bq_` properties.
     */
    public function test_artemis_property_projection(): void
    {
        ConformanceResults::each('artemis', function (array $case): void {
            $captured = null;
            $stomp = Mockery::mock(StatefulStomp::class);
            $stomp->shouldReceive('send')->once()->with(
                'orders',
                Mockery::on(function (Message $m) use (&$captured): bool {
                    $captured = $m;

                    return true;
                }),
            );

            $fixture = self::parse($this->fixture(['file' => $case['envelope_file']]));
            $this->artemisQueue($stomp)->push(new ConformanceTracedProducerJob((string) $fixture->job, (array) json_decode(json_encode($fixture->data), true), (string) $fixture->trace_id));

            $out = self::parse((string) $captured->body);
            self::assertSame($case['jms_type'], $out->job, 'the URN rides the body `job` (routing key)');
            self::assertSame($case['correlation_id'], $captured['correlation-id'], 'correlation-id = trace_id');
            foreach ((array) $case['properties'] as $name => $value) {
                self::assertSame($value, $captured[$name], $name);
            }
        }, 'property_projection');

        // §7.5: attempts = max(body.attempts, delivery-count), 0-based. The drop-in driver reads the
        // bq_attempts STOMP header (or the body) and adds one, so the cases it cannot satisfy are listed in
        // conformance-known-gaps.json.
        ConformanceResults::each('artemis', function (array $case): void {
            $body = $this->fixture(['file' => 'fixtures/order-created.json']);
            $decoded = json_decode($body, true, 512, JSON_THROW_ON_ERROR);
            $decoded['attempts'] = (int) $case['body_attempts'];
            $body = json_encode($decoded, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);

            $stomp = Mockery::mock(StatefulStomp::class);
            $stomp->shouldReceive('subscribe')->zeroOrMoreTimes();
            $stomp->shouldReceive('read')->once()->andReturn(new Frame('MESSAGE', [
                'message-id' => 'm-1',
                'correlation-id' => 'trace',
                'delivery-count' => (string) $case['delivery_count'],
            ], $body));

            $job = $this->artemisQueue($stomp)->pop('orders');
            self::assertInstanceOf(BabelQueueArtemisJob::class, $job);
            self::assertSame((int) $case['expected_attempts'], $job->attempts(), 'attempts');
        }, 'attempts_reconciliation');

        ConformanceResults::each('artemis', function (array $case): void {
            $value = $case['value'] ?? null;
            $body = $this->fixture(['file' => 'fixtures/order-created.json']);

            $headers = ['message-id' => 'm-1', 'correlation-id' => 'trace'];
            if ($value !== null) {
                $headers['bq_schema_version'] = (string) $value;
            }
            $stomp = Mockery::mock(StatefulStomp::class);
            $stomp->shouldReceive('subscribe')->zeroOrMoreTimes();
            $stomp->shouldReceive('read')->once()->andReturn(new Frame('MESSAGE', $headers, $body));

            $job = $this->artemisQueue($stomp)->pop('orders');
            self::assertInstanceOf(BabelQueueArtemisJob::class, $job);
            if ($case['expect'] === 'decode') {
                self::assertSame('urn:babel:orders:created', $job->getUrn(), 'decoded');
            } else {
                self::assertSame(BabelQueueArtemisJob::UNSUPPORTED_SCHEMA_URN_PREFIX . $value, $job->getUrn(), 'rejected without decoding');
                self::assertSame($body, $job->getRawBody());
            }
        }, 'schema_version_gate');
    }

    // ----------------------------------------------------------------- helpers

    /** @param array<string, mixed> $case */
    private function assertConsumed(array $case, PolyglotMessage $job): void
    {
        $expect = (array) $case['expect'];
        $source = self::parse($this->fixture($case));

        self::assertSame($expect['urn'], $job->getUrn(), 'urn');
        self::assertTrue(self::deepEqual(self::toObject($expect['data']), self::toObject($job->getData())), 'data');
        self::assertSame($source->trace_id, $job->getTraceId(), 'trace_id preserved');
        self::assertSame($expect['lang'], $job->getMeta()['lang'], 'lang');
        self::assertSame($expect['schema_version'], $job->getMeta()['schema_version'], 'schema_version');
        self::assertSame($source->meta->id, $job->getMeta()['id'], 'meta.id');
        self::assertSame($expect['attempts'], $source->attempts, 'fixture attempts');
    }

    private function expectUnknownUrn(PolyglotMessage $job): void
    {
        try {
            (new BabelQueueDispatcher($this->app, ['urn:babel:orders:created' => ConformanceHandler::class]))->dispatch($job);
            self::fail('a message without an identity must be rejected');
        } catch (UnknownUrnException) {
            self::assertTrue(true);
        }
    }

    /** Pop `$body` from RabbitMQ, release it, and return the republished AMQP message. */
    private function releaseAndCapture(string $body): AMQPMessage
    {
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
        self::assertInstanceOf(BabelQueueRabbitJob::class, $job);
        $job->release();

        self::assertCount(1, $published, 'a release republishes exactly once');

        return $published[0];
    }

    /** The body the adapter's DeadLetterPublisher moves to the DLQ, applying the case's reason. */
    private function deadLetterBody(\stdClass $case, string $body): string
    {
        $captured = null;
        $connection = Mockery::mock(QueueContract::class);
        $connection->shouldReceive('pushRaw')->once()->andReturnUsing(
            static function (string $payload, ?string $queue) use (&$captured): string {
                $captured = $payload;

                return 'id';
            },
        );
        $factory = Mockery::mock(QueueFactory::class);
        $factory->shouldReceive('connection')->andReturn($connection);

        $message = $this->rabbitJob($body);
        (new DeadLetterPublisher($factory, ['enabled' => true]))->publish($message, (string) $case->apply_dead_letter->reason, new \RuntimeException('conformance runner'));

        self::assertIsString($captured);

        return $captured;
    }

    private function assertRoundtripBody(\stdClass $case, mixed $out, string $name, bool $deadLettered = false): void
    {
        $violations = [
            ...self::pointerViolations($out, $case->expect_preserved ?? (object) [], $name),
            ...self::absentViolations($out, (array) ($case->expect_absent_after_reencode ?? []), $name),
        ];

        if ($deadLettered && isset($case->expect_dead_letter)) {
            foreach (get_object_vars($case->expect_dead_letter) as $field => $expected) {
                $pointer = '/dead_letter/' . $field;
                if (! self::has($out, $pointer) || ! self::deepEqual($expected, self::pointer($out, $pointer, $name))) {
                    $violations[] = sprintf('%s: %s must be %s', $name, $pointer, json_encode($expected));
                }
            }
        }

        self::assertSame([], $violations, implode("\n", $violations));
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

    /**
     * A Redis queue over a mocked connection; `$onEval` receives every `EVAL` argument list
     * (script first) and returns what Redis would.
     *
     * @param  callable(array<int, mixed>): mixed  $onEval
     */
    private function redisQueue(callable $onEval): BabelQueueRedisQueue
    {
        $connection = Mockery::mock(RedisConnection::class);
        $connection->shouldReceive('eval')->andReturnUsing(static fn (...$args): mixed => $onEval($args));
        // The driver's own scripts go through Connection::command('eval', [script, numKeys, ...args]) on a
        // connection that is not phpredis (this mock); the argument list is the same shape as eval()'s.
        $connection->shouldReceive('command')->with('eval', Mockery::type('array'))
            ->andReturnUsing(static fn (string $method, array $args): mixed => $onEval($args));
        // Laravel 11's laterRaw() writes the delayed zset with ZADD instead of a Lua script; record it in the
        // eval shape (`['zadd', numkeys, key, ...]`) so callers read the key at the same index.
        $connection->shouldReceive('zadd')->andReturnUsing(static fn (...$args): mixed => $onEval(['zadd', 1, ...$args]));
        $factory = Mockery::mock(RedisFactory::class);
        $factory->shouldReceive('connection')->andReturn($connection);

        $queue = new BabelQueueRedisQueue($factory, 'default', null, 60, null);
        $queue->setContainer($this->app);
        $queue->setConnectionName('babelqueue-redis');

        return $queue;
    }

    private function sqsQueue(SqsClient $sqs): BabelQueueSqsQueue
    {
        $queue = new BabelQueueSqsQueue($sqs, 'orders', self::SQS_PREFIX, '', null);
        $queue->setContainer($this->app);
        $queue->setConnectionName('bqsqs');

        return $queue;
    }

    /** Receive `$body` with an optional `bq-schema-version` attribute and ApproximateReceiveCount. */
    private function sqsPop(string $body, ?string $schemaVersionAttribute, ?string $receiveCount = '1'): ?BabelQueueSqsJob
    {
        $message = [
            'Body' => $body,
            'ReceiptHandle' => 'rh-1',
            'MessageId' => 'sqs-1',
            'Attributes' => $receiveCount === null ? [] : ['ApproximateReceiveCount' => $receiveCount],
        ];
        if ($schemaVersionAttribute !== null) {
            $message['MessageAttributes'] = [
                'bq-schema-version' => ['DataType' => 'Number', 'StringValue' => $schemaVersionAttribute],
            ];
        }

        $sqs = Mockery::mock(SqsClient::class);
        $sqs->shouldReceive('receiveMessage')->once()->andReturn(new Result(['Messages' => [$message]]));

        $job = $this->sqsQueue($sqs)->pop();

        return $job instanceof BabelQueueSqsJob ? $job : null;
    }

    private function artemisQueue(StatefulStomp $stomp): BabelQueueArtemisQueue
    {
        $queue = new BabelQueueArtemisQueue(static fn () => $stomp, ['queue' => 'orders']);
        $queue->setContainer($this->app);
        $queue->setConnectionName('babelqueue-artemis');

        return $queue;
    }

    /** Normalise a decoded (assoc) value to the stdClass form used for strict comparison. */
    private static function toObject(mixed $value): mixed
    {
        return json_decode(json_encode($value, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION), false, 512, JSON_THROW_ON_ERROR);
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

/** A producer job that continues a fixed trace id. */
final class ConformanceTracedProducerJob implements ShouldQueuePolyglot, HasTraceId
{
    /** @param array<string, mixed> $payload */
    public function __construct(private string $urn, private array $payload, private string $traceId)
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

    public function getBabelTraceId(): ?string
    {
        return $this->traceId;
    }
}
