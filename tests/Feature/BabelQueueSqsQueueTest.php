<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Feature;

use Aws\Result;
use Aws\Sqs\SqsClient;
use BabelQueue\Consumer\BabelQueueDispatcher;
use BabelQueue\Consumer\DeadLetterPublisher;
use BabelQueue\Contracts\ShouldQueuePolyglot;
use BabelQueue\Exceptions\InvalidEnvelopeException;
use BabelQueue\Queue\BabelQueueSqsQueue;
use BabelQueue\Queue\Jobs\BabelQueueSqsJob;
use BabelQueue\Tests\TestCase;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Mockery;
use PHPUnit\Framework\Attributes\DataProvider;

/**
 * The SQS driver against a Mockery-mocked AWS client (no real AWS): the produce
 * side emits the canonical envelope + the §3 MessageAttributes projection, and the
 * consume side re-wraps the reserved SqsJob as a URN-routed polyglot job.
 */
final class BabelQueueSqsQueueTest extends TestCase
{
    private const PREFIX = 'https://sqs.eu-central-1.amazonaws.com/123456789012';

    private function queue(SqsClient $sqs): BabelQueueSqsQueue
    {
        $queue = new BabelQueueSqsQueue($sqs, 'orders', self::PREFIX, '', null);
        $queue->setContainer($this->app);
        $queue->setConnectionName('bqsqs');

        return $queue;
    }

    public function test_push_polyglot_emits_canonical_envelope_with_contract_attributes(): void
    {
        $captured = null;

        $sqs = Mockery::mock(SqsClient::class);
        $sqs->shouldReceive('sendMessage')->once()->with(
            Mockery::on(function (array $args) use (&$captured): bool {
                $captured = $args;

                return true;
            }),
        )->andReturn(new Result(['MessageId' => 'sqs-1']));

        $id = $this->queue($sqs)->push(new SqsOrderJob());

        $this->assertNotSame('', $id);
        $this->assertSame(self::PREFIX.'/orders', $captured['QueueUrl']);

        $env = json_decode($captured['MessageBody'], true);
        $this->assertSame('urn:babel:orders:created', $env['job']);
        $this->assertSame($id, $env['meta']['id']);
        $this->assertSame(['order_id' => 7], $env['data']);

        $attrs = $captured['MessageAttributes'];
        $this->assertSame(['DataType' => 'String', 'StringValue' => 'urn:babel:orders:created'], $attrs['bq-job']);
        $this->assertSame(['DataType' => 'Number', 'StringValue' => '1'], $attrs['bq-schema-version']);
        $this->assertSame('php', $attrs['bq-source-lang']['StringValue']);
        $this->assertSame($id, $attrs['bq-message-id']['StringValue']);
        $this->assertNotSame('', $attrs['bq-trace-id']['StringValue']);
    }

    public function test_pop_wraps_the_reserved_job_as_a_polyglot_consume_job(): void
    {
        $env = '{"job":"urn:babel:orders:created","trace_id":"trace-9","data":{"order_id":7},'
            .'"meta":{"id":"msg-9","queue":"orders","lang":"php","schema_version":1,'
            .'"created_at":1749132727000},"attempts":0}';

        $sqs = Mockery::mock(SqsClient::class);
        $sqs->shouldReceive('receiveMessage')->once()->andReturn(new Result(['Messages' => [[
            'Body' => $env,
            'ReceiptHandle' => 'rh-1',
            'MessageId' => 'sqs-1',
            'Attributes' => ['ApproximateReceiveCount' => '1'],
        ]]]));

        $job = $this->queue($sqs)->pop();

        $this->assertInstanceOf(BabelQueueSqsJob::class, $job);
        $this->assertSame('urn:babel:orders:created', $job->getUrn());
        $this->assertSame('trace-9', $job->getTraceId());
        $this->assertSame(['order_id' => 7], $job->getData());
        $this->assertSame('msg-9', $job->getJobId()); // prefers the polyglot meta.id
    }

    private const VALID_BODY = '{"job":"urn:babel:orders:created","trace_id":"trace-9","data":{"order_id":7},'
        .'"meta":{"id":"msg-9","queue":"orders","lang":"php","schema_version":1,'
        .'"created_at":1749132727000},"attempts":0}';

    /**
     * Pop one reserved message carrying the given `bq-schema-version` attribute (null = no attributes),
     * capturing the receiveMessage arguments.
     *
     * @param  array<string, mixed>|null  $args
     */
    private function popOne(string $body, ?string $schemaVersionAttribute, ?array &$args = null): ?BabelQueueSqsJob
    {
        $message = [
            'Body' => $body,
            'ReceiptHandle' => 'rh-1',
            'MessageId' => 'sqs-1',
            'Attributes' => ['ApproximateReceiveCount' => '1'],
        ];

        if ($schemaVersionAttribute !== null) {
            $message['MessageAttributes'] = [
                'bq-schema-version' => ['DataType' => 'Number', 'StringValue' => $schemaVersionAttribute],
            ];
        }

        $sqs = Mockery::mock(SqsClient::class);
        $sqs->shouldReceive('receiveMessage')->once()->with(
            Mockery::on(function (array $a) use (&$args): bool {
                $args = $a;

                return true;
            }),
        )->andReturn(new Result(['Messages' => [$message]]));

        $job = $this->queue($sqs)->pop();

        return $job instanceof BabelQueueSqsJob ? $job : null;
    }

    public function test_pop_requests_all_message_attributes(): void
    {
        $args = null;
        $this->popOne(self::VALID_BODY, '1', $args);

        $this->assertSame(['All'], $args['MessageAttributeNames']);
        $this->assertSame(['ApproximateReceiveCount'], $args['AttributeNames']);
        $this->assertSame(self::PREFIX.'/orders', $args['QueueUrl']);
    }

    /**
     * @return array<string, array{?string}>
     */
    public static function decodableVersionAttributes(): array
    {
        return [
            'attribute absent' => [null],
            'empty' => [''],
            'spaces' => ['   '],
            'ascii whitespace' => [" \t\n\x0B\f\r"],
            'supported version' => ['1'],
        ];
    }

    #[DataProvider('decodableVersionAttributes')]
    public function test_a_missing_blank_or_supported_schema_version_attribute_decodes_normally(?string $attribute): void
    {
        $job = $this->popOne(self::VALID_BODY, $attribute);

        $this->assertInstanceOf(BabelQueueSqsJob::class, $job);
        $this->assertSame('urn:babel:orders:created', $job->getUrn());
        $this->assertSame(['order_id' => 7], $job->getData());
        $this->assertSame(1, $job->getMeta()['schema_version']);
    }

    /**
     * @return array<string, array{string}>
     */
    public static function unsupportedVersionAttributes(): array
    {
        return [
            'two' => ['2'],
            'garbage' => ['x'],
            'untrimmed one' => [' 1'],
            'trailing space' => ['1 '],
            'leading zero' => ['01'],
            'nbsp is not blank' => ["\u{00A0}"],
            'ideographic space is not blank' => ["\u{3000}"],
            'file separator is not blank' => ["\x1C"],
        ];
    }

    #[DataProvider('unsupportedVersionAttributes')]
    public function test_an_unsupported_schema_version_attribute_is_rejected_without_decoding_the_body(string $attribute): void
    {
        // A body that would fail to decode (and carries a supported meta.schema_version if it did):
        // the gate must not read it at all.
        $job = $this->popOne('this is not json', $attribute);

        $this->assertInstanceOf(BabelQueueSqsJob::class, $job);
        $this->assertSame(BabelQueueSqsJob::UNSUPPORTED_SCHEMA_URN_PREFIX.$attribute, $job->getUrn());
        $this->assertSame('this is not json', $job->getRawBody());
        $this->assertSame($attribute, $job->getMeta()['schema_version']);
    }

    /**
     * §3.7 consumer-side schema-version gate: the vendored manifest's `sqs.schema_version_gate` cases
     * (GR-5 parity with the other SDKs). The body is always the valid fixture envelope, so a `reject`
     * verdict can only come from the attribute gate.
     */
    public function test_schema_version_gate_matches_the_golden_cases(): void
    {
        $manifest = json_decode(
            (string) file_get_contents(__DIR__.'/../conformance/manifest.json'),
            true,
            512,
            JSON_THROW_ON_ERROR,
        );
        $gate = $manifest['sqs']['schema_version_gate'];
        $body = (string) file_get_contents(__DIR__.'/../conformance/'.$gate['fixture']);
        $fixtureUrn = (string) json_decode($body, true, 512, JSON_THROW_ON_ERROR)['job'];

        foreach ($gate['cases'] as $case) {
            $absent = ($case['absent'] ?? false) === true;
            $label = $absent ? 'absent' : bin2hex((string) $case['value']);

            $job = $this->popOne($body, $absent ? null : (string) $case['value']);

            $this->assertNotNull($job, $label);
            $this->assertSame($body, $job->getRawBody(), $label);

            if ($case['expect'] === 'decode') {
                $this->assertSame($fixtureUrn, $job->getUrn(), $label);
            } else {
                $this->assertSame(BabelQueueSqsJob::UNSUPPORTED_SCHEMA_URN_PREFIX.$case['value'], $job->getUrn(), $label);
            }
        }
    }

    public function test_a_gated_message_with_a_valid_body_is_still_rejected_and_never_handled(): void
    {
        $handled = false;
        $this->app->bind('SqsGateProbe', function () use (&$handled) {
            return new class($handled) {
                public function __construct(private bool &$handled)
                {
                }

                public function handle(): void
                {
                    $this->handled = true;
                }
            };
        });

        // The body itself claims schema_version 1; only the attribute says otherwise.
        $job = $this->popOne(self::VALID_BODY, '2');
        $this->assertNotNull($job);

        $dispatcher = new BabelQueueDispatcher($this->app, ['urn:babel:orders:created' => 'SqsGateProbe']);

        try {
            $dispatcher->dispatch($job);
            $this->fail('a gated message must be rejected');
        } catch (InvalidEnvelopeException) {
            // expected: no DLQ configured, so the worker's retry / failed_jobs path records it
        }

        $this->assertFalse($handled);
    }

    public function test_a_gated_message_with_a_non_json_body_is_dead_lettered_as_poison_and_deleted(): void
    {
        $captured = null;
        $dlqQueue = Mockery::mock(QueueContract::class);
        $dlqQueue->shouldReceive('pushRaw')->once()->withArgs(function ($payload) use (&$captured): bool {
            $captured = json_decode($payload, true);

            return true;
        })->andReturn('x');
        $factory = Mockery::mock(QueueFactory::class);
        $factory->shouldReceive('connection')->once()->andReturn($dlqQueue);

        $sqs = Mockery::mock(SqsClient::class);
        $sqs->shouldReceive('receiveMessage')->once()->andReturn(new Result(['Messages' => [[
            'Body' => 'not json at all',
            'ReceiptHandle' => 'rh-1',
            'MessageId' => 'sqs-1',
            'Attributes' => ['ApproximateReceiveCount' => '1'],
            'MessageAttributes' => ['bq-schema-version' => ['DataType' => 'Number', 'StringValue' => '2']],
        ]]]));
        $sqs->shouldReceive('deleteMessage')->once();

        $job = $this->queue($sqs)->pop();
        $this->assertInstanceOf(BabelQueueSqsJob::class, $job);

        (new BabelQueueDispatcher($this->app, [], 'fail', 0, new DeadLetterPublisher($factory, ['enabled' => true])))
            ->dispatch($job);

        $this->assertSame('poison', $captured['dead_letter']['reason']);
        $this->assertSame('not json at all', $captured['raw']);
        $this->assertArrayNotHasKey('job', $captured);
        $this->assertTrue($job->isDeleted());
    }

    public function test_pop_returns_null_on_an_empty_queue(): void
    {
        $sqs = Mockery::mock(SqsClient::class);
        $sqs->shouldReceive('receiveMessage')->once()->andReturn(new Result(['Messages' => null]));

        $this->assertNull($this->queue($sqs)->pop());
    }

    public function test_standard_laravel_payload_gets_no_message_attributes(): void
    {
        $captured = null;

        $sqs = Mockery::mock(SqsClient::class);
        $sqs->shouldReceive('sendMessage')->once()->with(
            Mockery::on(function (array $args) use (&$captured): bool {
                $captured = $args;

                return true;
            }),
        )->andReturn(new Result(['MessageId' => 'x']));

        // A stock Laravel job payload has job = "Class@method" and no meta.schema_version.
        $standard = '{"uuid":"u","displayName":"App\\\\Jobs\\\\Foo",'
            .'"job":"Illuminate\\\\Queue\\\\CallQueuedHandler@call","data":{"x":1}}';
        $this->queue($sqs)->pushRaw($standard);

        $this->assertArrayNotHasKey('MessageAttributes', $captured);
    }
}

final class SqsOrderJob implements ShouldQueuePolyglot
{
    public function getBabelUrn(): string
    {
        return 'urn:babel:orders:created';
    }

    /** @return array<string, mixed> */
    public function toPayload(): array
    {
        return ['order_id' => 7];
    }
}
