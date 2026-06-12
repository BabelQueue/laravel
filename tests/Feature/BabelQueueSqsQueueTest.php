<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Feature;

use Aws\Result;
use Aws\Sqs\SqsClient;
use BabelQueue\Contracts\ShouldQueuePolyglot;
use BabelQueue\Queue\BabelQueueSqsQueue;
use BabelQueue\Queue\Jobs\BabelQueueSqsJob;
use BabelQueue\Tests\TestCase;
use Mockery;

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
