<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Feature;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\ShouldQueuePolyglot;
use BabelQueue\Queue\BabelQueueArtemisQueue;
use BabelQueue\Queue\Connectors\BabelQueueArtemisConnector;
use BabelQueue\Queue\Jobs\BabelQueueArtemisJob;
use BabelQueue\Tests\TestCase;
use Mockery;
use Stomp\StatefulStomp;
use Stomp\Transport\Frame;
use Stomp\Transport\Message;

/**
 * The Artemis (STOMP) driver, exercised against a mocked STOMP client (no broker): the produce
 * path (canonical envelope + §7 STOMP headers), the consume path (subscribe → read → job →
 * ack/release), the Artemis scheduled delay and the destination prefix.
 */
final class BabelQueueArtemisQueueTest extends TestCase
{
    /** @param array<string,mixed> $config */
    private function makeQueue(StatefulStomp $stomp, array $config = []): BabelQueueArtemisQueue
    {
        $queue = new BabelQueueArtemisQueue(static fn () => $stomp, array_merge(['queue' => 'orders'], $config));
        $queue->setContainer($this->app);
        $queue->setConnectionName('babelqueue-artemis');

        return $queue;
    }

    public function test_push_sends_the_canonical_envelope_with_contract_stomp_headers(): void
    {
        $captured = null;
        $stomp = Mockery::mock(StatefulStomp::class);
        $stomp->shouldReceive('send')->once()->with(
            'orders',
            Mockery::on(function (Message $m) use (&$captured): bool {
                $captured = $m;

                return true;
            }),
        );

        $id = $this->makeQueue($stomp)->push(new ArtemisOrderJob());

        $this->assertNotSame('', $id);
        $env = json_decode((string) $captured->body, true);
        $this->assertSame('urn:babel:orders:created', $env['job']);
        $this->assertSame(['order_id' => 7], $env['data']);
        $this->assertSame($id, $env['meta']['id']);

        $this->assertSame('application/json', $captured['content-type']);
        $this->assertSame('babelqueue', $captured['bq_app_id']);
        $this->assertSame($env['trace_id'], $captured['correlation-id']);
        $this->assertSame('1', $captured['bq_schema_version']);
        $this->assertSame('php', $captured['bq_source_lang']);
        $this->assertSame('0', $captured['bq_attempts']);
    }

    public function test_push_raw_sends_and_returns_the_payload_id(): void
    {
        $stomp = Mockery::mock(StatefulStomp::class);
        $stomp->shouldReceive('send')->once();

        $payload = EnvelopeCodec::encode(EnvelopeCodec::fromJob(new ArtemisOrderJob(), 'orders'));
        $id = $this->makeQueue($stomp)->pushRaw($payload, 'orders');

        $this->assertSame(json_decode($payload, true)['meta']['id'], $id);
    }

    public function test_push_of_a_standard_job_falls_back_to_raw_send(): void
    {
        $stomp = Mockery::mock(StatefulStomp::class);
        $stomp->shouldReceive('send')->once();

        $this->makeQueue($stomp)->push(new ArtemisPlainJob());
        $this->addToAssertionCount(1);
    }

    public function test_later_sets_the_artemis_scheduled_delay_header(): void
    {
        $captured = null;
        $stomp = Mockery::mock(StatefulStomp::class);
        $stomp->shouldReceive('send')->once()->with(
            'orders',
            Mockery::on(function (Message $m) use (&$captured): bool {
                $captured = $m;

                return true;
            }),
        );

        $this->makeQueue($stomp)->later(5, new ArtemisOrderJob());

        $this->assertSame('5000', $captured['AMQ_SCHEDULED_DELAY']);
    }

    public function test_pop_with_no_frame_returns_null(): void
    {
        $stomp = Mockery::mock(StatefulStomp::class);
        $stomp->shouldReceive('subscribe')->once()->with('orders', null, 'client-individual');
        $stomp->shouldReceive('read')->once()->andReturn(false);

        $this->assertNull($this->makeQueue($stomp)->pop('orders'));
    }

    public function test_pop_wraps_a_frame_and_delete_acks_it(): void
    {
        $body = EnvelopeCodec::encode(EnvelopeCodec::fromJob(new ArtemisOrderJob(), 'orders'));
        $frame = new Frame('MESSAGE', ['message-id' => 'stomp-1'], $body);

        $stomp = Mockery::mock(StatefulStomp::class);
        $stomp->shouldReceive('subscribe')->once()->with('orders', null, 'client-individual');
        $stomp->shouldReceive('read')->once()->andReturn($frame);
        $stomp->shouldReceive('ack')->once()->with($frame);

        $job = $this->makeQueue($stomp)->pop('orders');

        $this->assertInstanceOf(BabelQueueArtemisJob::class, $job);
        $this->assertSame($body, $job->getRawBody());
        $this->assertSame(json_decode($body, true)['meta']['id'], $job->getJobId()); // meta.id wins over message-id
        $this->assertSame(1, $job->attempts()); // no prior bq_attempts header → 1
        $job->delete();
    }

    public function test_release_republishes_with_incremented_attempts_then_acks(): void
    {
        $body = EnvelopeCodec::encode(EnvelopeCodec::fromJob(new ArtemisOrderJob(), 'orders'));
        $frame = new Frame('MESSAGE', ['correlation-id' => 'trace-1', 'bq_attempts' => '2'], $body);

        $captured = null;
        $stomp = Mockery::mock(StatefulStomp::class);
        $stomp->shouldReceive('subscribe')->once();
        $stomp->shouldReceive('read')->once()->andReturn($frame);
        $stomp->shouldReceive('send')->once()->with(
            'orders',
            Mockery::on(function (Message $m) use (&$captured): bool {
                $captured = $m;

                return true;
            }),
        );
        $stomp->shouldReceive('ack')->once()->with($frame);

        $job = $this->makeQueue($stomp)->pop('orders');
        $this->assertSame(3, $job->attempts()); // prior 2 + this delivery
        $job->release();

        $this->assertSame('3', $captured['bq_attempts']);
        $this->assertSame('trace-1', $captured['correlation-id']); // trace id preserved across retry
    }

    public function test_destination_prefix_is_applied(): void
    {
        $stomp = Mockery::mock(StatefulStomp::class);
        $stomp->shouldReceive('send')->once()->with('/queue/orders', Mockery::any());

        $this->makeQueue($stomp, ['destination_prefix' => '/queue/'])->push(new ArtemisOrderJob());
        $this->addToAssertionCount(1);
    }

    public function test_size_is_zero_stomp_has_no_depth_primitive(): void
    {
        $stomp = Mockery::mock(StatefulStomp::class);

        $this->assertSame(0, $this->makeQueue($stomp)->size('orders'));
    }

    public function test_connector_builds_an_artemis_queue_without_opening_a_socket(): void
    {
        $queue = (new BabelQueueArtemisConnector())->connect(['queue' => 'orders', 'host' => '127.0.0.1']);

        $this->assertInstanceOf(BabelQueueArtemisQueue::class, $queue);
        $this->assertSame('orders', $queue->getQueue(null));
    }
}

final class ArtemisOrderJob implements ShouldQueuePolyglot
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

final class ArtemisPlainJob
{
    public function handle(): void
    {
    }
}
