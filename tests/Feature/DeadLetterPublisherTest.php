<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Feature;

use BabelQueue\Consumer\DeadLetterPublisher;
use BabelQueue\Contracts\PolyglotMessage;
use BabelQueue\Queue\Concerns\ParsesPolyglotEnvelope;
use BabelQueue\Tests\TestCase;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Jobs\Job;
use Mockery;
use RuntimeException;

/**
 * Covers the cross-language dead-letter queue: the original envelope is preserved
 * and an additive "dead_letter" block is attached, so any SDK can triage it.
 */
final class DeadLetterPublisherTest extends TestCase
{
    public function test_disabled_dlq_is_a_noop(): void
    {
        $factory = Mockery::mock(QueueFactory::class);
        $factory->shouldNotReceive('connection');

        $dlq = new DeadLetterPublisher($factory, ['enabled' => false]);
        $dlq->publish($this->message('orders', 'bq-redis', '{"job":"urn:babel:orders:created","trace_id":"t1","data":{},"meta":{"id":"m1"},"attempts":3}'), 'failed');

        $this->assertFalse($dlq->enabled());
    }

    public function test_enabled_dlq_publishes_the_annotated_envelope(): void
    {
        $captured = null;
        $queue = Mockery::mock(QueueContract::class);
        $queue->shouldReceive('pushRaw')->once()->withArgs(function ($payload, $q) use (&$captured): bool {
            $captured = json_decode($payload, true);

            return $q === 'orders.dlq';
        })->andReturn('x');

        $factory = Mockery::mock(QueueFactory::class);
        $factory->shouldReceive('connection')->once()->with('bq-redis')->andReturn($queue);

        $dlq = new DeadLetterPublisher($factory, ['enabled' => true, 'suffix' => '.dlq']);
        $dlq->publish(
            $this->message('orders', 'bq-redis', '{"job":"urn:babel:orders:created","trace_id":"t1","data":{"order_id":7},"meta":{"id":"m1"},"attempts":3}'),
            'failed',
            new RuntimeException('boom'),
        );

        // Original envelope preserved verbatim …
        $this->assertSame('urn:babel:orders:created', $captured['job']);
        $this->assertSame('t1', $captured['trace_id']);
        $this->assertSame('m1', $captured['meta']['id']);
        // … plus the additive dead_letter block.
        $this->assertSame('failed', $captured['dead_letter']['reason']);
        $this->assertSame('orders', $captured['dead_letter']['original_queue']);
        $this->assertSame('boom', $captured['dead_letter']['error']);
        $this->assertSame(RuntimeException::class, $captured['dead_letter']['exception']);
        $this->assertSame('php', $captured['dead_letter']['lang']);
        $this->assertIsInt($captured['dead_letter']['failed_at']);
    }

    public function test_explicit_dlq_queue_name_overrides_the_suffix(): void
    {
        $queue = Mockery::mock(QueueContract::class);
        $queue->shouldReceive('pushRaw')->once()->with(Mockery::type('string'), 'failures')->andReturn('x');

        $factory = Mockery::mock(QueueFactory::class);
        $factory->shouldReceive('connection')->once()->with('bq-redis')->andReturn($queue);

        $dlq = new DeadLetterPublisher($factory, ['enabled' => true, 'queue' => 'failures']);
        $dlq->publish($this->message('orders', 'bq-redis', '{"job":"u","data":{},"meta":{},"attempts":1}'), 'failed');
    }

    private function message(string $queue, string $connection, string $body): PolyglotMessage
    {
        return new class($queue, $connection, $body, $this->app) extends Job implements PolyglotMessage {
            use ParsesPolyglotEnvelope;

            public function __construct(string $queue, string $connection, private string $body, Container $container)
            {
                $this->queue = $queue;
                $this->connectionName = $connection;
                $this->container = $container;
            }

            public function getRawBody(): string
            {
                return $this->body;
            }

            public function getJobId()
            {
                return $this->getMeta()['id'] ?? null;
            }

            public function attempts()
            {
                return 3;
            }
        };
    }
}
