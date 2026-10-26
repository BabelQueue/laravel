<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Feature;

use BabelQueue\Consumer\DeadLetterPublisher;
use BabelQueue\Contracts\PolyglotMessage;
use BabelQueue\Queue\BabelQueueSqsQueue;
use BabelQueue\Queue\Concerns\ParsesPolyglotEnvelope;
use BabelQueue\Tests\TestCase;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Jobs\Job;
use Illuminate\Queue\SqsQueue;
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

    public function test_a_poison_body_is_dead_lettered_as_the_original_object_or_as_raw(): void
    {
        $cases = [
            // a list "data" is rejected by decode(): the original object is kept verbatim, not reshaped
            'list data' => ['{"job":"urn:a","timestamp":1,"data":[1,2]}', ['job' => 'urn:a', 'timestamp' => 1, 'data' => [1, 2]]],
            'not json' => ['not json', ['raw' => 'not json']],
        ];

        foreach ($cases as $name => [$body, $expected]) {
            $captured = null;
            $queue = Mockery::mock(QueueContract::class);
            $queue->shouldReceive('pushRaw')->once()->withArgs(function ($payload) use (&$captured): bool {
                $captured = json_decode($payload, true);

                return true;
            })->andReturn('x');
            $factory = Mockery::mock(QueueFactory::class);
            $factory->shouldReceive('connection')->once()->andReturn($queue);

            (new DeadLetterPublisher($factory, ['enabled' => true]))->publish($this->message('orders', 'bq-redis', $body), 'poison');

            $this->assertSame('poison', $captured['dead_letter']['reason'], $name);
            unset($captured['dead_letter']);
            $this->assertSame($expected, $captured, $name);
        }
    }

    public function test_a_poison_body_is_published_byte_exact_and_never_throws(): void
    {
        $cases = [
            'empty object stays an object' => ['{}', '{"dead_letter":{', null],
            'big integer stays exact' => ['{"job":"urn:a","data":{"n":12345678901234567890}}', '12345678901234567890}', null],
            'overflowing number is carried verbatim' => ['{"job":"urn:a","attempts":0,"data":{"x":1e400}}', '{"x":1e400}', null],
            'invalid utf-8 is carried as base64' => ["\xff\xfe\x00bad", '"raw":"' . base64_encode("\xff\xfe\x00bad") . '"', 'base64'],
            'invalid utf-8 inside an object' => ["{\"job\":\"\xff\"}", '"raw_encoding":"base64"', 'base64'],
        ];

        foreach ($cases as $name => [$body, $needle, $encoding]) {
            $payload = null;
            $queue = Mockery::mock(QueueContract::class);
            $queue->shouldReceive('pushRaw')->once()->withArgs(function ($p) use (&$payload): bool {
                $payload = $p;

                return true;
            })->andReturn('x');
            $factory = Mockery::mock(QueueFactory::class);
            $factory->shouldReceive('connection')->once()->andReturn($queue);

            (new DeadLetterPublisher($factory, ['enabled' => true]))->publish($this->message('orders', 'bq-redis', $body), 'poison');

            $this->assertIsString($payload, $name);
            $this->assertStringContainsString($needle, $payload, $name);
            $decoded = json_decode($payload);
            $this->assertNotNull($decoded, $name . ': the DLQ copy must be valid JSON');
            $this->assertSame('poison', $decoded->dead_letter->reason, $name);
            $this->assertSame($encoding, $decoded->raw_encoding ?? null, $name);
        }
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

    private const BODY = '{"job":"urn:babel:orders:created","trace_id":"t1","data":{},"meta":{"id":"m1"},"attempts":1}';

    /**
     * Publish `$body` through a publisher whose destination connection is `$destination`, returning the DLQ
     * queue name that `pushRaw()` received.
     *
     * @param  class-string<QueueContract>  $destination
     * @param  array<string, mixed>  $config
     */
    private function dlqTarget(string $destination, string $originalQueue, array $config = []): string
    {
        $target = null;
        $queue = Mockery::mock($destination);
        $queue->shouldReceive('pushRaw')->once()->withArgs(function ($payload, $q) use (&$target): bool {
            $target = $q;

            return true;
        })->andReturn('x');

        $factory = Mockery::mock(QueueFactory::class);
        $factory->shouldReceive('connection')->once()->andReturn($queue);

        (new DeadLetterPublisher($factory, ['enabled' => true] + $config))
            ->publish($this->message($originalQueue, 'bq-conn', self::BODY), 'failed');

        return (string) $target;
    }

    public function test_the_sqs_default_dlq_name_is_queue_dash_dlq(): void
    {
        $this->assertSame('orders-dlq', $this->dlqTarget(BabelQueueSqsQueue::class, 'orders'));
        $this->assertSame('orders-dlq', $this->dlqTarget(BabelQueueSqsQueue::class, 'orders', ['suffix' => null]));
    }

    public function test_the_sqs_default_dlq_name_of_a_fifo_queue_keeps_fifo_last(): void
    {
        $this->assertSame('orders-dlq.fifo', $this->dlqTarget(BabelQueueSqsQueue::class, 'orders.fifo'));
    }

    public function test_the_sqs_default_applies_to_the_last_segment_of_a_queue_url(): void
    {
        $this->assertSame(
            'https://sqs.eu-west-1.amazonaws.com/123456789012/orders-dlq',
            $this->dlqTarget(BabelQueueSqsQueue::class, 'https://sqs.eu-west-1.amazonaws.com/123456789012/orders'),
        );
        $this->assertSame(
            'https://sqs.eu-west-1.amazonaws.com/123456789012/orders-dlq.fifo',
            $this->dlqTarget(BabelQueueSqsQueue::class, 'https://sqs.eu-west-1.amazonaws.com/123456789012/orders.fifo'),
        );
    }

    public function test_the_stock_laravel_sqs_driver_gets_the_sqs_default_too(): void
    {
        $this->assertSame('orders-dlq', $this->dlqTarget(SqsQueue::class, 'orders'));
        $this->assertSame('orders-dlq.fifo', $this->dlqTarget(SqsQueue::class, 'orders.fifo'));
    }

    public function test_an_explicit_suffix_on_a_fifo_queue_goes_before_dot_fifo(): void
    {
        // `orders.fifo` + `-dlq` as written would be `orders.fifo-dlq`, a name SQS cannot create.
        $this->assertSame('orders-dlq.fifo', $this->dlqTarget(BabelQueueSqsQueue::class, 'orders.fifo', ['suffix' => '-dlq']));
        $this->assertSame('orders_dead.fifo', $this->dlqTarget(BabelQueueSqsQueue::class, 'orders.fifo', ['suffix' => '_dead']));
    }

    public function test_an_explicit_suffix_or_queue_name_wins_on_sqs(): void
    {
        $this->assertSame('orders.dlq', $this->dlqTarget(BabelQueueSqsQueue::class, 'orders', ['suffix' => '.dlq']));
        $this->assertSame('orders_dead', $this->dlqTarget(BabelQueueSqsQueue::class, 'orders', ['suffix' => '_dead']));
        $this->assertSame('orders_dead.fifo', $this->dlqTarget(BabelQueueSqsQueue::class, 'orders.fifo', ['suffix' => '_dead']));
        $this->assertSame('failures', $this->dlqTarget(BabelQueueSqsQueue::class, 'orders', ['queue' => 'failures']));
        $this->assertSame('failures.fifo', $this->dlqTarget(BabelQueueSqsQueue::class, 'orders.fifo', ['queue' => 'failures.fifo']));
    }

    public function test_every_other_driver_keeps_the_dot_dlq_default(): void
    {
        $this->assertSame('orders.dlq', $this->dlqTarget(QueueContract::class, 'orders'));
        $this->assertSame('orders.dlq', $this->dlqTarget(QueueContract::class, 'orders', ['suffix' => null]));
        $this->assertSame('orders.fifo.dlq', $this->dlqTarget(QueueContract::class, 'orders.fifo'));
    }

    public function test_the_default_follows_the_destination_connection_not_the_source(): void
    {
        // A message from a Redis connection dead-lettered onto an SQS connection gets the SQS name.
        $this->assertSame('orders-dlq', $this->dlqTarget(BabelQueueSqsQueue::class, 'orders', ['connection' => 'bq-sqs']));
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
