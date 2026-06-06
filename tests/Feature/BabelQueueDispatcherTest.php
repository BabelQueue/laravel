<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Feature;

use BabelQueue\Consumer\BabelQueueDispatcher;
use BabelQueue\Consumer\DeadLetterPublisher;
use BabelQueue\Contracts\PolyglotMessage;
use BabelQueue\Exceptions\UnknownUrnException;
use BabelQueue\Queue\Concerns\ParsesPolyglotEnvelope;
use BabelQueue\Tests\TestCase;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Jobs\Job;
use Mockery;
use RuntimeException;

final class BabelQueueDispatcherTest extends TestCase
{
    public function test_dispatch_routes_urn_to_handler_and_acks(): void
    {
        OrderConsumerStub::$received = [];

        $dispatcher = new BabelQueueDispatcher($this->app, [
            'urn:babel:orders:process' => OrderConsumerStub::class,
        ]);

        $message = $this->message('{"job":"urn:babel:orders:process","trace_id":"7b3f9c2a-e41d-4f88","data":{"order_id":7},"meta":{"id":"m1"}}');

        $dispatcher->dispatch($message);

        $this->assertSame(['order_id' => 7], OrderConsumerStub::$received['data']);
        $this->assertSame(['id' => 'm1'], OrderConsumerStub::$received['meta']);
        $this->assertSame('7b3f9c2a-e41d-4f88', OrderConsumerStub::$received['traceId']);
        $this->assertTrue($message->isDeleted(), 'message should be acked on success');
    }

    public function test_missing_trace_id_is_passed_as_empty_string(): void
    {
        OrderConsumerStub::$received = [];

        $dispatcher = new BabelQueueDispatcher($this->app, [
            'urn:babel:orders:process' => OrderConsumerStub::class,
        ]);

        // A legacy/non-conformant envelope with no trace_id must not blow up.
        $dispatcher->dispatch($this->message('{"job":"urn:babel:orders:process","data":{},"meta":{}}'));

        $this->assertSame('', OrderConsumerStub::$received['traceId']);
    }

    public function test_unknown_urn_fails_by_default(): void
    {
        $this->expectException(UnknownUrnException::class);

        (new BabelQueueDispatcher($this->app, []))
            ->dispatch($this->message('{"job":"urn:unknown","data":{},"meta":{}}'));
    }

    public function test_unknown_urn_can_be_dropped(): void
    {
        $message = $this->message('{"job":"urn:unknown","data":{},"meta":{}}');

        (new BabelQueueDispatcher($this->app, [], 'delete'))->dispatch($message);

        $this->assertTrue($message->isDeleted());
    }

    public function test_permanent_failure_forwards_to_handler(): void
    {
        OrderConsumerStub::$failed = false;

        (new BabelQueueDispatcher($this->app, ['urn:babel:orders:process' => OrderConsumerStub::class]))
            ->fail($this->message('{"job":"urn:babel:orders:process","data":{},"meta":{}}'), new RuntimeException('boom'));

        $this->assertTrue(OrderConsumerStub::$failed);
    }

    public function test_permanent_failure_routes_to_dead_letter_queue(): void
    {
        $captured = null;
        $dlq = $this->deadLetterPublisher($captured);

        (new BabelQueueDispatcher($this->app, [], 'fail', 0, $dlq))
            ->fail($this->message('{"job":"urn:babel:orders:created","trace_id":"t1","data":{},"meta":{}}'), new RuntimeException('boom'));

        $this->assertSame('failed', $captured['dead_letter']['reason']);
        $this->assertSame('t1', $captured['trace_id']);
        $this->assertSame('boom', $captured['dead_letter']['error']);
    }

    public function test_unknown_urn_dead_letter_strategy_quarantines_and_acks(): void
    {
        $captured = null;
        $dlq = $this->deadLetterPublisher($captured);

        $message = $this->message('{"job":"urn:unknown","data":{},"meta":{}}');

        (new BabelQueueDispatcher($this->app, [], 'dead_letter', 0, $dlq))->dispatch($message);

        $this->assertSame('unknown_urn', $captured['dead_letter']['reason']);
        $this->assertTrue($message->isDeleted());
    }

    /**
     * A real (final) DeadLetterPublisher backed by a mocked queue; $captured
     * receives the decoded payload that would be published to the DLQ.
     *
     * @param  array<string, mixed>|null  $captured
     */
    private function deadLetterPublisher(&$captured): DeadLetterPublisher
    {
        $queue = Mockery::mock(QueueContract::class);
        $queue->shouldReceive('pushRaw')->once()->withArgs(function ($payload) use (&$captured): bool {
            $captured = json_decode($payload, true);

            return true;
        })->andReturn('x');

        $factory = Mockery::mock(QueueFactory::class);
        $factory->shouldReceive('connection')->once()->andReturn($queue);

        return new DeadLetterPublisher($factory, ['enabled' => true]);
    }

    private function message(string $body): PolyglotMessage
    {
        return new class($body, $this->app) extends Job implements PolyglotMessage {
            use ParsesPolyglotEnvelope;

            public function __construct(private string $body, Container $container)
            {
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
                return 1;
            }
        };
    }
}

class OrderConsumerStub
{
    /** @var array<string, mixed> */
    public static array $received = [];

    public static bool $failed = false;

    /**
     * @param  array<string, mixed>  $data
     * @param  array<string, mixed>  $meta
     */
    public function handle(array $data, array $meta, string $traceId): void
    {
        self::$received = compact('data', 'meta', 'traceId');
    }

    /**
     * @param  array<string, mixed>  $data
     */
    public function failed(array $data, ?\Throwable $exception): void
    {
        self::$failed = true;
    }
}
