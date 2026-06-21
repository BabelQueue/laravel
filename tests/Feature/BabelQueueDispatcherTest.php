<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Feature;

use BabelQueue\Consumer\BabelQueueDispatcher;
use BabelQueue\Consumer\DeadLetterPublisher;
use BabelQueue\Contracts\PolyglotMessage;
use BabelQueue\Exceptions\UnknownUrnException;
use BabelQueue\Idempotency\ClaimingStore;
use BabelQueue\Idempotency\ClaimParkedException;
use BabelQueue\Idempotency\InMemoryStore;
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

    public function test_idempotency_disabled_runs_handler_on_every_delivery(): void
    {
        CountingConsumerStub::$count = 0;

        // No store passed → behaviour is unchanged: each delivery of the same id runs the handler.
        $dispatcher = new BabelQueueDispatcher($this->app, [
            'urn:babel:orders:process' => CountingConsumerStub::class,
        ]);

        $body = '{"job":"urn:babel:orders:process","data":{},"meta":{"id":"dup-1"}}';
        $first = $this->message($body);
        $second = $this->message($body);

        $dispatcher->dispatch($first);
        $dispatcher->dispatch($second);

        $this->assertSame(2, CountingConsumerStub::$count, 'with idempotency off the handler runs per delivery');
        $this->assertTrue($first->isDeleted());
        $this->assertTrue($second->isDeleted());
    }

    public function test_idempotent_consumption_dedupes_a_duplicate_delivery(): void
    {
        CountingConsumerStub::$count = 0;

        // A shared in-memory store across both deliveries: the first runs, the duplicate is skipped.
        $dispatcher = new BabelQueueDispatcher(
            $this->app,
            ['urn:babel:orders:process' => CountingConsumerStub::class],
            'fail',
            0,
            null,
            new InMemoryStore(),
        );

        $body = '{"job":"urn:babel:orders:process","data":{},"meta":{"id":"dup-2"}}';
        $first = $this->message($body);
        $second = $this->message($body);

        $dispatcher->dispatch($first);
        $dispatcher->dispatch($second);

        $this->assertSame(1, CountingConsumerStub::$count, 'a duplicate delivery must not run the handler again');
        $this->assertTrue($first->isDeleted(), 'first delivery is acked after running');
        $this->assertTrue($second->isDeleted(), 'a deduped duplicate is still acked so the broker stops redelivering');
    }

    public function test_idempotent_consumption_runs_a_distinct_id(): void
    {
        CountingConsumerStub::$count = 0;

        $store = new InMemoryStore();
        $dispatcher = new BabelQueueDispatcher(
            $this->app,
            ['urn:babel:orders:process' => CountingConsumerStub::class],
            'fail',
            0,
            null,
            $store,
        );

        $dispatcher->dispatch($this->message('{"job":"urn:babel:orders:process","data":{},"meta":{"id":"a"}}'));
        $dispatcher->dispatch($this->message('{"job":"urn:babel:orders:process","data":{},"meta":{"id":"b"}}'));

        $this->assertSame(2, CountingConsumerStub::$count, 'distinct ids each run once');
    }

    public function test_idempotent_consumption_is_fail_open_without_a_message_id(): void
    {
        CountingConsumerStub::$count = 0;

        $dispatcher = new BabelQueueDispatcher(
            $this->app,
            ['urn:babel:orders:process' => CountingConsumerStub::class],
            'fail',
            0,
            null,
            new InMemoryStore(),
        );

        // No usable meta.id → cannot dedupe; every delivery runs (fail-open).
        $body = '{"job":"urn:babel:orders:process","data":{},"meta":{}}';
        $dispatcher->dispatch($this->message($body));
        $dispatcher->dispatch($this->message($body));

        $this->assertSame(2, CountingConsumerStub::$count);
    }

    public function test_failed_handler_leaves_id_unmarked_so_a_redelivery_reruns(): void
    {
        ThrowOnceConsumerStub::$attempts = 0;

        $store = new InMemoryStore();
        $dispatcher = new BabelQueueDispatcher(
            $this->app,
            ['urn:babel:orders:process' => ThrowOnceConsumerStub::class],
            'fail',
            0,
            null,
            $store,
        );

        $body = '{"job":"urn:babel:orders:process","data":{},"meta":{"id":"retry-1"}}';

        // First delivery throws → the id is NOT remembered, the exception propagates (retry/DLQ apply).
        try {
            $dispatcher->dispatch($this->message($body));
            $this->fail('the failing handler should have thrown');
        } catch (RuntimeException $e) {
            $this->assertSame('boom', $e->getMessage());
        }
        $this->assertFalse($store->seen('retry-1'), 'a thrown handler must not mark the id seen');

        // Redelivery: the handler runs again and now succeeds, then commits.
        $dispatcher->dispatch($this->message($body));

        $this->assertSame(2, ThrowOnceConsumerStub::$attempts);
        $this->assertTrue($store->seen('retry-1'));
    }

    public function test_claiming_store_parks_a_concurrent_in_flight_duplicate(): void
    {
        CountingConsumerStub::$count = 0;

        // A claiming store whose claim() is already held → ClaimingDispatch throws ClaimParkedException
        // so the delivery is NOT acked and the broker redelivers it later.
        $store = new FakeClaimingStore(claimGranted: false, seen: false);
        $dispatcher = new BabelQueueDispatcher(
            $this->app,
            ['urn:babel:orders:process' => CountingConsumerStub::class],
            'fail',
            0,
            null,
            $store,
        );

        $message = $this->message('{"job":"urn:babel:orders:process","data":{},"meta":{"id":"inflight-1"}}');

        try {
            $dispatcher->dispatch($message);
            $this->fail('a lost claim should park via ClaimParkedException');
        } catch (ClaimParkedException $e) {
            $this->assertSame('inflight-1', $e->messageId);
        }

        $this->assertSame(0, CountingConsumerStub::$count, 'a parked delivery does not run the handler');
        $this->assertFalse($message->isDeleted(), 'a parked delivery must NOT be acked');
    }

    public function test_claiming_store_runs_then_commits_when_the_claim_is_won(): void
    {
        CountingConsumerStub::$count = 0;

        $store = new FakeClaimingStore(claimGranted: true, seen: false);
        $dispatcher = new BabelQueueDispatcher(
            $this->app,
            ['urn:babel:orders:process' => CountingConsumerStub::class],
            'fail',
            0,
            null,
            $store,
        );

        $message = $this->message('{"job":"urn:babel:orders:process","data":{},"meta":{"id":"won-1"}}');
        $dispatcher->dispatch($message);

        $this->assertSame(1, CountingConsumerStub::$count);
        $this->assertSame(['won-1'], $store->remembered, 'a won claim commits on success');
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

/**
 * Counts how many times handle() runs — the probe for "did the handler run once / twice / zero
 * times" under idempotent consumption.
 */
class CountingConsumerStub
{
    public static int $count = 0;

    /**
     * @param  array<string, mixed>  $data
     * @param  array<string, mixed>  $meta
     */
    public function handle(array $data, array $meta, string $traceId): void
    {
        self::$count++;
    }
}

/**
 * Throws on its first invocation and succeeds afterwards — proves a thrown handler leaves the id
 * unmarked so a redelivery re-runs (post-success dedupe, not at-least-once defeating).
 */
class ThrowOnceConsumerStub
{
    public static int $attempts = 0;

    /**
     * @param  array<string, mixed>  $data
     * @param  array<string, mixed>  $meta
     */
    public function handle(array $data, array $meta, string $traceId): void
    {
        self::$attempts++;

        if (self::$attempts === 1) {
            throw new RuntimeException('boom');
        }
    }
}

/**
 * A deterministic {@see ClaimingStore} fake: claim() returns a fixed verdict and seen() a fixed
 * flag, so the test drives the {@see \BabelQueue\Idempotency\ClaimingDispatch} branches (claim won
 * vs lost) without a live Redis/PDO backend.
 */
final class FakeClaimingStore implements ClaimingStore
{
    /** @var list<string> */
    public array $remembered = [];

    /** @var list<string> */
    public array $released = [];

    public function __construct(
        private bool $claimGranted,
        private bool $seen,
    ) {
    }

    public function seen(string $messageId): bool
    {
        return $this->seen;
    }

    public function claim(string $messageId, int $ttlSeconds): bool
    {
        return $this->claimGranted;
    }

    public function release(string $messageId): void
    {
        $this->released[] = $messageId;
    }

    public function remember(string $messageId): void
    {
        $this->remembered[] = $messageId;
    }

    public function forget(string $messageId): void
    {
    }
}
