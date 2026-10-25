<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Integration;

use BabelQueue\Consumer\BabelQueueDispatcher;
use BabelQueue\Consumer\DeadLetterPublisher;
use BabelQueue\Consumer\DeadLetterWriteFailed;
use BabelQueue\Queue\BabelQueueRedisQueue;
use BabelQueue\Queue\Jobs\BabelQueueJob;
use BabelQueue\Tests\TestCase;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Contracts\Redis\Factory as RedisFactory;
use Illuminate\Queue\Worker;
use Illuminate\Queue\WorkerOptions;
use Illuminate\Redis\Connections\Connection;
use Mockery;
use RuntimeException;

/**
 * ADR-0036 decisions 4 and 7 against a real Redis (skipped unless BABELQUEUE_TEST_REDIS is set; the CI jobs
 * provide a redis service): through the real reserve script and Laravel's real `Worker`,
 *
 *  - when the dead-letter write throws, the member stays in `:reserved` and `:delayed` stays empty, and the
 *    `retry_after` recovery brings it back on the ready list;
 *  - an identity-less body is written to `<queue>.dlq` as a `poison` record whatever `on_unknown_urn` says,
 *    and leaves `:reserved`.
 */
final class BabelQueueRedisDeadLetterFailureTest extends TestCase
{
    private const IDENTITY_LESS = '{"trace_id":"t-1","data":{"x":1},"attempts":0}';

    private string $queueName = '';

    /** @param \Illuminate\Foundation\Application $app */
    protected function defineEnvironment($app): void
    {
        $url = getenv('BABELQUEUE_TEST_REDIS') ?: 'redis://127.0.0.1:6379';
        $parts = parse_url($url) ?: [];

        $app['config']->set('database.redis.client', 'predis');
        $app['config']->set('database.redis.bqtest', [
            'host' => $parts['host'] ?? '127.0.0.1',
            'port' => $parts['port'] ?? 6379,
            'database' => isset($parts['path']) ? (int) ltrim((string) $parts['path'], '/') : 0,
        ]);
    }

    protected function tearDown(): void
    {
        if ($this->queueName !== '') {
            $this->command("for i = 1, #KEYS do redis.call('del', KEYS[i]) end return 1", $this->keys());
        }

        parent::tearDown();
    }

    private function queue(): BabelQueueRedisQueue
    {
        if (! getenv('BABELQUEUE_TEST_REDIS')) {
            $this->markTestSkipped('Set BABELQUEUE_TEST_REDIS to run the Redis dead-letter failure integration test.');
        }

        $this->queueName = 'bqdlq_' . bin2hex(random_bytes(4));
        $queue = new BabelQueueRedisQueue($this->app->make(RedisFactory::class), $this->queueName, 'bqtest', 90, null, false, -1);
        $queue->setContainer($this->app);
        $queue->setConnectionName('babelqueue-redis');

        return $queue;
    }

    /** @return list<string> */
    private function keys(): array
    {
        $base = 'queues:' . $this->queueName;

        return [$base, $base . ':delayed', $base . ':reserved', $base . ':notify', $base . '.dlq', $base . '.dlq:notify'];
    }

    /**
     * @param  list<string>  $keys
     * @param  list<string|int>  $args
     */
    private function command(string $lua, array $keys, array $args = []): mixed
    {
        /** @var Connection $connection */
        $connection = $this->app['redis']->connection('bqtest');

        return $connection->eval($lua, count($keys), ...$keys, ...$args);
    }

    private function cardinality(string $key, string $type): int
    {
        return (int) $this->command('return redis.call(ARGV[1], KEYS[1])', [$key], [$type]);
    }

    private function worker(): Worker
    {
        /** @var Worker $worker */
        $worker = $this->app->make('queue.worker');

        return $worker;
    }

    private function useBrokenDeadLetterQueue(string $strategy): void
    {
        $queue = Mockery::mock(QueueContract::class);
        $queue->shouldReceive('pushRaw')->andThrow(new RuntimeException('dlq broker is down'));
        $factory = Mockery::mock(QueueFactory::class);
        $factory->shouldReceive('connection')->andReturn($queue);

        $this->app->instance(BabelQueueDispatcher::class, new BabelQueueDispatcher(
            $this->app,
            [],
            $strategy,
            0,
            new DeadLetterPublisher($factory, ['enabled' => true]),
        ));
    }

    private function useRedisDeadLetterQueue(string $strategy): void
    {
        $this->app['config']->set('queue.connections.bqdlq', [
            'driver' => 'babelqueue-redis',
            'connection' => 'bqtest',
            'queue' => $this->queueName,
            'retry_after' => 90,
        ]);

        $this->app->instance(BabelQueueDispatcher::class, new BabelQueueDispatcher(
            $this->app,
            [],
            $strategy,
            0,
            new DeadLetterPublisher($this->app['queue'], ['enabled' => true, 'connection' => 'bqdlq']),
        ));
    }

    public function test_a_failed_dlq_write_leaves_the_member_in_reserved_and_the_retry_after_recovery_returns_it(): void
    {
        $queue = $this->queue();
        $this->useBrokenDeadLetterQueue('fail');
        $base = 'queues:' . $this->queueName;

        $queue->pushRaw(self::IDENTITY_LESS, $this->queueName);
        $job = $queue->pop($this->queueName);
        $this->assertInstanceOf(BabelQueueJob::class, $job);
        $this->assertSame(1, $this->cardinality($base . ':reserved', 'zcard'));

        try {
            $this->worker()->process('babelqueue-redis', $job, new WorkerOptions(maxTries: 0));
            $this->fail('the DLQ failure must reach the worker');
        } catch (DeadLetterWriteFailed) {
            $this->addToAssertionCount(1);
        }

        $this->assertSame(1, $this->cardinality($base . ':reserved', 'zcard'), 'the member stays in :reserved');
        $this->assertSame(0, $this->cardinality($base . ':delayed', 'zcard'), 'it is not moved to :delayed');
        $this->assertSame(0, $this->cardinality($base, 'llen'), 'it is not back on the ready list yet');

        // retry_after recovery: age the reservation past its deadline; the next pop migrates it back (attempt counted).
        $this->command(
            "local m = redis.call('zrange', KEYS[1], 0, 0)[1] return redis.call('zadd', KEYS[1], 1, m)",
            [$base . ':reserved'],
        );

        $again = $queue->pop($this->queueName);
        $this->assertInstanceOf(BabelQueueJob::class, $again);
        $this->assertSame(2, $again->attempts(), 'the recovery counted the first delivery: the second delivery is attempt 2');
        $this->assertSame('t-1', json_decode($again->getRawBody(), true)['trace_id'] ?? null);
    }

    public function test_an_identity_less_body_is_dead_lettered_as_poison_and_leaves_reserved(): void
    {
        $queue = $this->queue();
        $base = 'queues:' . $this->queueName;

        foreach (['delete', 'fail', 'release', 'dead_letter'] as $strategy) {
            $this->useRedisDeadLetterQueue($strategy);

            $queue->pushRaw(self::IDENTITY_LESS, $this->queueName);
            $job = $queue->pop($this->queueName);
            $this->assertInstanceOf(BabelQueueJob::class, $job, $strategy);

            $this->worker()->process('babelqueue-redis', $job, new WorkerOptions(maxTries: 0));

            $this->assertSame(0, $this->cardinality($base . ':reserved', 'zcard'), $strategy);
            $this->assertSame(0, $this->cardinality($base . ':delayed', 'zcard'), $strategy);
            $this->assertSame(0, $this->cardinality($base, 'llen'), $strategy);
        }

        $records = $this->command('return redis.call("lrange", KEYS[1], 0, -1)', [$base . '.dlq']);
        $this->assertIsArray($records);
        $this->assertCount(4, $records, 'one poison record per strategy');
        foreach ($records as $record) {
            $decoded = json_decode((string) $record, true, 512, JSON_THROW_ON_ERROR);
            $this->assertSame('poison', $decoded['dead_letter']['reason']);
            $this->assertSame($this->queueName, $decoded['dead_letter']['original_queue']);
            $this->assertSame('t-1', $decoded['trace_id'], 'the original bytes are kept');
        }
    }
}
