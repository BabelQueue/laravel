<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Integration;

use BabelQueue\Contracts\ShouldQueuePolyglot;
use BabelQueue\Queue\BabelQueueRedisQueue;
use BabelQueue\Queue\Jobs\BabelQueueJob;
use BabelQueue\Tests\TestCase;
use Illuminate\Contracts\Queue\Queue as QueueContract;

/**
 * The Redis driver against a real Redis (predis). Skipped unless
 * BABELQUEUE_TEST_REDIS is set; the CI coverage job provides a redis service.
 * Covers the produce + reserve/ack round-trip that the parent RedisQueue's Lua
 * reservation makes impractical to mock.
 */
final class BabelQueueRedisQueueTest extends TestCase
{
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
        $app['config']->set('queue.connections.bqredis', [
            'driver' => 'babelqueue-redis',
            'connection' => 'bqtest',
            'queue' => 'bqtest_' . bin2hex(random_bytes(4)),
            'retry_after' => 90,
            'block_for' => null,
        ]);
    }

    private function queue(): QueueContract
    {
        if (! getenv('BABELQUEUE_TEST_REDIS')) {
            $this->markTestSkipped('Set BABELQUEUE_TEST_REDIS to run the Redis driver integration test.');
        }

        return $this->app['queue']->connection('bqredis');
    }

    public function test_push_polyglot_then_pop_round_trips_the_canonical_envelope(): void
    {
        $queue = $this->queue();
        $this->assertInstanceOf(BabelQueueRedisQueue::class, $queue);

        $id = $queue->push(new RedisOrderJob());
        $this->assertNotSame('', $id);
        $this->assertSame(1, $queue->size());

        $job = $queue->pop();
        $this->assertInstanceOf(BabelQueueJob::class, $job);

        $env = json_decode($job->getRawBody(), true);
        $this->assertSame('urn:babel:orders:created', $env['job']);
        $this->assertSame($id, $env['meta']['id']);
        $this->assertSame(['order_id' => 7], $env['data']);
        $this->assertSame($id, $job->getJobId()); // prefers the polyglot meta.id

        $job->delete();
    }

    public function test_pop_returns_null_on_an_empty_queue(): void
    {
        $this->assertNull($this->queue()->pop());
    }

    public function test_push_of_a_standard_job_uses_the_parent_redis_queue(): void
    {
        $queue = $this->queue();
        $queue->push(new RedisPlainJob()); // not polyglot → parent RedisQueue::push

        $this->assertSame(1, $queue->size());
    }
}

final class RedisPlainJob
{
    public function handle(): void
    {
    }
}

final class RedisOrderJob implements ShouldQueuePolyglot
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
