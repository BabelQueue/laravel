<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Integration;

use BabelQueue\Queue\BabelQueueRedisQueue;
use BabelQueue\Queue\Jobs\BabelQueueJob;
use BabelQueue\Tests\Support\ConformanceResults;
use BabelQueue\Tests\TestCase;
use Illuminate\Contracts\Redis\Factory as RedisFactory;
use Illuminate\Redis\Connections\Connection;
use Illuminate\Support\Carbon;

/**
 * The behavioural `redis_layout` conformance blocks (zset layout, broker-bindings.md section 1.1 / 1.6,
 * ADR-0033) executed against a real Redis through the real queue class: `migrate`, `reserve`,
 * `pcall_poison` and `release`. The `delayed_score` block and the pure `redis_delay` section run in
 * `Feature/AdapterConformanceTest` (no Redis needed).
 *
 * Runner contract (manifest `redis_layout`): when `BQ_REQUIRE_REDIS` is set and Redis is unreachable the
 * runner FAILS; when it is unset the blocks are recorded as skipped with an ADR-cited reason.
 */
final class RedisLayoutConformanceTest extends TestCase
{
    private const LIVE_SUBS = ['migrate', 'reserve', 'pcall_poison', 'release'];

    /** Legacy `:processing` layout blocks: Laravel's Redis layout is the zset one only (ADR-0033). */
    private const LEGACY_SUBS = [
        'processing_sweep', 'processing_token', 'late_ack', 'identical_copies', 'retry_after_null',
        'lrem_count', 'malformed_member', 'orphan', 'legacy_release_gate', 'legacy_dlq_gate',
    ];

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

    public function test_legacy_processing_layout_blocks_do_not_apply_to_laravel(): void
    {
        foreach (self::LEGACY_SUBS as $sub) {
            ConformanceResults::skipAll(
                'redis_layout',
                'ADR-0033: the legacy :processing layout (broker-bindings.md section 1.7) is not used by the Laravel-compatible zset layout of this package; legacy mode belongs to the SDKs that implement it',
                $sub,
            );
        }

        $this->addToAssertionCount(1);
    }

    public function test_redis_layout_behavioural_blocks_against_a_real_redis(): void
    {
        $connection = $this->reachableConnection();

        if ($connection === null) {
            if (getenv('BQ_REQUIRE_REDIS') !== false && getenv('BQ_REQUIRE_REDIS') !== '') {
                $this->fail('BQ_REQUIRE_REDIS is set but Redis is not reachable (set BABELQUEUE_TEST_REDIS): the redis_layout blocks cannot be skipped (manifest runner contract)');
            }

            foreach (self::LIVE_SUBS as $sub) {
                ConformanceResults::skipAll(
                    'redis_layout',
                    'ADR-0033: no reachable Redis (BABELQUEUE_TEST_REDIS unset or down) and BQ_REQUIRE_REDIS is not set; the behavioural blocks need a real Redis',
                    $sub,
                );
            }
            $this->addToAssertionCount(1);
            $this->markTestSkipped('No reachable Redis: redis_layout behavioural blocks skipped (set BABELQUEUE_TEST_REDIS).');
        }

        foreach (self::LIVE_SUBS as $sub) {
            ConformanceResults::each('redis_layout', function (\stdClass $case) use ($connection): void {
                $this->runCase($connection, $case);
            }, $sub, true);
        }
    }

    private function reachableConnection(): ?Connection
    {
        if (! getenv('BABELQUEUE_TEST_REDIS')) {
            return null;
        }

        try {
            /** @var Connection $connection */
            $connection = $this->app['redis']->connection('bqtest');
            $connection->eval('return 1', 0);

            return $connection;
        } catch (\Throwable) {
            return null;
        }
    }

    private function runCase(Connection $connection, \stdClass $case): void
    {
        $queueName = (string) $case->queue;
        $base = 'queues:' . $queueName;
        $keys = [$base, $base . ':delayed', $base . ':reserved', $base . ':notify'];

        $retryAfter = property_exists($case, 'retry_after') ? ($case->retry_after === null ? null : (int) $case->retry_after) : 90;
        $batch = property_exists($case, 'migration_batch_size') ? (int) $case->migration_batch_size : -1;

        $factory = $this->app->make(RedisFactory::class);
        $queue = new BabelQueueRedisQueue($factory, $queueName, 'bqtest', $retryAfter, null, false, $batch);
        $queue->setContainer($this->app);
        $queue->setConnectionName('babelqueue-redis');

        $this->clear($connection, $keys);

        try {
            $this->seedState($connection, $base, $case->state);

            /** @var array<string, mixed> $reservations */
            $reservations = [];

            foreach ($case->steps as $step) {
                Carbon::setTestNow(Carbon::createFromTimestamp((int) floor((float) $step->now)));
                $expect = $step->expect ?? null;

                switch ($step->op) {
                    case 'push_delayed':
                        (new \ReflectionMethod($queue, 'laterRaw'))->invoke($queue, $step->delay, $step->body, $queueName);
                        break;
                    case 'migrate':
                        (new \ReflectionMethod($queue, 'migrate'))->invoke($queue, $base);
                        break;
                    case 'reserve':
                        $job = $queue->pop($queueName);
                        $outcome = $this->outcomeOf($job);
                        if (isset($step->as)) {
                            $reservations[(string) $step->as] = $job;
                        }
                        if ($expect !== null && isset($expect->outcome)) {
                            self::assertSame($expect->outcome, $outcome, 'reserve outcome');
                        }
                        if ($expect !== null && isset($expect->handler_body)) {
                            self::assertNotNull($job, 'a handler body was expected');
                            self::assertSame($expect->handler_body, $job->getRawBody(), 'handler body is the raw member');
                        }
                        if ($expect !== null && isset($expect->reported_attempts)) {
                            self::assertNotNull($job);
                            self::assertSame((int) $expect->reported_attempts, $job->attempts(), 'reported attempts');
                        }
                        break;
                    case 'release':
                        $job = $reservations[(string) $step->reservation] ?? null;
                        self::assertNotNull($job, 'release of an unknown reservation');
                        $job->release($step->delay);
                        break;
                    default:
                        self::fail('unknown op ' . $step->op);
                }

                if ($expect !== null) {
                    $this->assertState($connection, $base, $expect);
                }
            }
        } finally {
            Carbon::setTestNow();
            $this->clear($connection, $keys);
        }
    }

    /**
     * `empty` for no job, `poison` when the reserve script handed the raw element over untouched (a decodable
     * envelope is reserved as the re-encoded member with `attempts` incremented, so it never equals the raw body),
     * `delivered` otherwise. The outcome comes from what the Lua actually did; the SDK's own poison test
     * ({@see BabelQueueJob::isPoison()}) must agree with it, so a divergence between the two fails here.
     */
    private function outcomeOf(?BabelQueueJob $job): string
    {
        if ($job === null) {
            return 'empty';
        }

        $outcome = $job->getReservedJob() === $job->getRawBody() ? 'poison' : 'delivered';

        self::assertSame($outcome === 'poison', $job->isPoison(), 'BabelQueueJob::isPoison() disagrees with the reserve script');

        return $outcome;
    }

    /** @param list<string> $keys */
    private function clear(Connection $connection, array $keys): void
    {
        $connection->eval("for i = 1, #KEYS do redis.call('del', KEYS[i]) end return 1", count($keys), ...$keys);
    }

    private function seedState(Connection $connection, string $base, \stdClass $state): void
    {
        $ready = $this->members($state->ready ?? [], $state->ready_generate ?? null);
        foreach ($ready as $member) {
            $connection->eval("return redis.call('rpush', KEYS[1], ARGV[1])", 1, $base, $member);
        }

        $delayed = $this->entries($state->delayed ?? [], $state->delayed_generate ?? null);
        foreach ($delayed as [$member, $score]) {
            $connection->eval("return redis.call('zadd', KEYS[1], ARGV[1], ARGV[2])", 1, $base . ':delayed', $this->scoreArg($score), $member);
        }

        foreach ($this->entries($state->reserved ?? [], null) as [$member, $score]) {
            $connection->eval("return redis.call('zadd', KEYS[1], ARGV[1], ARGV[2])", 1, $base . ':reserved', $this->scoreArg($score), $member);
        }

        for ($i = 0; $i < (int) ($state->notify ?? 0); $i++) {
            $connection->eval("return redis.call('rpush', KEYS[1], '1')", 1, $base . ':notify');
        }
    }

    private function assertState(Connection $connection, string $base, \stdClass $expect): void
    {
        if (property_exists($expect, 'ready') || property_exists($expect, 'ready_generate')) {
            $want = $this->members($expect->ready ?? [], $expect->ready_generate ?? null);
            /** @var list<string> $got */
            $got = $connection->eval("return redis.call('lrange', KEYS[1], 0, -1)", 1, $base);
            self::assertSame($want, array_map('strval', $got), 'ready list');
        }

        if (property_exists($expect, 'notify')) {
            $length = $connection->eval("return redis.call('llen', KEYS[1])", 1, $base . ':notify');
            self::assertSame((int) $expect->notify, (int) $length, 'notify tokens');
        }

        foreach (['delayed', 'reserved'] as $set) {
            if (! property_exists($expect, $set) && ! property_exists($expect, $set . '_generate')) {
                continue;
            }

            $want = $this->entries($expect->{$set} ?? [], $expect->{$set . '_generate'} ?? null);
            $got = $this->zrange($connection, $base . ':' . $set);

            self::assertCount(count($want), $got, $set . ' size');

            $unmatched = $got;
            foreach ($want as [$wantMember, $wantScore, $wantJson]) {
                $found = null;
                foreach ($unmatched as $index => [$gotMember, $gotScore]) {
                    if ($this->scoreOf($wantScore) !== $gotScore) {
                        continue;
                    }
                    if ($wantJson === null ? $gotMember === $wantMember : $this->sameJson($wantJson, $gotMember)) {
                        $found = $index;
                        break;
                    }
                }
                self::assertNotNull($found, $set . ' is missing an entry with score ' . var_export($wantScore, true) . ': ' . ($wantMember ?? json_encode($wantJson)));
                unset($unmatched[$found]);
            }
        }
    }

    /** `{}` vs `[]` stay distinct because both sides are decoded without the assoc flag. */
    private function sameJson(mixed $want, string $gotRaw): bool
    {
        try {
            $got = json_decode($gotRaw, false, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            return false;
        }

        return json_encode($want, JSON_PRESERVE_ZERO_FRACTION) === json_encode($got, JSON_PRESERVE_ZERO_FRACTION);
    }

    /**
     * @param  list<mixed>  $listed
     * @return list<string>
     */
    private function members(array $listed, mixed $generate): array
    {
        $out = array_map('strval', $listed);

        if ($generate instanceof \stdClass) {
            for ($i = 0; $i < (int) $generate->count; $i++) {
                $out[] = sprintf((string) $generate->member, $i);
            }
        }

        return $out;
    }

    /**
     * @param  list<mixed>  $listed
     * @return list<array{0: ?string, 1: int|float|string, 2: mixed}> member, score, decoded-json-or-null
     */
    private function entries(array $listed, mixed $generate): array
    {
        $out = [];
        foreach ($listed as $entry) {
            if (property_exists($entry, 'member_json') || property_exists($entry, 'body_json')) {
                $json = property_exists($entry, 'member_json') ? $entry->member_json : $entry->body_json;
                $out[] = [null, $entry->score, $json];
                continue;
            }
            $out[] = [(string) $entry->member, $entry->score, null];
        }

        if ($generate instanceof \stdClass) {
            for ($i = 0; $i < (int) $generate->count; $i++) {
                $out[] = [sprintf((string) $generate->member, $i), $generate->score, null];
            }
        }

        return $out;
    }

    /** @return list<array{0: string, 1: float}> */
    private function zrange(Connection $connection, string $key): array
    {
        /** @var list<string> $flat */
        $flat = $connection->eval("return redis.call('zrange', KEYS[1], 0, -1, 'WITHSCORES')", 1, $key);
        $out = [];
        for ($i = 0; $i + 1 < count($flat); $i += 2) {
            $out[] = [(string) $flat[$i], $this->scoreOf($flat[$i + 1])];
        }

        return $out;
    }

    private function scoreOf(mixed $score): float
    {
        if (is_string($score)) {
            $lower = strtolower($score);
            if ($lower === 'inf' || $lower === '+inf') {
                return INF;
            }
            if ($lower === '-inf') {
                return -INF;
            }
        }

        return (float) $score;
    }

    private function scoreArg(mixed $score): string
    {
        return is_string($score) ? $score : (string) $score;
    }
}
