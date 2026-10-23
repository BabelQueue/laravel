<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Feature;

use BabelQueue\Queue\BabelQueueRedisQueue;
use BabelQueue\Queue\Connectors\BabelQueueRedisConnector;
use BabelQueue\Queue\Jobs\BabelQueueJob;
use BabelQueue\Tests\TestCase;
use Illuminate\Contracts\Redis\Factory as RedisFactory;
use Illuminate\Redis\Connections\Connection as RedisConnection;
use Mockery;

/**
 * The reserve / release mechanics of {@see BabelQueueRedisQueue} on a mocked Redis client (the Lua itself
 * only runs against a real Redis: tests/Integration/RedisLayoutConformanceTest.php).
 */
final class BabelQueueRedisPopTest extends TestCase
{
    /** @param list<mixed> $reply what the reserve script returns: `[raw, reserved]` */
    private function queueReplying(array $reply): BabelQueueRedisQueue
    {
        $connection = Mockery::mock(RedisConnection::class);
        $connection->shouldReceive('eval')->andReturn($reply);
        $connection->shouldReceive('command')->with('eval', Mockery::type('array'))->andReturn($reply);
        $factory = Mockery::mock(RedisFactory::class);
        $factory->shouldReceive('connection')->andReturn($connection);

        $queue = new BabelQueueRedisQueue($factory, 'default', null, 60, null);
        $queue->setContainer($this->app);
        $queue->setConnectionName('babelqueue-redis');

        return $queue;
    }

    /** @return iterable<string, array{0: string}> */
    public static function falsyMembers(): iterable
    {
        yield 'empty string' => [''];
        yield 'zero string' => ['0'];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('falsyMembers')]
    public function test_a_reserved_member_php_calls_falsy_still_reaches_the_handler(string $member): void
    {
        $job = $this->queueReplying([$member, $member])->pop('default');

        $this->assertInstanceOf(BabelQueueJob::class, $job);
        $this->assertSame($member, $job->getRawBody());
        $this->assertSame($member, $job->getReservedJob());
    }

    public function test_an_empty_reply_is_an_empty_queue(): void
    {
        $this->assertNull($this->queueReplying([null, null])->pop('default'));
    }

    public function test_released_body_increments_attempts_and_keeps_every_other_member(): void
    {
        $raw = '{"job":"urn:bq:a:b","data":{"e":[],"o":{},"f":1.0,"s":"ü\/x","big":123456789012345},"attempts":2,"x_ext":{"k":[1,2]}}';

        $released = BabelQueueRedisQueue::releasedBody($raw);

        $this->assertSame(
            '{"job":"urn:bq:a:b","data":{"e":[],"o":{},"f":1.0,"s":"ü\/x","big":123456789012345},"attempts":3,"x_ext":{"k":[1,2]}}',
            $released,
        );
    }

    public function test_released_body_keeps_integers_beyond_int64_and_float_spelling_byte_for_byte(): void
    {
        $raw = '{"job":"u","data":{"n":12345678901234567890,"f":1.50,"e":1E5},"attempts":0}';

        $this->assertSame(
            '{"job":"u","data":{"n":12345678901234567890,"f":1.50,"e":1E5},"attempts":1}',
            BabelQueueRedisQueue::releasedBody($raw),
        );
    }

    public function test_released_body_rewrites_only_the_top_level_attempts_member(): void
    {
        $raw = '{"data":{"attempts":9,"s":"x\"},\"attempts\":7"},"x":[{"attempts":5}],"attempts" : 2 }';

        $this->assertSame(
            '{"data":{"attempts":9,"s":"x\"},\"attempts\":7"},"x":[{"attempts":5}],"attempts" : 3 }',
            BabelQueueRedisQueue::releasedBody($raw),
        );
    }

    public function test_released_body_follows_the_last_duplicate_attempts_key(): void
    {
        $this->assertSame('{"attempts":1,"attempts":5}', BabelQueueRedisQueue::releasedBody('{"attempts":1,"attempts":4}'));
    }

    public function test_released_body_increments_a_float_attempts_as_a_float(): void
    {
        $this->assertSame('{"attempts":3.0}', BabelQueueRedisQueue::releasedBody('{"attempts":2.0}'));
    }

    /** @return iterable<string, array{0: string}> */
    public static function unreleasableBodies(): iterable
    {
        yield 'not json' => ['not-json'];
        yield 'top-level null' => ['null'];
        yield 'list' => ['[1,2]'];
        yield 'attempts is a string' => ['{"attempts":"1"}'];
        yield 'no attempts' => ['{"job":"urn:bq:a:b"}'];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('unreleasableBodies')]
    public function test_released_body_is_null_when_the_body_is_not_an_object_with_numeric_attempts(string $raw): void
    {
        $this->assertNull(BabelQueueRedisQueue::releasedBody($raw));
    }

    public function test_pop_script_decodes_under_pcall_and_reserves_the_raw_element_otherwise(): void
    {
        $script = BabelQueueRedisQueue::popScript();

        $this->assertStringContainsString('pcall(cjson.decode, job)', $script);
        $this->assertStringContainsString("type(decoded['attempts']) == 'number'", $script);
        $this->assertStringContainsString('reserved = job', $script);
    }

    public function test_pop_script_re_encodes_under_pcall_so_an_unencodable_body_is_still_reserved(): void
    {
        $script = BabelQueueRedisQueue::popScript();

        // cjson.encode raises on values it cannot represent (NaN / inf from an overflowing number): without
        // pcall the whole reserve would abort after the element left the queue. The raw element is kept instead.
        $this->assertStringContainsString('pcall(cjson.encode, decoded)', $script);
        $this->assertLessThan(
            strpos($script, "redis.call('zadd', KEYS[2]"),
            strpos($script, 'pcall(cjson.encode, decoded)'),
        );
    }

    public function test_a_falsy_member_taken_from_a_secondary_queue_marks_the_secondary_queue_as_having_a_job(): void
    {
        $queue = $this->queueReplying(['0', '0']);
        $had = new \ReflectionProperty($queue, 'secondaryQueueHadJob');

        $queue->pop('default', 1);

        $this->assertTrue($had->getValue($queue));
    }

    public function test_a_falsy_member_taken_from_the_primary_queue_does_not_touch_the_secondary_flag(): void
    {
        $queue = $this->queueReplying(['0', '0']);
        $had = new \ReflectionProperty($queue, 'secondaryQueueHadJob');

        $queue->pop('default', 0);

        $this->assertFalse($had->getValue($queue));
    }

    public function test_connector_defaults_retry_after_to_90_and_migration_batch_size_to_unlimited(): void
    {
        $factory = Mockery::mock(RedisFactory::class);
        $queue = (new BabelQueueRedisConnector($factory))->connect(['queue' => 'default']);

        $retryAfter = new \ReflectionProperty($queue, 'retryAfter');
        $batch = new \ReflectionProperty($queue, 'migrationBatchSize');

        $this->assertSame(90, $retryAfter->getValue($queue));
        $this->assertSame(-1, $batch->getValue($queue));
    }
}
