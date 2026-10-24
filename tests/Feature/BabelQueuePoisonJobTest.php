<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Feature;

use BabelQueue\Consumer\BabelQueueDispatcher;
use BabelQueue\Consumer\DeadLetterPublisher;
use BabelQueue\Queue\BabelQueueRedisQueue;
use BabelQueue\Queue\Jobs\BabelQueueJob;
use BabelQueue\Tests\TestCase;
use Illuminate\Contracts\Events\Dispatcher as EventDispatcher;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Events\JobFailed;
use Mockery;

/**
 * broker-bindings.md section 1.1 (A1): the reserve script hands over a body that does not decode to an object
 * with a numeric `attempts` untouched. The consume job must route it to the DLQ (`poison`) or the failed
 * path, remove it from `:reserved`, and never release it or throw while counting attempts.
 */
final class BabelQueuePoisonJobTest extends TestCase
{
    /** @return iterable<string, array{0: string}> */
    public static function poisonBodies(): iterable
    {
        yield 'not json' => ['not-json'];
        yield 'empty string' => [''];
        yield 'truncated json' => ['{"job":"urn:bq:a:b","data":'];
        yield 'attempts is a string' => ['{"job":"urn:bq:a:b","data":{},"attempts":"3"}'];
        yield 'attempts is an array' => ['{"job":"urn:bq:a:b","data":{},"attempts":[1]}'];
        yield 'attempts is null' => ['{"job":"urn:bq:a:b","data":{},"attempts":null}'];
        yield 'attempts is missing' => ['{"job":"urn:bq:a:b","data":{}}'];
        yield 'top-level list' => ['[1,2]'];
        yield 'top-level string' => ['"x"'];
        // the reserve script (cjson) flags these; PHP json_decode would read the first one as a valid envelope
        yield 'number beyond double range' => ['{"job":"urn:bq:a:b","attempts":0,"data":{"x":1e400}}'];
        yield 'invalid utf-8' => ["\xff\xfe{"];
    }

    /** @return iterable<string, array{0: string}> */
    public static function nonNumericAttempts(): iterable
    {
        yield 'string' => ['{"job":"urn:bq:a:b","attempts":"3"}'];
        yield 'array' => ['{"job":"urn:bq:a:b","attempts":[1]}'];
        yield 'null' => ['{"job":"urn:bq:a:b","attempts":null}'];
        yield 'missing' => ['{"job":"urn:bq:a:b"}'];
        yield 'not json' => ['not-json'];
    }

    private function job(string $body, ?BabelQueueRedisQueue $redis = null): BabelQueueJob
    {
        return new BabelQueueJob(
            $this->app,
            $redis ?? $this->redisExpectingNoRelease(),
            $body,
            $body,
            'babelqueue-redis',
            'orders',
        );
    }

    /** A Redis queue that accepts the `:reserved` removal and fails the test on any release. */
    private function redisExpectingNoRelease(int $deletes = 0): BabelQueueRedisQueue
    {
        $redis = Mockery::mock(BabelQueueRedisQueue::class);
        $redis->shouldReceive('deleteReserved')->times($deletes)->andReturnNull();
        $redis->shouldNotReceive('deleteAndRelease');
        $redis->shouldNotReceive('release');

        return $redis;
    }

    /** @param array<string, mixed>|null $captured */
    private function useDeadLetterQueue(?array &$captured, bool $enabled = true): void
    {
        $queue = Mockery::mock(QueueContract::class);

        if ($enabled) {
            $queue->shouldReceive('pushRaw')->once()->withArgs(function ($payload, $target) use (&$captured): bool {
                $captured = ['body' => json_decode($payload, true), 'queue' => $target];

                return true;
            })->andReturn('x');
        } else {
            $queue->shouldNotReceive('pushRaw');
        }

        $factory = Mockery::mock(QueueFactory::class);
        $factory->shouldReceive('connection')->andReturn($queue);

        $this->app->instance(BabelQueueDispatcher::class, new BabelQueueDispatcher(
            $this->app,
            [],
            'fail',
            0,
            new DeadLetterPublisher($factory, ['enabled' => $enabled]),
        ));
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('poisonBodies')]
    public function test_poison_body_goes_to_the_dlq_with_reason_poison_and_leaves_reserved(string $body): void
    {
        $captured = null;
        $this->useDeadLetterQueue($captured);

        $job = $this->job($body, $this->redisExpectingNoRelease(1));

        $job->fire();

        $this->assertNotNull($captured, 'the poison body must be published to the DLQ');
        $this->assertSame('orders.dlq', $captured['queue']);
        $this->assertSame('poison', $captured['body']['dead_letter']['reason']);
        $this->assertTrue($job->isDeleted(), 'the element must be removed from :reserved');
        $this->assertFalse($job->isReleased());
    }

    public function test_poison_is_the_reserve_script_signal_not_a_php_reinterpretation_of_the_body(): void
    {
        $body = '{"job":"urn:bq:a:b","attempts":0,"data":{"x":1e400}}';

        // reserved === raw: the script handed the element over untouched
        $this->assertTrue((new BabelQueueJob($this->app, Mockery::mock(BabelQueueRedisQueue::class), $body, $body, 'babelqueue-redis', 'orders'))->isPoison());

        // reserved !== raw: the script re-encoded the envelope with attempts incremented, even for a body PHP rejects
        $this->assertFalse((new BabelQueueJob($this->app, Mockery::mock(BabelQueueRedisQueue::class), 'not-json', 'reserved-member', 'babelqueue-redis', 'orders'))->isPoison());
    }

    /**
     * broker-bindings.md section 1.1 (K-G): cjson decodes and re-encodes an envelope that carries invalid UTF-8
     * with a numeric `attempts`, so the reserved member differs from the raw body. It is still poison: dead-lettered
     * on first delivery as the ADR-0035 base64 wrapper, never dispatched, never released.
     */
    public function test_a_decodable_envelope_with_invalid_utf8_is_poison_dead_lettered_in_the_base64_wrapper(): void
    {
        $raw = "{\"job\":\"urn:bq:a:b\",\"attempts\":0,\"data\":{\"s\":\"\xff\xfe\"}}";
        $reserved = "{\"job\":\"urn:bq:a:b\",\"attempts\":1,\"data\":{\"s\":\"\xff\xfe\"}}";

        $handled = false;
        $captured = null;
        $queue = Mockery::mock(QueueContract::class);
        $queue->shouldReceive('pushRaw')->once()->withArgs(function ($payload, $target) use (&$captured): bool {
            $captured = ['body' => json_decode($payload, true), 'queue' => $target];

            return true;
        })->andReturn('x');
        $factory = Mockery::mock(QueueFactory::class);
        $factory->shouldReceive('connection')->andReturn($queue);
        $this->app->instance(BabelQueueDispatcher::class, new BabelQueueDispatcher(
            $this->app,
            ['urn:bq:a:b' => static function () use (&$handled): void {
                $handled = true;
            }],
            'fail',
            0,
            new DeadLetterPublisher($factory, ['enabled' => true]),
        ));

        $redis = $this->redisExpectingNoRelease(1);
        $job = new BabelQueueJob($this->app, $redis, $raw, $reserved, 'babelqueue-redis', 'orders');

        $this->assertTrue($job->isPoison());

        $job->fire();

        $this->assertFalse($handled, 'a handler must never see an invalid UTF-8 envelope');
        $this->assertFalse($job->isReleased());
        $this->assertTrue($job->isDeleted());
        $this->assertNotNull($captured);
        $this->assertSame('orders.dlq', $captured['queue']);
        $this->assertSame('poison', $captured['body']['dead_letter']['reason']);
        $this->assertSame('base64', $captured['body']['raw_encoding']);
        $this->assertSame($raw, base64_decode($captured['body']['raw'], true), 'the stored bytes are carried exactly');
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('poisonBodies')]
    public function test_poison_body_takes_the_failed_path_when_the_dlq_is_disabled(string $body): void
    {
        $captured = null;
        $this->useDeadLetterQueue($captured, false);

        $failed = [];
        $events = $this->app->make(EventDispatcher::class);
        $events->listen(JobFailed::class, static function (JobFailed $event) use (&$failed): void {
            $failed[] = $event->exception->getMessage();
        });

        $job = $this->job($body, $this->redisExpectingNoRelease(1));

        $job->fire();

        $this->assertCount(1, $failed, 'the poison body must reach the failed path exactly once');
        $this->assertTrue($job->hasFailed());
        $this->assertTrue($job->isDeleted(), 'the element must be removed from :reserved');
        $this->assertFalse($job->isReleased());
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('nonNumericAttempts')]
    public function test_attempts_never_throws_and_counts_a_non_numeric_value_as_zero(string $body): void
    {
        $this->assertSame(1, $this->job($body)->attempts());
    }

    public function test_attempts_adds_one_to_a_numeric_value(): void
    {
        $this->assertSame(3, $this->job('{"job":"urn:bq:a:b","attempts":2}')->attempts());
        $this->assertSame(1, $this->job('{"job":"urn:bq:a:b","attempts":0}')->attempts());
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('poisonBodies')]
    public function test_get_name_is_safe_for_any_body(string $body): void
    {
        $this->assertIsString($this->job($body)->getName());
    }

    public function test_a_valid_envelope_is_not_poison_and_still_reaches_the_dispatcher(): void
    {
        $body = '{"job":"urn:bq:a:b","data":{},"meta":{"schema_version":1},"attempts":0}';
        // the reserve script re-encodes a decodable envelope (attempts + 1), so the reserved member differs from the raw body
        $job = new BabelQueueJob($this->app, $this->redisExpectingNoRelease(), $body, str_replace('"attempts":0', '"attempts":1', $body), 'babelqueue-redis', 'orders');

        $this->assertFalse($job->isPoison());
    }

    public function test_dispatcher_quarantine_is_a_noop_when_the_dlq_is_disabled(): void
    {
        $captured = null;
        $this->useDeadLetterQueue($captured, false);

        $job = $this->job('not-json', $this->redisExpectingNoRelease());

        $this->assertFalse($this->app->make(BabelQueueDispatcher::class)->quarantinePoison($job));
        $this->assertFalse($job->isDeleted(), 'nothing may be touched so the caller can fail the message');
    }
}
