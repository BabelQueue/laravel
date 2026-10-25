<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Feature;

use BabelQueue\Consumer\BabelQueueDispatcher;
use BabelQueue\Consumer\DeadLetterPublisher;
use BabelQueue\Consumer\DeadLetterWriteFailed;
use BabelQueue\Exceptions\UnknownUrnException;
use BabelQueue\Queue\BabelQueueRedisQueue;
use BabelQueue\Queue\Jobs\BabelQueueJob;
use BabelQueue\Tests\TestCase;
use Illuminate\Contracts\Events\Dispatcher as EventDispatcher;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Worker;
use Illuminate\Queue\WorkerOptions;
use Mockery;
use RuntimeException;

/**
 * Laravel Redis, ADR-0036 decisions 4 and 7 (broker-bindings.md sections 1.1 and 1.7), driven through the real
 * `BabelQueueJob` and Laravel's real `Worker` with a mocked Redis queue (the live-Redis counterpart is
 * `Integration/BabelQueueRedisDeadLetterFailureTest`):
 *
 *  - an identity-less body (no valid `job` and no valid `urn`) is poison whatever `on_unknown_urn` says, with the DLQ enabled;
 *  - a failed dead-letter write leaves the reservation in `:reserved`: nothing deletes it and the worker does not release it.
 */
final class BabelQueueDeadLetterFailureTest extends TestCase
{
    private const IDENTITY_LESS = '{"trace_id":"t-1","data":{"x":1},"attempts":0}';

    private const UNMAPPED = '{"job":"urn:bq:not:mapped","trace_id":"t-1","data":{},"meta":{"id":"m-1","queue":"orders","schema_version":1},"attempts":0}';

    private const MAPPED = '{"job":"urn:bq:a:b","trace_id":"t-1","data":{},"meta":{"id":"m-2","queue":"orders","schema_version":1},"attempts":0}';

    /** @var list<string> */
    private array $trace = [];

    protected function setUp(): void
    {
        parent::setUp();

        DlqFailureHandler::$handled = 0;
        DlqFailureHandler::$failedHook = 0;
        DlqFailureHandler::$throw = false;
    }

    /** @return iterable<string, array{0: string, 1: string}> */
    public static function identityLessCases(): iterable
    {
        foreach (['delete', 'fail', 'release', 'dead_letter'] as $strategy) {
            yield $strategy . ' / no identity' => [$strategy, self::IDENTITY_LESS];
            yield $strategy . ' / blank job, no urn' => [$strategy, '{"job":" \t","data":{},"attempts":0}'];
            yield $strategy . ' / job not a string, urn not a string' => [$strategy, '{"job":5,"urn":["x"],"data":{},"attempts":0}'];
        }
    }

    /**
     * @param  (callable(string, string): void)|null  $push  the DLQ write; null = record it
     * @return array{0: BabelQueueDispatcher, 1: \Closure(): list<array{0: string, 1: string}>}
     */
    private function useDeadLetterQueue(string $strategy, bool $enabled = true, ?callable $push = null): array
    {
        $written = [];
        $queue = Mockery::mock(QueueContract::class);
        $queue->shouldReceive('pushRaw')->andReturnUsing(function (string $payload, string $target) use (&$written, $push): string {
            if ($push !== null) {
                $push($payload, $target);
            }
            $written[] = [$payload, $target];
            $this->trace[] = 'publish';

            return 'x';
        });
        $factory = Mockery::mock(QueueFactory::class);
        $factory->shouldReceive('connection')->andReturn($queue);

        $dispatcher = new BabelQueueDispatcher(
            $this->app,
            ['urn:bq:a:b' => DlqFailureHandler::class],
            $strategy,
            0,
            new DeadLetterPublisher($factory, ['enabled' => $enabled]),
        );
        $this->app->instance(BabelQueueDispatcher::class, $dispatcher);

        return [$dispatcher, static function () use (&$written): array {
            return $written;
        }];
    }

    private function dlqDown(): callable
    {
        return static function (): void {
            throw new RuntimeException('dlq broker is down');
        };
    }

    /** A Redis queue mock that records the `:reserved` removal and the release. */
    private function redis(int $deletes, int $releases = 0): BabelQueueRedisQueue
    {
        $redis = Mockery::mock(BabelQueueRedisQueue::class);
        $redis->shouldReceive('deleteReserved')->times($deletes)->andReturnUsing(function (): void {
            $this->trace[] = 'delete';
        });
        $redis->shouldReceive('deleteAndRelease')->times($releases)->andReturnNull();
        $redis->shouldNotReceive('release');

        return $redis;
    }

    /** The reserve script re-encodes a decodable body with attempts + 1, so the reserved member differs from the raw one. */
    private function job(string $body, BabelQueueRedisQueue $redis): BabelQueueJob
    {
        $reserved = preg_replace('/"attempts":0/', '"attempts":1', $body) ?? $body;
        $this->assertNotSame($body, $reserved);

        return new BabelQueueJob($this->app, $redis, $body, $reserved, 'babelqueue-redis', 'orders');
    }

    private function worker(): Worker
    {
        /** @var Worker $worker */
        $worker = $this->app->make('queue.worker');

        return $worker;
    }

    private function logSpy(int $times): void
    {
        $log = Mockery::mock();
        $log->shouldReceive('error')->times($times)->withArgs(function (string $message, array $context): bool {
            return str_contains($message, 'dead-letter write failed')
                && $context['queue'] === 'orders'
                && ! array_key_exists('body', $context);
        });
        $this->app->instance('log', $log);
    }

    // -- decision 4: an identity-less body is poison, unconditionally (DLQ enabled) ----------------------------

    #[\PHPUnit\Framework\Attributes\DataProvider('identityLessCases')]
    public function test_an_identity_less_body_is_dead_lettered_as_poison_whatever_on_unknown_urn_says(string $strategy, string $body): void
    {
        [, $written] = $this->useDeadLetterQueue($strategy);

        $job = $this->job($body, $this->redis(1));
        $job->fire();

        $this->assertSame(0, DlqFailureHandler::$handled);
        $this->assertTrue($job->isDeleted(), 'the element is removed from :reserved');
        $this->assertFalse($job->isReleased());
        $this->assertCount(1, $written());
        [$payload, $target] = $written()[0];
        $this->assertSame('orders.dlq', $target);
        $record = json_decode($payload, true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame('poison', $record['dead_letter']['reason']);
        $this->assertSame('orders', $record['dead_letter']['original_queue']);
        $this->assertStringContainsString('"data":', $payload);
        $this->assertStringContainsString(substr($body, 1, strlen($body) - 2), $payload, 'the original bytes are spliced, not re-encoded');
    }

    public function test_an_identity_less_body_keeps_the_existing_path_when_the_dlq_is_disabled(): void
    {
        [, $written] = $this->useDeadLetterQueue('delete', false);
        $job = $this->job(self::IDENTITY_LESS, $this->redis(1));
        $job->fire();
        $this->assertTrue($job->isDeleted(), 'on_unknown_urn: delete');
        $this->assertSame([], $written());

        $this->useDeadLetterQueue('fail', false);
        $job = $this->job(self::IDENTITY_LESS, $this->redis(0));
        $this->expectException(UnknownUrnException::class);
        $job->fire();
    }

    public function test_a_valid_unmapped_urn_still_follows_on_unknown_urn(): void
    {
        [, $written] = $this->useDeadLetterQueue('delete');

        $job = $this->job(self::UNMAPPED, $this->redis(1));
        $job->fire();

        $this->assertTrue($job->isDeleted());
        $this->assertSame([], $written(), 'a valid envelope with no handler is not poison');
    }

    // -- decision 7: a failed DLQ write leaves the member in :reserved ----------------------------------------

    public function test_a_failed_poison_write_keeps_the_reservation_and_the_worker_does_not_release_it(): void
    {
        $this->useDeadLetterQueue('fail', true, $this->dlqDown());
        $this->logSpy(1);

        $job = $this->job(self::IDENTITY_LESS, $this->redis(0, 0)); // neither delete nor deleteAndRelease

        try {
            $this->worker()->process('babelqueue-redis', $job, new WorkerOptions(maxTries: 0));
            $this->fail('the DLQ failure must reach the worker (it is reported there)');
        } catch (DeadLetterWriteFailed $e) {
            $this->assertStringContainsString('dlq broker is down', $e->getMessage());
            $this->assertInstanceOf(RuntimeException::class, $e->getPrevious());
        }

        $this->assertFalse($job->isDeleted());
        $this->assertFalse($job->isReleased());
        $this->assertTrue($job->hasFailed(), 'marked failed so the worker skips its release()');
        $this->assertSame([], $this->trace);
    }

    public function test_the_laravel_exception_is_the_cores_exception_with_the_reason_kept(): void
    {
        $cause = new RuntimeException('dlq broker is down');
        $e = DeadLetterWriteFailed::forQueue('orders.dlq', 'poison', $cause);

        $this->assertInstanceOf(\BabelQueue\Consume\DeadLetterWriteFailed::class, $e);
        $this->assertInstanceOf(\BabelQueue\Exceptions\BabelQueueException::class, $e);
        $this->assertSame('Writing the dead-letter record (reason "poison") to [orders.dlq] failed: dlq broker is down', $e->getMessage());
        $this->assertSame($cause, $e->getPrevious());

        // the core's two-argument form keeps working, and a failure is required
        $core = DeadLetterWriteFailed::forQueue('orders.dlq', $cause);
        $this->assertInstanceOf(DeadLetterWriteFailed::class, $core);
        $this->assertSame('Writing the dead-letter record to [orders.dlq] failed: dlq broker is down', $core->getMessage());
        $this->expectException(\InvalidArgumentException::class);
        DeadLetterWriteFailed::forQueue('orders.dlq', 'poison');
    }

    public function test_a_failed_unknown_urn_dead_letter_write_keeps_the_reservation(): void
    {
        $this->useDeadLetterQueue('dead_letter', true, $this->dlqDown());
        $this->logSpy(1);

        $job = $this->job(self::UNMAPPED, $this->redis(0, 0));

        $this->expectException(DeadLetterWriteFailed::class);
        try {
            $this->worker()->process('babelqueue-redis', $job, new WorkerOptions(maxTries: 0));
        } finally {
            $this->assertFalse($job->isDeleted());
            $this->assertFalse($job->isReleased());
        }
    }

    public function test_a_failed_unsupported_version_dead_letter_write_keeps_the_reservation(): void
    {
        $this->useDeadLetterQueue('fail', true, $this->dlqDown());
        $this->logSpy(1);

        $body = '{"job":"urn:bq:a:b","data":{},"meta":{"id":"m","queue":"orders","schema_version":2},"attempts":0}';
        $job = $this->job($body, $this->redis(0, 0));

        $this->expectException(DeadLetterWriteFailed::class);
        try {
            $this->worker()->process('babelqueue-redis', $job, new WorkerOptions(maxTries: 0));
        } finally {
            $this->assertFalse($job->isDeleted());
            $this->assertFalse($job->isReleased());
        }
    }

    public function test_a_handler_failure_is_still_released_by_the_worker(): void
    {
        // The control: only a DLQ write failure keeps the reservation, a plain handler exception is retried.
        $this->useDeadLetterQueue('fail');
        DlqFailureHandler::$throw = true;

        $job = $this->job(self::MAPPED, $this->redis(0, 1)); // deleteAndRelease exactly once

        try {
            $this->worker()->process('babelqueue-redis', $job, new WorkerOptions(maxTries: 0));
            $this->fail('the handler exception reaches the worker');
        } catch (RuntimeException $e) {
            $this->assertSame('handler boom', $e->getMessage());
        }

        $this->assertTrue($job->isReleased());
    }

    public function test_exhausted_retries_with_a_failed_dlq_write_keep_the_reservation(): void
    {
        $this->useDeadLetterQueue('fail', true, $this->dlqDown());
        $this->logSpy(1);

        $job = $this->job(self::MAPPED, $this->redis(0, 0));

        try {
            $job->fail(new RuntimeException('handler boom'));
            $this->fail('the DLQ failure is rethrown');
        } catch (DeadLetterWriteFailed) {
            $this->assertFalse($job->isDeleted(), 'Laravel deletes before failed(); the DLQ record is written first here');
            $this->assertTrue($job->hasFailed());
            $this->assertSame(0, DlqFailureHandler::$failedHook);
        }
    }

    public function test_exhausted_retries_write_the_dlq_record_before_deleting_and_only_once(): void
    {
        [, $written] = $this->useDeadLetterQueue('fail');
        $failedEvents = 0;
        $this->app->make(EventDispatcher::class)->listen(JobFailed::class, static function () use (&$failedEvents): void {
            $failedEvents++;
        });

        $job = $this->job(self::MAPPED, $this->redis(1));
        $job->fail(new RuntimeException('handler boom'));

        $this->assertSame(['publish', 'delete'], $this->trace);
        $this->assertCount(1, $written(), 'the failed record is written once, not again by failed()');
        $record = json_decode($written()[0][0], true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame('failed', $record['dead_letter']['reason']);
        $this->assertSame('handler boom', $record['dead_letter']['error']);
        $this->assertSame(1, DlqFailureHandler::$failedHook, 'the handler failed() hook still runs');
        $this->assertSame(1, $failedEvents);
    }

    public function test_exhausted_retries_without_a_dlq_behave_as_before(): void
    {
        [, $written] = $this->useDeadLetterQueue('fail', false);

        $job = $this->job(self::MAPPED, $this->redis(1));
        $job->fail(new RuntimeException('handler boom'));

        $this->assertSame([], $written());
        $this->assertTrue($job->isDeleted());
        $this->assertSame(1, DlqFailureHandler::$failedHook);
    }
}

final class DlqFailureHandler
{
    public static int $handled = 0;

    public static int $failedHook = 0;

    public static bool $throw = false;

    public function handle(): void
    {
        self::$handled++;

        if (self::$throw) {
            throw new RuntimeException('handler boom');
        }
    }

    public function failed(): void
    {
        self::$failedHook++;
    }
}
