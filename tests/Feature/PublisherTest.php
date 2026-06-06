<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Feature;

use BabelQueue\Facades\BabelQueue;
use BabelQueue\Producer\Publisher;
use BabelQueue\Support\PolyglotEnvelopeJob;
use BabelQueue\Tests\TestCase;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Mockery;

/**
 * Covers the ergonomic producer facade. The Publisher must wrap (urn, data) in a
 * PolyglotEnvelopeJob and push it through the normal queue pipeline — so the
 * facade path and the interface path share one encoder and one wire format.
 */
final class PublisherTest extends TestCase
{
    public function test_publish_wraps_urn_and_data_and_returns_the_id(): void
    {
        $queue = Mockery::mock(QueueContract::class);
        $queue->shouldReceive('push')->once()->withArgs(function ($job, $data, $q): bool {
            return $job instanceof PolyglotEnvelopeJob
                && $job->getBabelUrn() === 'urn:babel:orders:created'
                && $job->toPayload() === ['order_id' => 1042]
                && $job->getBabelTraceId() === null
                && $data === ''
                && $q === null;
        })->andReturn('meta-id-1');

        $factory = Mockery::mock(QueueFactory::class);
        $factory->shouldReceive('connection')->once()->with(null)->andReturn($queue);

        $publisher = new Publisher($factory);

        $this->assertSame('meta-id-1', $publisher->publish('urn:babel:orders:created', ['order_id' => 1042]));
    }

    public function test_publish_targets_a_queue_and_forwards_an_inherited_trace_id(): void
    {
        $queue = Mockery::mock(QueueContract::class);
        $queue->shouldReceive('push')->once()->withArgs(function ($job, $data, $q): bool {
            return $job instanceof PolyglotEnvelopeJob
                && $job->getBabelTraceId() === 'trace-xyz'
                && $q === 'orders';
        })->andReturn('id');

        $factory = Mockery::mock(QueueFactory::class);
        $factory->shouldReceive('connection')->once()->with('bq-redis')->andReturn($queue);

        (new Publisher($factory))
            ->onConnection('bq-redis')
            ->publish('urn:babel:orders:created', [], 'orders', 'trace-xyz');
    }

    public function test_later_publishes_with_a_delay(): void
    {
        $queue = Mockery::mock(QueueContract::class);
        $queue->shouldReceive('later')->once()->withArgs(function ($delay, $job, $data, $q): bool {
            return $delay === 60
                && $job instanceof PolyglotEnvelopeJob
                && $job->getBabelUrn() === 'urn:babel:orders:created';
        })->andReturn('id');

        $factory = Mockery::mock(QueueFactory::class);
        $factory->shouldReceive('connection')->once()->with(null)->andReturn($queue);

        (new Publisher($factory))->later(60, 'urn:babel:orders:created', ['order_id' => 1]);
    }

    public function test_facade_proxies_to_the_publisher_service(): void
    {
        // Publisher is final (not Mockery-mockable); swap a real one backed by a
        // mocked queue and assert the facade call reaches it and returns its id.
        $queue = Mockery::mock(QueueContract::class);
        $queue->shouldReceive('push')->once()->withArgs(
            fn ($job) => $job instanceof PolyglotEnvelopeJob && $job->getBabelUrn() === 'urn:babel:orders:created',
        )->andReturn('id-9');

        $factory = Mockery::mock(QueueFactory::class);
        $factory->shouldReceive('connection')->once()->andReturn($queue);

        BabelQueue::swap(new Publisher($factory));

        $this->assertSame('id-9', BabelQueue::publish('urn:babel:orders:created', ['order_id' => 1]));
    }
}
