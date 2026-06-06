<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Integration;

use BabelQueue\Consumer\BabelQueueDispatcher;
use BabelQueue\Queue\BabelQueueRabbitQueue;
use BabelQueue\Queue\BabelQueueRedisQueue;
use BabelQueue\Tests\TestCase;
use Illuminate\Support\Facades\Queue;

/**
 * Proves the provider wires both polyglot drivers and the URN dispatcher into a
 * real Laravel application container. No live broker is touched: resolving a
 * queue connection only constructs the driver — it does not open a socket.
 */
final class ServiceProviderTest extends TestCase
{
    /**
     * @param  \Illuminate\Foundation\Application  $app
     */
    protected function defineEnvironment($app): void
    {
        $app['config']->set('queue.connections.bq-redis', [
            'driver' => 'babelqueue-redis',
            'connection' => 'default',
            'queue' => 'default',
        ]);

        $app['config']->set('queue.connections.bq-rabbit', [
            'driver' => 'babelqueue-rabbitmq',
            'queue' => 'default',
            'exchange' => '',
        ]);

        $app['config']->set('babelqueue.handlers', [
            'urn:test:order' => \stdClass::class,
        ]);
    }

    public function test_redis_connection_resolves_to_the_polyglot_queue(): void
    {
        $this->assertInstanceOf(BabelQueueRedisQueue::class, Queue::connection('bq-redis'));
    }

    public function test_rabbitmq_connection_resolves_to_the_polyglot_queue(): void
    {
        $this->assertInstanceOf(BabelQueueRabbitQueue::class, Queue::connection('bq-rabbit'));
    }

    public function test_dispatcher_is_a_singleton_built_from_config(): void
    {
        $first = $this->app->make(BabelQueueDispatcher::class);
        $second = $this->app->make(BabelQueueDispatcher::class);

        $this->assertInstanceOf(BabelQueueDispatcher::class, $first);
        $this->assertSame($first, $second);
    }

    public function test_package_config_is_merged(): void
    {
        $this->assertSame(
            \stdClass::class,
            $this->app['config']->get('babelqueue.handlers.urn:test:order'),
        );
    }
}
