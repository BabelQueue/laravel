<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Integration;

use BabelQueue\Consumer\BabelQueueDispatcher;
use BabelQueue\Idempotency\IdempotencyStore;
use BabelQueue\Idempotency\InMemoryStore;
use BabelQueue\Idempotency\PdoStore;
use BabelQueue\Queue\BabelQueueRabbitQueue;
use BabelQueue\Queue\BabelQueueRedisQueue;
use BabelQueue\Queue\BabelQueueSqsQueue;
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

        $app['config']->set('queue.connections.bq-sqs', [
            'driver' => 'babelqueue-sqs',
            'key' => 'test',
            'secret' => 'test',
            'region' => 'us-east-1',
            'prefix' => 'https://sqs.us-east-1.amazonaws.com/123456789012',
            'queue' => 'default',
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

    public function test_sqs_connection_resolves_to_the_polyglot_queue(): void
    {
        $this->assertInstanceOf(BabelQueueSqsQueue::class, Queue::connection('bq-sqs'));
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

    public function test_idempotency_store_binds_the_memory_backend(): void
    {
        $this->app['config']->set('babelqueue.idempotency.store', 'memory');

        $store = $this->app->make(IdempotencyStore::class);

        $this->assertInstanceOf(InMemoryStore::class, $store);
    }

    public function test_idempotency_store_binds_the_database_backend_over_a_laravel_connection(): void
    {
        // Drive the 'database' store onto the testbench sqlite connection: proves the provider
        // maps a Laravel DB connection's PDO onto the php-sdk PdoStore.
        $this->app['config']->set('database.default', 'testing');
        $this->app['config']->set('database.connections.testing', [
            'driver' => 'sqlite',
            'database' => ':memory:',
        ]);
        $this->app['config']->set('babelqueue.idempotency.store', 'database');
        $this->app['config']->set('babelqueue.idempotency.connection', 'testing');

        $store = $this->app->make(IdempotencyStore::class);

        $this->assertInstanceOf(PdoStore::class, $store);
    }

    public function test_idempotency_store_is_a_custom_rebindable_singleton(): void
    {
        $custom = new InMemoryStore();
        $this->app->instance(IdempotencyStore::class, $custom);

        $this->assertSame($custom, $this->app->make(IdempotencyStore::class));
    }

    public function test_dispatcher_dedupes_through_the_bound_store_when_enabled(): void
    {
        $this->app['config']->set('babelqueue.idempotency.enabled', true);
        $this->app['config']->set('babelqueue.idempotency.store', 'memory');
        $this->app['config']->set('babelqueue.handlers', [
            'urn:test:idem' => ProviderCountingConsumer::class,
        ]);

        ProviderCountingConsumer::$count = 0;
        $dispatcher = $this->app->make(BabelQueueDispatcher::class);

        $body = '{"job":"urn:test:idem","data":{},"meta":{"id":"prov-dup"}}';
        $dispatcher->dispatch($this->polyglotMessage($body));
        $dispatcher->dispatch($this->polyglotMessage($body));

        $this->assertSame(1, ProviderCountingConsumer::$count, 'the wired dispatcher dedupes via the bound store');
    }

    public function test_dispatcher_does_not_dedupe_when_idempotency_disabled(): void
    {
        $this->app['config']->set('babelqueue.idempotency.enabled', false);
        $this->app['config']->set('babelqueue.handlers', [
            'urn:test:idem' => ProviderCountingConsumer::class,
        ]);

        ProviderCountingConsumer::$count = 0;
        $dispatcher = $this->app->make(BabelQueueDispatcher::class);

        $body = '{"job":"urn:test:idem","data":{},"meta":{"id":"prov-dup"}}';
        $dispatcher->dispatch($this->polyglotMessage($body));
        $dispatcher->dispatch($this->polyglotMessage($body));

        $this->assertSame(2, ProviderCountingConsumer::$count, 'disabled idempotency leaves behaviour unchanged');
    }

    private function polyglotMessage(string $body): \BabelQueue\Contracts\PolyglotMessage
    {
        return new class($body, $this->app) extends \Illuminate\Queue\Jobs\Job implements \BabelQueue\Contracts\PolyglotMessage {
            use \BabelQueue\Queue\Concerns\ParsesPolyglotEnvelope;

            public function __construct(private string $body, \Illuminate\Container\Container $container)
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

class ProviderCountingConsumer
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
