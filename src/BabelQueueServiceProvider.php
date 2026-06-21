<?php

declare(strict_types=1);

namespace BabelQueue;

use BabelQueue\Consumer\BabelQueueDispatcher;
use BabelQueue\Consumer\DeadLetterPublisher;
use BabelQueue\Idempotency\IdempotencyStore;
use BabelQueue\Idempotency\InMemoryStore;
use BabelQueue\Idempotency\PdoStore;
use BabelQueue\Idempotency\RedisStore;
use BabelQueue\Producer\Publisher;
use BabelQueue\Queue\Connectors\BabelQueueArtemisConnector;
use BabelQueue\Queue\Connectors\BabelQueueRabbitConnector;
use BabelQueue\Queue\Connectors\BabelQueueRedisConnector;
use BabelQueue\Queue\Connectors\BabelQueueSqsConnector;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Queue\QueueManager;
use Illuminate\Support\ServiceProvider;
use InvalidArgumentException;

/**
 * Wires BabelQueue into the Laravel application.
 *
 * Produce side: it teaches Laravel's QueueManager how to build the polyglot
 * drivers — any config/queue.php connection whose "driver" is "babelqueue-redis"
 * or "babelqueue-rabbitmq" resolves to the matching BabelQueue queue.
 *
 * Consume side: it registers the {@see BabelQueueDispatcher} (configured from
 * config/babelqueue.php) that routes inbound messages from their URN to a PHP
 * handler class.
 */
class BabelQueueServiceProvider extends ServiceProvider
{
    /**
     * Register container bindings: merge config and bind the URN dispatcher.
     */
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/../config/babelqueue.php', 'babelqueue');

        $this->app->singleton(DeadLetterPublisher::class, static function (Application $app): DeadLetterPublisher {
            return new DeadLetterPublisher(
                $app['queue'],
                (array) ($app['config']->get('babelqueue.dead_letter', [])),
            );
        });

        // The idempotency store (bound only when used). A user may rebind this to
        // any IdempotencyStore — including a custom one — and the dispatcher will
        // dedupe through it. Default backends are Laravel-native (Redis / database).
        $this->app->singleton(IdempotencyStore::class, static function (Application $app): IdempotencyStore {
            /** @var array<string, mixed> $config */
            $config = (array) ($app['config']->get('babelqueue.idempotency', []));

            return self::makeIdempotencyStore($app, $config);
        });

        $this->app->singleton(BabelQueueDispatcher::class, static function (Application $app): BabelQueueDispatcher {
            /** @var array<string, mixed> $config */
            $config = $app['config']->get('babelqueue', []);

            /** @var array<string, mixed> $idempotency */
            $idempotency = (array) ($config['idempotency'] ?? []);
            $idempotencyEnabled = (bool) ($idempotency['enabled'] ?? false);

            return new BabelQueueDispatcher(
                $app,
                $config['handlers'] ?? [],
                (string) ($config['on_unknown_urn'] ?? 'fail'),
                (int) ($config['unknown_urn_release_delay'] ?? 0),
                $app->make(DeadLetterPublisher::class),
                $idempotencyEnabled ? $app->make(IdempotencyStore::class) : null,
                (int) ($idempotency['ttl'] ?? 3600),
            );
        });

        // Producer facade service (BabelQueue\Facades\BabelQueue → publish()).
        $this->app->singleton(Publisher::class, static function (Application $app): Publisher {
            return new Publisher(
                $app['queue'],
                $app['config']->get('babelqueue.connection'),
            );
        });
    }

    /**
     * Register the queue connectors and expose the publishable config.
     *
     * Connectors are resolved lazily, only when a queue connection is first
     * established, so registering them in boot() is safe and sufficient.
     */
    public function boot(): void
    {
        /** @var QueueManager $manager */
        $manager = $this->app['queue'];

        $manager->addConnector('babelqueue-redis', function (): BabelQueueRedisConnector {
            return new BabelQueueRedisConnector($this->app['redis']);
        });

        $manager->addConnector('babelqueue-rabbitmq', function (): BabelQueueRabbitConnector {
            return new BabelQueueRabbitConnector();
        });

        $manager->addConnector('babelqueue-sqs', function (): BabelQueueSqsConnector {
            return new BabelQueueSqsConnector();
        });

        $manager->addConnector('babelqueue-artemis', function (): BabelQueueArtemisConnector {
            return new BabelQueueArtemisConnector();
        });

        if ($this->app->runningInConsole()) {
            $this->publishes([
                __DIR__ . '/../config/babelqueue.php' => $this->app->configPath('babelqueue.php'),
            ], 'babelqueue-config');
        }
    }

    /**
     * Build the configured php-sdk idempotency store over a Laravel-native backend.
     *
     * 'redis' / 'database' map onto Laravel's own Redis and database connections, so an app
     * reuses the infrastructure it already runs — and both are ClaimingStores, so the dispatcher
     * gets the atomic in-flight claim (park-on-duplicate). 'memory' is the single-process
     * InMemoryStore for tests / a lone worker.
     *
     * @param  array<string, mixed>  $config  The babelqueue.idempotency config block.
     */
    private static function makeIdempotencyStore(Application $app, array $config): IdempotencyStore
    {
        $store = (string) ($config['store'] ?? 'redis');
        $connection = $config['connection'] ?? null;
        $connection = is_string($connection) && $connection !== '' ? $connection : null;

        switch ($store) {
            case 'redis':
                // Laravel's Redis connection exposes the underlying predis client via client();
                // RedisStore speaks the same predis ClientInterface the reference transport uses.
                /** @var \Illuminate\Redis\RedisManager $redis */
                $redis = $app['redis'];
                $client = $redis->connection($connection)->client();

                if (! $client instanceof \Predis\ClientInterface) {
                    throw new InvalidArgumentException(
                        'BabelQueue idempotency store "redis" requires a predis-backed Redis '
                        . 'connection (config/database.php redis.client = "predis").'
                    );
                }

                return new RedisStore($client, (string) ($config['prefix'] ?? 'bq:idem:'));

            case 'database':
                /** @var \Illuminate\Database\DatabaseManager $db */
                $db = $app['db'];

                return new PdoStore(
                    $db->connection($connection)->getPdo(),
                    (string) ($config['table'] ?? 'bq_idempotency'),
                );

            case 'memory':
                return new InMemoryStore();

            default:
                throw new InvalidArgumentException(sprintf(
                    'Unknown BabelQueue idempotency store [%s]. Use "redis", "database", "memory", '
                    . 'or rebind %s to a custom store.',
                    $store,
                    IdempotencyStore::class,
                ));
        }
    }
}
