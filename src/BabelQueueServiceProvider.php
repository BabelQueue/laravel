<?php

declare(strict_types=1);

namespace BabelQueue;

use BabelQueue\Consumer\BabelQueueDispatcher;
use BabelQueue\Consumer\DeadLetterPublisher;
use BabelQueue\Producer\Publisher;
use BabelQueue\Queue\Connectors\BabelQueueRabbitConnector;
use BabelQueue\Queue\Connectors\BabelQueueRedisConnector;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Queue\QueueManager;
use Illuminate\Support\ServiceProvider;

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

        $this->app->singleton(BabelQueueDispatcher::class, static function (Application $app): BabelQueueDispatcher {
            /** @var array<string, mixed> $config */
            $config = $app['config']->get('babelqueue', []);

            return new BabelQueueDispatcher(
                $app,
                $config['handlers'] ?? [],
                (string) ($config['on_unknown_urn'] ?? 'fail'),
                (int) ($config['unknown_urn_release_delay'] ?? 0),
                $app->make(DeadLetterPublisher::class),
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

        if ($this->app->runningInConsole()) {
            $this->publishes([
                __DIR__ . '/../config/babelqueue.php' => $this->app->configPath('babelqueue.php'),
            ], 'babelqueue-config');
        }
    }
}
