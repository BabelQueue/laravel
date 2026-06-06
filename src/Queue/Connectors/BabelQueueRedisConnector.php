<?php

declare(strict_types=1);

namespace BabelQueue\Queue\Connectors;

use BabelQueue\Queue\BabelQueueRedisQueue;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\Connectors\RedisConnector;

/**
 * Connector for the "babelqueue-redis" queue driver.
 *
 * It mirrors {@see RedisConnector::connect()} verbatim (Laravel 11/12 arg
 * order) but hands back a {@see BabelQueueRedisQueue} instead of the stock
 * RedisQueue. The connection-resolution, retry and blocking semantics are thus
 * identical to Laravel's native Redis driver.
 */
class BabelQueueRedisConnector extends RedisConnector
{
    /**
     * Establish a queue connection backed by the polyglot Redis queue.
     *
     * @param  array<string, mixed>  $config
     */
    public function connect(array $config): Queue
    {
        return new BabelQueueRedisQueue(
            $this->redis,
            $config['queue'] ?? 'default',
            $config['connection'] ?? $this->connection,
            $config['retry_after'] ?? 60,
            $config['block_for'] ?? null,
            $config['after_commit'] ?? false,
            $config['migration_batch_size'] ?? -1,
        );
    }
}
