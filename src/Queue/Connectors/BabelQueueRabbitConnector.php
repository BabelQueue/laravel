<?php

declare(strict_types=1);

namespace BabelQueue\Queue\Connectors;

use BabelQueue\Queue\BabelQueueRabbitQueue;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\Connectors\ConnectorInterface;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPSSLConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;

/**
 * Connector for the "babelqueue-rabbitmq" queue driver.
 *
 * It does NOT open a socket eagerly. Instead it hands the queue a lightweight
 * factory closure that lazily builds an AMQP connection on first use, which
 * also makes transparent reconnection trivial for the queue itself.
 */
class BabelQueueRabbitConnector implements ConnectorInterface
{
    /**
     * Establish a queue connection backed by the polyglot RabbitMQ queue.
     *
     * @param  array<string, mixed>  $config
     */
    public function connect(array $config): Queue
    {
        return new BabelQueueRabbitQueue($this->connectionFactory($config), $config);
    }

    /**
     * Build a closure that returns a fresh, connected AMQP connection.
     *
     * @param  array<string, mixed>  $config
     * @return \Closure(): AbstractConnection
     */
    protected function connectionFactory(array $config): \Closure
    {
        return static function () use ($config): AbstractConnection {
            $host = (string) ($config['host'] ?? '127.0.0.1');
            $port = (int) ($config['port'] ?? 5672);
            $user = (string) ($config['user'] ?? 'guest');
            $password = (string) ($config['password'] ?? 'guest');
            $vhost = (string) ($config['vhost'] ?? '/');

            $ssl = $config['options']['ssl'] ?? false;

            if ($ssl !== false) {
                return new AMQPSSLConnection(
                    $host,
                    $port,
                    $user,
                    $password,
                    $vhost,
                    is_array($ssl) ? $ssl : [],
                );
            }

            return new AMQPStreamConnection($host, $port, $user, $password, $vhost);
        };
    }
}
