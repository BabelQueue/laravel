<?php

declare(strict_types=1);

namespace BabelQueue\Queue\Connectors;

use BabelQueue\Queue\BabelQueueArtemisQueue;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Stomp\Client;
use Stomp\StatefulStomp;

/**
 * Connector for the "babelqueue-artemis" queue driver (Apache ActiveMQ Artemis over STOMP).
 *
 * Like the RabbitMQ connector it does NOT open a socket eagerly: it hands the queue a lazy
 * factory closure that builds a connected {@see StatefulStomp} on first use, so transparent
 * reconnection stays trivial for the queue itself.
 *
 * STOMP is the PHP path to the §7 Artemis binding (ADR-0018): Artemis bridges STOMP ↔ core ↔
 * AMQP 1.0 ↔ JMS on the same address, so a message this driver produces is consumed natively by
 * the Java (JMS) / .NET / Node / Python / Go Artemis SDKs, and a message they produce is consumed
 * here. Routing is body-authoritative (§7.8) — the dispatcher routes on the envelope's `job` URN.
 */
class BabelQueueArtemisConnector implements ConnectorInterface
{
    /**
     * Establish a queue connection backed by the polyglot Artemis (STOMP) queue.
     *
     * It does NOT open a socket eagerly: the queue receives a lazy factory closure that builds and
     * connects a STOMP client on first use, which also makes transparent reconnection trivial.
     *
     * @param  array<string, mixed>  $config
     */
    public function connect(array $config): Queue
    {
        return new BabelQueueArtemisQueue(fn (): StatefulStomp => $this->makeConnection($config), $config);
    }

    /**
     * Build a configured client and connect it.
     *
     * @param  array<string, mixed>  $config
     */
    protected function makeConnection(array $config): StatefulStomp
    {
        $client = $this->configureClient($config);
        $client->connect();

        return new StatefulStomp($client);
    }

    /**
     * Build a configured-but-unconnected STOMP client from the connection config (URI, credentials,
     * vhost, read timeout). Separated from the socket-opening connect() so it is unit-testable
     * without a broker.
     *
     * @param  array<string, mixed>  $config
     */
    protected function configureClient(array $config): Client
    {
        $scheme = ($config['options']['ssl'] ?? false) !== false ? 'ssl' : 'tcp';
        $uri = sprintf(
            '%s://%s:%d',
            $scheme,
            (string) ($config['host'] ?? '127.0.0.1'),
            (int) ($config['port'] ?? 61613),
        );

        $client = new Client($uri);

        if (! empty($config['username'])) {
            $client->setLogin((string) $config['username'], (string) ($config['password'] ?? ''));
        }

        if (! empty($config['vhost'])) {
            $client->setVhostname((string) $config['vhost']);
        }

        // A short read timeout makes pop() non-blocking-ish: read() returns no frame once the
        // timeout lapses, so the Laravel worker loop polls instead of blocking indefinitely.
        $client->getConnection()->setReadTimeout((int) ($config['read_timeout'] ?? 1), 0);

        return $client;
    }
}
