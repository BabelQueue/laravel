<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Feature;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\ShouldQueuePolyglot;
use BabelQueue\Exceptions\BabelQueueException;
use BabelQueue\Queue\BabelQueueRabbitQueue;
use BabelQueue\Queue\Connectors\BabelQueueRabbitConnector;
use BabelQueue\Queue\Jobs\BabelQueueRabbitJob;
use BabelQueue\Tests\TestCase;
use Mockery;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

/**
 * The RabbitMQ driver, exercised against a mocked AMQP channel (no broker): the
 * produce path (canonical envelope + contract AMQP properties), the consume path
 * (basic_get → job → ack/release), size, delays and topology.
 */
final class BabelQueueRabbitQueueTest extends TestCase
{
    /** @param array<string,mixed> $config */
    private function makeQueue(AMQPChannel $channel, array $config = []): BabelQueueRabbitQueue
    {
        $connection = Mockery::mock(AbstractConnection::class);
        $connection->shouldReceive('isConnected')->andReturn(true);
        $connection->shouldReceive('channel')->andReturn($channel);
        $connection->shouldReceive('close')->andReturnNull();
        $connection->shouldReceive('isWriting')->andReturn(false);

        $queue = new BabelQueueRabbitQueue(static fn () => $connection, array_merge(['queue' => 'orders'], $config));
        $queue->setContainer($this->app);
        $queue->setConnectionName('babelqueue-rabbitmq');

        return $queue;
    }

    /** A channel mock with the topology/no-op calls already stubbed. */
    private function channel(int $messageCount = 0): Mockery\MockInterface
    {
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('is_open')->andReturn(true);
        $channel->shouldReceive('queue_declare')->andReturn(['orders', $messageCount, 0]);
        $channel->shouldReceive('exchange_declare')->andReturnNull();
        $channel->shouldReceive('queue_bind')->andReturnNull();
        $channel->shouldReceive('close')->andReturnNull();

        return $channel;
    }

    public function test_push_publishes_the_canonical_envelope_with_contract_properties(): void
    {
        $captured = null;
        $channel = $this->channel();
        $channel->shouldReceive('basic_publish')->once()->with(
            Mockery::on(function (AMQPMessage $m) use (&$captured): bool {
                $captured = $m;

                return true;
            }),
            '',        // default exchange
            'orders',  // routing key == queue name
        );

        $id = $this->makeQueue($channel)->push(new RabbitOrderJob());

        $this->assertNotSame('', $id);
        $env = json_decode($captured->getBody(), true);
        $this->assertSame('urn:babel:orders:created', $env['job']);
        $this->assertSame(['order_id' => 7], $env['data']);
        $this->assertSame($id, $env['meta']['id']);

        $props = $captured->get_properties();
        $this->assertSame('application/json', $props['content_type']);
        $this->assertSame('urn:babel:orders:created', $props['type']);
        $this->assertSame($env['trace_id'], $props['correlation_id']);
        $this->assertSame($id, $props['message_id']);
        $this->assertSame('babelqueue', $props['app_id']);

        $headers = $props['application_headers']->getNativeData();
        $this->assertSame(1, $headers['x-schema-version']);
        $this->assertSame('php', $headers['x-source-lang']);
    }

    public function test_push_raw_publishes_and_returns_the_payload_id(): void
    {
        $channel = $this->channel();
        $channel->shouldReceive('basic_publish')->once();

        $payload = EnvelopeCodec::encode(EnvelopeCodec::fromJob(new RabbitOrderJob(), 'orders'));
        $id = $this->makeQueue($channel)->pushRaw($payload, 'orders');

        $this->assertSame(json_decode($payload, true)['meta']['id'], $id);
    }

    public function test_push_of_a_standard_job_falls_back_to_raw_publish(): void
    {
        $channel = $this->channel();
        $channel->shouldReceive('basic_publish')->once();

        // A plain queued object goes through createPayload()/enqueueRaw(); no exception.
        $this->makeQueue($channel)->push(new RabbitPlainJob());
        $this->addToAssertionCount(1);
    }

    public function test_later_without_a_delayed_exchange_throws(): void
    {
        $this->expectException(BabelQueueException::class);

        $this->makeQueue($this->channel())->later(60, new RabbitOrderJob());
    }

    public function test_later_with_a_delayed_exchange_sets_the_x_delay_header(): void
    {
        $captured = null;
        $channel = $this->channel();
        $channel->shouldReceive('basic_publish')->once()->with(
            Mockery::on(function (AMQPMessage $m) use (&$captured): bool {
                $captured = $m;

                return true;
            }),
            'delayed',
            'orders',
        );

        $this->makeQueue($channel, ['exchange' => 'delayed', 'exchange_type' => 'x-delayed-message'])
            ->later(5, new RabbitOrderJob());

        $headers = $captured->get_properties()['application_headers']->getNativeData();
        $this->assertSame(5000, $headers['x-delay']);
    }

    public function test_size_returns_the_ready_message_count(): void
    {
        $this->assertSame(5, $this->makeQueue($this->channel(5))->size('orders'));
    }

    public function test_pop_on_an_empty_queue_returns_null(): void
    {
        $channel = $this->channel();
        $channel->shouldReceive('basic_get')->once()->with('orders')->andReturnNull();

        $this->assertNull($this->makeQueue($channel)->pop('orders'));
    }

    public function test_pop_wraps_a_message_and_delete_acks_it(): void
    {
        $body = EnvelopeCodec::encode(EnvelopeCodec::fromJob(new RabbitOrderJob(), 'orders'));
        $channel = $this->channel();
        $channel->shouldReceive('basic_ack')->once()->with(1);

        $message = Mockery::mock(AMQPMessage::class);
        $message->shouldReceive('getBody')->andReturn($body);
        $message->shouldReceive('getChannel')->andReturn($channel);
        $message->shouldReceive('getDeliveryTag')->andReturn(1);
        $message->shouldReceive('has')->with('message_id')->andReturn(false);
        $message->shouldReceive('has')->with('application_headers')->andReturn(false);

        $channel->shouldReceive('basic_get')->once()->with('orders')->andReturn($message);

        $job = $this->makeQueue($channel)->pop('orders');

        $this->assertInstanceOf(BabelQueueRabbitJob::class, $job);
        $this->assertSame($body, $job->getRawBody());
        $this->assertSame(json_decode($body, true)['meta']['id'], $job->getJobId());
        $this->assertSame(1, $job->attempts()); // no prior attempts header → 1
        $job->delete();
    }

    public function test_release_republishes_with_incremented_attempts_then_acks(): void
    {
        $body = EnvelopeCodec::encode(EnvelopeCodec::fromJob(new RabbitOrderJob(), 'orders'));
        $channel = $this->channel();
        $channel->shouldReceive('basic_publish')->once(); // the republish
        $channel->shouldReceive('basic_ack')->once()->with(1);

        $headers = new AMQPTable(['x-attempts' => 2]);
        $message = Mockery::mock(AMQPMessage::class);
        $message->shouldReceive('getBody')->andReturn($body);
        $message->shouldReceive('getChannel')->andReturn($channel);
        $message->shouldReceive('getDeliveryTag')->andReturn(1);
        $message->shouldReceive('has')->with('message_id')->andReturn(true);
        $message->shouldReceive('get')->with('message_id')->andReturn('msg-1');
        $message->shouldReceive('has')->with('application_headers')->andReturn(true);
        $message->shouldReceive('get')->with('application_headers')->andReturn($headers);
        $message->shouldReceive('get_properties')->andReturn(['application_headers' => $headers]);

        $channel->shouldReceive('basic_get')->once()->with('orders')->andReturn($message);

        $job = $this->makeQueue($channel)->pop('orders');
        $this->assertSame('msg-1', $job->getJobId());
        $this->assertSame(3, $job->attempts()); // prior 2 + this delivery
        $job->release();
    }

    public function test_later_with_a_standard_job_and_delayed_exchange_publishes(): void
    {
        $channel = $this->channel();
        $channel->shouldReceive('basic_publish')->once();

        $this->makeQueue($channel, ['exchange' => 'delayed', 'exchange_type' => 'x-delayed-message'])
            ->later(5, new RabbitPlainJob());
        $this->addToAssertionCount(1);
    }

    public function test_push_raw_with_a_non_json_payload_returns_a_null_id(): void
    {
        $channel = $this->channel();
        $channel->shouldReceive('basic_publish')->once();

        $this->assertNull($this->makeQueue($channel)->pushRaw('not-json', 'orders'));
    }

    public function test_connector_builds_a_rabbit_queue_without_opening_a_socket(): void
    {
        $queue = (new BabelQueueRabbitConnector())->connect(['queue' => 'orders', 'host' => '127.0.0.1']);

        $this->assertInstanceOf(BabelQueueRabbitQueue::class, $queue);
        $this->assertSame('orders', $queue->getQueue(null));
    }
}

final class RabbitOrderJob implements ShouldQueuePolyglot
{
    public function getBabelUrn(): string
    {
        return 'urn:babel:orders:created';
    }

    /** @return array<string, mixed> */
    public function toPayload(): array
    {
        return ['order_id' => 7];
    }
}

final class RabbitPlainJob
{
    public function handle(): void
    {
    }
}
