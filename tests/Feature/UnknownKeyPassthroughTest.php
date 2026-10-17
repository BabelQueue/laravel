<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Feature;

use BabelQueue\Consumer\DeadLetterPublisher;
use BabelQueue\Contracts\PolyglotMessage;
use BabelQueue\Queue\BabelQueueRabbitQueue;
use BabelQueue\Queue\Concerns\ParsesPolyglotEnvelope;
use BabelQueue\Queue\Jobs\BabelQueueRabbitJob;
use BabelQueue\Tests\TestCase;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Factory as QueueFactory;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Jobs\Job;
use Mockery;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Message\AMQPMessage;
use RuntimeException;

/**
 * ADR-0031: a consumer that re-emits a message MUST keep the keys it does not know.
 *
 * A payload carrying an unknown top-level key, an unknown `meta` key (scalar, nested and
 * an empty object `{}`) goes through the two worker paths that re-emit a message:
 *
 *   - retry:  pop -> release() -> republish;
 *   - failed: DeadLetterPublisher (the worker's exhausted-retries path).
 */
final class UnknownKeyPassthroughTest extends TestCase
{
    private const BODY = '{"job":"urn:acme:orders:created","trace_id":"trace-1","data":{"order_id":7},'
        . '"meta":{"id":"m1","queue":"orders","lang":"go","schema_version":1,"created_at":1700000000,'
        . '"vendor_flag":true,"ext":{"acme":{"region":"eu"}},"empty_meta":{}},'
        . '"x_top":{"k":[1,2]},"x_empty":{},"attempts":1}';

    /** The same payload without the empty objects (JSON `{}`), which PHP arrays cannot tell from `[]`. */
    private const BODY_NO_EMPTY = '{"job":"urn:acme:orders:created","trace_id":"trace-1","data":{"order_id":7},'
        . '"meta":{"id":"m1","queue":"orders","lang":"go","schema_version":1,"created_at":1700000000,'
        . '"vendor_flag":true,"ext":{"acme":{"region":"eu"}}},'
        . '"x_top":{"k":[1,2]},"attempts":1}';

    public function test_unknown_keys_survive_a_release_republish(): void
    {
        $published = [];
        $channel = $this->channel();
        $channel->shouldReceive('basic_publish')->once()->andReturnUsing(
            static function (AMQPMessage $message) use (&$published): void {
                $published[] = $message;
            },
        );
        $channel->shouldReceive('basic_ack')->once()->with(1);

        $message = Mockery::mock(AMQPMessage::class);
        $message->shouldReceive('getBody')->andReturn(self::BODY);
        $message->shouldReceive('getChannel')->andReturn($channel);
        $message->shouldReceive('getDeliveryTag')->andReturn(1);
        $message->shouldReceive('has')->andReturn(false);
        $message->shouldReceive('get_properties')->andReturn([]);
        $channel->shouldReceive('basic_get')->once()->andReturn($message);

        $job = $this->makeQueue($channel)->pop('orders');
        self::assertInstanceOf(BabelQueueRabbitJob::class, $job);
        $job->release();

        self::assertCount(1, $published);
        self::assertEquals(self::parse(self::BODY), self::parse($published[0]->getBody()));
        self::assertStringContainsString('"x_empty":{}', $published[0]->getBody());
        self::assertStringContainsString('"empty_meta":{}', $published[0]->getBody());
    }

    public function test_unknown_keys_survive_the_failed_dead_letter_path(): void
    {
        $captured = $this->deadLetter(self::BODY_NO_EMPTY);
        $out = self::parse($captured);
        $in = self::parse(self::BODY_NO_EMPTY);

        // Everything the producer wrote is still there; the only addition is the dead_letter block.
        self::assertSame('failed', $out->dead_letter->reason);
        unset($out->dead_letter);
        self::assertEquals($in, $out);
    }

    /**
     * ADR-0031: an unknown key holding an empty object keeps its `{}` shape. DeadLetterPublisher
     * decodes the body to PHP arrays, where `{}` and `[]` are the same value.
     */
    public function test_empty_objects_in_unknown_keys_survive_the_failed_dead_letter_path(): void
    {
        $captured = $this->deadLetter(self::BODY);

        self::assertStringContainsString('"x_empty":{}', $captured);
        self::assertStringContainsString('"empty_meta":{}', $captured);
    }

    private function deadLetter(string $body): string
    {
        $captured = null;
        $queue = Mockery::mock(QueueContract::class);
        $queue->shouldReceive('pushRaw')->once()->withArgs(function (string $payload, string $q) use (&$captured): bool {
            $captured = $payload;

            return $q === 'orders.dlq';
        })->andReturn('x');

        $factory = Mockery::mock(QueueFactory::class);
        $factory->shouldReceive('connection')->once()->with('bq-redis')->andReturn($queue);

        $dlq = new DeadLetterPublisher($factory, ['enabled' => true]);
        $dlq->publish($this->message($body), 'failed', new RuntimeException('boom'));

        self::assertIsString($captured);

        return $captured;
    }

    private static function parse(string $json): \stdClass
    {
        /** @var \stdClass */
        return json_decode($json, false, 512, JSON_THROW_ON_ERROR);
    }

    private function message(string $body): PolyglotMessage
    {
        return new class($body, $this->app) extends Job implements PolyglotMessage {
            use ParsesPolyglotEnvelope;

            public function __construct(private string $body, Container $container)
            {
                $this->queue = 'orders';
                $this->connectionName = 'bq-redis';
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
                return 2;
            }
        };
    }

    private function makeQueue(AMQPChannel $channel): BabelQueueRabbitQueue
    {
        $connection = Mockery::mock(AbstractConnection::class);
        $connection->shouldReceive('isConnected')->andReturn(true);
        $connection->shouldReceive('channel')->andReturn($channel);
        $connection->shouldReceive('close')->andReturnNull();
        $connection->shouldReceive('isWriting')->andReturn(false);

        $queue = new BabelQueueRabbitQueue(static fn () => $connection, ['queue' => 'orders']);
        $queue->setContainer($this->app);
        $queue->setConnectionName('babelqueue-rabbitmq');

        return $queue;
    }

    private function channel(): Mockery\MockInterface
    {
        $channel = Mockery::mock(AMQPChannel::class);
        $channel->shouldReceive('is_open')->andReturn(true);
        $channel->shouldReceive('queue_declare')->andReturn(['orders', 0, 0]);
        $channel->shouldReceive('exchange_declare')->andReturnNull();
        $channel->shouldReceive('queue_bind')->andReturnNull();
        $channel->shouldReceive('close')->andReturnNull();

        return $channel;
    }
}
