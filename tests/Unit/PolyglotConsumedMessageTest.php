<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Unit;

use BabelQueue\Consumer\PolyglotConsumedMessage;
use BabelQueue\Contracts\PolyglotMessage;
use BabelQueue\Queue\Concerns\ParsesPolyglotEnvelope;
use BabelQueue\Tests\TestCase;
use Illuminate\Container\Container;
use Illuminate\Queue\Jobs\Job;

/**
 * The bridge that adapts a Laravel {@see PolyglotMessage} to the php-sdk core
 * {@see \BabelQueue\Contracts\ConsumedMessage} view, so the idempotency helpers
 * (which type-hint ConsumedMessage) can read the canonical "meta.id" off a
 * Laravel delivery. Each test pins one delegating method so a regression in the
 * adapter surface — not just the dispatcher that consumes it — is caught here.
 */
final class PolyglotConsumedMessageTest extends TestCase
{
    public function test_it_delegates_the_envelope_view_to_the_underlying_job(): void
    {
        $adapter = new PolyglotConsumedMessage($this->polyglotMessage(
            '{"job":"urn:babel:orders:process","trace_id":"7b3f9c2a-e41d-4f88","data":{"order_id":7},"meta":{"id":"m1"}}'
        ));

        $this->assertSame('urn:babel:orders:process', $adapter->getUrn());
        $this->assertSame('7b3f9c2a-e41d-4f88', $adapter->getTraceId());
        $this->assertSame(['order_id' => 7], $adapter->getData());
        $this->assertSame(['id' => 'm1'], $adapter->getMeta());
    }

    public function test_attempts_is_forwarded_and_coerced_to_int(): void
    {
        // The base Job::attempts() may hand back a non-int (e.g. a string from a
        // transport); the adapter contract is attempts(): int, so it must coerce.
        $adapter = new PolyglotConsumedMessage($this->polyglotMessage(
            '{"job":"urn:babel:orders:process","data":{},"meta":{"id":"m2"}}',
            attempts: '3',
        ));

        $value = $adapter->attempts();

        $this->assertSame(3, $value);
    }

    public function test_envelope_reassembles_the_canonical_wire_shape(): void
    {
        // envelope() is what the php-sdk runtime would re-publish to a DLQ; it must
        // round-trip the four canonical keys read off the underlying message.
        $adapter = new PolyglotConsumedMessage($this->polyglotMessage(
            '{"job":"urn:babel:orders:process","trace_id":"tr-9","data":{"sku":"A1"},"meta":{"id":"m3"}}'
        ));

        $this->assertSame([
            'job' => 'urn:babel:orders:process',
            'trace_id' => 'tr-9',
            'data' => ['sku' => 'A1'],
            'meta' => ['id' => 'm3'],
        ], $adapter->envelope());
    }

    public function test_a_legacy_envelope_with_no_trace_id_bridges_to_an_empty_string(): void
    {
        // A non-conformant producer that omits trace_id must not blow up the bridge:
        // getTraceId() returns '' and the envelope still carries the empty key.
        $adapter = new PolyglotConsumedMessage($this->polyglotMessage(
            '{"job":"urn:babel:orders:process","data":{},"meta":{}}'
        ));

        $this->assertSame('', $adapter->getTraceId());
        $this->assertSame('', $adapter->envelope()['trace_id']);
        $this->assertSame([], $adapter->getData());
        $this->assertSame([], $adapter->getMeta());
    }

    /**
     * A representative {@see PolyglotMessage} backed by a raw JSON body, built the
     * same way the Redis/RabbitMQ consume jobs are (Job + ParsesPolyglotEnvelope).
     *
     * @param  int|string  $attempts
     */
    private function polyglotMessage(string $body, $attempts = 1): PolyglotMessage
    {
        return new class($body, $attempts, $this->app) extends Job implements PolyglotMessage {
            use ParsesPolyglotEnvelope;

            /**
             * @param  int|string  $attemptsValue
             */
            public function __construct(
                private string $body,
                private $attemptsValue,
                Container $container,
            ) {
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
                return $this->attemptsValue;
            }
        };
    }
}
