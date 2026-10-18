<?php

declare(strict_types=1);

namespace BabelQueue\Queue;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\ShouldQueuePolyglot;
use BabelQueue\Exceptions\BabelQueueException;
use BabelQueue\Queue\Jobs\BabelQueueSqsJob;
use BabelQueue\Support\SchemaVersionGate;
use Illuminate\Queue\SqsQueue;

/**
 * An Amazon SQS queue that speaks the BabelQueue polyglot protocol — both ways.
 *
 * Produce: jobs implementing {@see ShouldQueuePolyglot} are re-encoded as the
 * strict { job, trace_id, data, meta, attempts } JSON envelope and sent with the
 * native SQS MessageAttributes projection of §3 of the broker-bindings contract
 * (bq-job/bq-trace-id/bq-message-id/bq-schema-version/bq-source-lang/bq-created-at),
 * so a Go/Python/... consumer can route on bq-job and trace on bq-trace-id without
 * decoding the body. Standard Laravel jobs pass straight through to the parent
 * {@see SqsQueue} (no attributes), so this stays a drop-in replacement.
 *
 * Consume: pop() performs the same visibility-timeout receive as the parent, but also
 * requests the message attributes, version-gates on `bq-schema-version` (§3.7) and
 * wraps the reserved message as a {@see BabelQueueSqsJob}, which parses the URN
 * envelope and routes it to a PHP handler.
 */
class BabelQueueSqsQueue extends SqsQueue
{
    /** The SQS `DelaySeconds` ceiling (§3.4). */
    public const MAX_DELAY_SECONDS = 900;

    /**
     * @param  \BabelQueue\Contracts\ShouldQueuePolyglot|object|string  $job
     * @param  mixed  $data
     * @param  string|null  $queue
     * @return mixed
     */
    public function push($job, $data = '', $queue = null)
    {
        if ($job instanceof ShouldQueuePolyglot) {
            return $this->pushPolyglot($job, $queue);
        }

        return parent::push($job, $data, $queue);
    }

    /**
     * Push a job after a delay. A polyglot job is sent as the strict envelope with `DelaySeconds`
     * (§3.4); a delay SQS cannot honour is rejected with a {@see BabelQueueException} and never
     * silently dropped or truncated: more than {@see self::MAX_DELAY_SECONDS} seconds on any queue, or
     * any per-message delay on a FIFO queue (`.fifo`, which supports only a queue-level delay).
     * Standard Laravel jobs pass straight through to the parent.
     *
     * @param  \DateTimeInterface|\DateInterval|int  $delay
     * @param  \BabelQueue\Contracts\ShouldQueuePolyglot|object|string  $job
     * @param  mixed  $data
     * @param  string|null  $queue
     * @return mixed
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        if (! $job instanceof ShouldQueuePolyglot) {
            return parent::later($delay, $job, $data, $queue);
        }

        $seconds = $this->secondsUntil($delay);
        $resolved = $queue ?? $this->default;

        if ($seconds > self::MAX_DELAY_SECONDS) {
            throw new BabelQueueException(sprintf(
                'SQS cannot delay a message by %d seconds: DelaySeconds is capped at %d (broker-bindings.md §3.4).',
                $seconds,
                self::MAX_DELAY_SECONDS,
            ));
        }
        if ($seconds > 0 && str_ends_with($this->getQueue($resolved), '.fifo')) {
            throw new BabelQueueException(
                'SQS FIFO queues do not support a per-message delay; configure a queue-level DelaySeconds instead (broker-bindings.md §3.4).',
            );
        }

        $payload = EnvelopeCodec::fromJob($job, $resolved);

        $this->pushRaw(EnvelopeCodec::encode($payload), $queue, ['DelaySeconds' => $seconds]);

        return $payload['meta']['id'];
    }

    /**
     * Send a raw payload, adding the §3 MessageAttributes when the payload is a
     * conformant BabelQueue envelope. Standard Laravel payloads get none. Delayed
     * jobs flow through here too (the parent's later() calls pushRaw).
     *
     * @param  string  $payload
     * @param  string|null  $queue
     * @param  array<string, mixed>  $options
     * @return mixed
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        if (! array_key_exists('MessageAttributes', $options)) {
            $attributes = $this->messageAttributes($payload);
            if ($attributes !== []) {
                $options['MessageAttributes'] = $attributes;
            }
        }

        return parent::pushRaw($payload, $queue, $options);
    }

    /**
     * Pop the next job, wrapped as a polyglot consume job.
     *
     * The receive is the driver's own (not {@see SqsQueue::pop()}, which asks SQS for no message
     * attributes): it requests `MessageSystemAttributeNames = ['ApproximateReceiveCount']` (the
     * attempt counter — a message *system* attribute, not a queue attribute) and
     * `MessageAttributeNames = ['All']` so the §3.5 `bq-*` attributes come
     * back, and applies the §3.7 `bq-schema-version` gate before the body is decoded. A rejected
     * message still becomes a {@see BabelQueueSqsJob} (carrying the rejected version), so the
     * {@see \BabelQueue\Consumer\BabelQueueDispatcher} quarantines it on the DLQ (or fails it) via
     * its unsupported-schema-version path instead of handing it to a handler.
     *
     * @param  string|null  $queue
     */
    public function pop($queue = null)
    {
        $response = $this->sqs->receiveMessage([
            'QueueUrl' => $queue = $this->getQueue($queue),
            'MessageSystemAttributeNames' => ['ApproximateReceiveCount'],
            'MessageAttributeNames' => ['All'],
        ]);

        if (is_null($response['Messages']) || count($response['Messages']) === 0) {
            return null;
        }

        $message = $response['Messages'][0];

        $attribute = $message['MessageAttributes']['bq-schema-version']['StringValue'] ?? null;

        return new BabelQueueSqsJob(
            $this->container,
            $this->sqs,
            $message,
            $this->connectionName,
            $queue,
            SchemaVersionGate::rejected($attribute),
        );
    }

    /**
     * Encode a polyglot job as the canonical envelope and send it; returns meta.id.
     */
    protected function pushPolyglot(ShouldQueuePolyglot $job, ?string $queue): string
    {
        $payload = EnvelopeCodec::fromJob($job, $queue ?? $this->default);

        $this->pushRaw(EnvelopeCodec::encode($payload), $queue);

        return $payload['meta']['id'];
    }

    /**
     * Project a conformant envelope's contract fields onto SQS MessageAttributes;
     * a standard / non-conformant payload yields none (drop-in passthrough, and
     * never a class name on the wire).
     *
     * @return array<string, array{DataType: string, StringValue: string}>
     */
    private function messageAttributes(string $payload): array
    {
        $envelope = EnvelopeCodec::decode($payload);
        if (! EnvelopeCodec::accepts($envelope)) {
            return [];
        }

        $meta = is_array($envelope['meta'] ?? null) ? $envelope['meta'] : [];

        $attributes = ['bq-job' => self::string(EnvelopeCodec::urn($envelope))];

        $traceId = $envelope['trace_id'] ?? null;
        if (is_string($traceId) && $traceId !== '') {
            $attributes['bq-trace-id'] = self::string($traceId);
        }
        if (isset($meta['id']) && is_scalar($meta['id'])) {
            $attributes['bq-message-id'] = self::string((string) $meta['id']);
        }
        if (isset($meta['schema_version']) && is_scalar($meta['schema_version'])) {
            $attributes['bq-schema-version'] = self::number((string) $meta['schema_version']);
        }
        if (isset($meta['lang']) && is_string($meta['lang']) && $meta['lang'] !== '') {
            $attributes['bq-source-lang'] = self::string($meta['lang']);
        }
        if (isset($meta['created_at']) && is_scalar($meta['created_at'])) {
            $attributes['bq-created-at'] = self::number((string) $meta['created_at']);
        }

        return $attributes;
    }

    /**
     * @return array{DataType: string, StringValue: string}
     */
    private static function string(string $value): array
    {
        return ['DataType' => 'String', 'StringValue' => $value];
    }

    /**
     * @return array{DataType: string, StringValue: string}
     */
    private static function number(string $value): array
    {
        return ['DataType' => 'Number', 'StringValue' => $value];
    }
}
