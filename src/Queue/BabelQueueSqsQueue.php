<?php

declare(strict_types=1);

namespace BabelQueue\Queue;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\ShouldQueuePolyglot;
use BabelQueue\Queue\Jobs\BabelQueueSqsJob;
use Illuminate\Queue\Jobs\SqsJob;
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
 * Consume: pop() reuses the parent's receive (visibility-timeout reservation) and
 * re-wraps the reserved {@see SqsJob} as a {@see BabelQueueSqsJob}, which parses
 * the URN envelope and routes it to a PHP handler.
 */
class BabelQueueSqsQueue extends SqsQueue
{
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
     * @param  string|null  $queue
     */
    public function pop($queue = null)
    {
        $reserved = parent::pop($queue);

        if (! $reserved instanceof SqsJob) {
            return $reserved;
        }

        return new BabelQueueSqsJob(
            $this->container,
            $this->sqs,
            $reserved->getSqsJob(),
            $this->connectionName,
            $reserved->getQueue(),
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
