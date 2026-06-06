<?php

declare(strict_types=1);

namespace BabelQueue\Queue;

use BabelQueue\Contracts\ShouldQueuePolyglot;
use BabelQueue\Queue\Jobs\BabelQueueJob;
use BabelQueue\Codec\EnvelopeCodec;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Queue\RedisQueue;

/**
 * A Redis queue that speaks the BabelQueue polyglot protocol — both ways.
 *
 * Produce: jobs implementing {@see ShouldQueuePolyglot} are re-encoded as the
 * strict { job, data, meta, attempts } JSON envelope; standard Laravel jobs are
 * passed straight through to the parent, so this stays a drop-in replacement.
 *
 * Consume: pop() reuses the parent's atomic reservation and then wraps the
 * reserved job as a {@see BabelQueueJob}, which parses the URN envelope and
 * routes it to a PHP handler.
 */
class BabelQueueRedisQueue extends RedisQueue
{
    /**
     * Push a new job onto the queue.
     *
     * Note: the signature is deliberately left untyped to stay compatible with
     * {@see \Illuminate\Contracts\Queue\Queue::push()} across Laravel versions.
     *
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
     * Pop the next job off of the queue, wrapped as a polyglot consume job.
     *
     * The parent performs the atomic reserve (moving the job to the reserved
     * sorted set and incrementing attempts); we simply re-wrap the resulting
     * RedisJob as a {@see BabelQueueJob} so the URN dispatcher takes over in
     * fire(). Re-wrapping reuses the very same raw and reserved payloads, so all
     * delete/release bookkeeping continues to line up with the reserved set.
     *
     * @param  string|null  $queue
     * @param  int  $index
     */
    public function pop($queue = null, $index = 0): ?JobContract
    {
        $reserved = parent::pop($queue, $index);

        if ($reserved === null) {
            return null;
        }

        return new BabelQueueJob(
            $this->container,
            $this,
            $reserved->getRawBody(),
            $reserved->getReservedJob(),
            $this->connectionName,
            $reserved->getQueue(),
        );
    }

    /**
     * Encode a polyglot job as JSON and push the raw envelope onto Redis.
     *
     * @param  string|null  $queue  Logical queue name, or null for the default.
     * @return string The generated message id (meta.id).
     */
    protected function pushPolyglot(ShouldQueuePolyglot $job, ?string $queue): string
    {
        $payload = EnvelopeCodec::fromJob($job, $queue ?? $this->default);

        $this->pushRaw(EnvelopeCodec::encode($payload), $queue);

        return $payload['meta']['id'];
    }
}
