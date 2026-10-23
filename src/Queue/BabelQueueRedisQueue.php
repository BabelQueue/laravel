<?php

declare(strict_types=1);

namespace BabelQueue\Queue;

use BabelQueue\Contracts\ShouldQueuePolyglot;
use BabelQueue\Queue\Jobs\BabelQueueJob;
use BabelQueue\Codec\EnvelopeCodec;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Queue\Jobs\RedisJob;
use Illuminate\Queue\RedisQueue;
use Illuminate\Redis\Connections\PhpRedisConnection;

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
 *
 * Reservation: the reserve step runs {@see self::popScript()}, a `pcall`-hardened equivalent of
 * Laravel's pop Lua (ADR-0033). For a valid envelope it is byte-for-byte what the stock script
 * does; for a body that does not decode, or whose `attempts` is not a number, the stock script
 * aborts *after* the LPOP and the message is lost. Here the raw element is reserved as-is and
 * handed to the job, which takes the failed / dead-letter path and is removed from `:reserved`.
 * The standard `redis` connection is untouched (GR-6): only `babelqueue-*` connections use it.
 */
class BabelQueueRedisQueue extends RedisQueue
{
    /**
     * The `[raw, reserved]` pair of the last successful reserve, see {@see pop()}.
     *
     * @var array{0: mixed, 1: mixed}|null
     */
    private ?array $lastReservation = null;

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
        $this->lastReservation = null;

        $reserved = parent::pop($queue, $index);
        $reservation = $this->takeLastReservation();

        // Laravel decides "something was reserved" with a truthiness test, which drops a reserved member
        // that PHP calls falsy (the empty string and "0"): the element would sit in `:reserved` unseen until
        // its deadline, then return forever. The reservation recorded by retrieveNextJob() recovers it, so
        // such a body reaches the handler as poison like any other undecodable element.
        if ($reserved === null && $reservation !== null) {
            [$rawBody, $reservedMember] = $reservation;

            // Same bookkeeping as Laravel's pop() for a job taken from a secondary queue.
            if ($index > 0) {
                $this->secondaryQueueHadJob = true;
            }

            return new BabelQueueJob(
                $this->container,
                $this,
                (string) $rawBody,
                (string) $reservedMember,
                $this->connectionName,
                $queue ?: $this->default,
            );
        }

        // The parent always reserves a RedisJob; anything else we cannot wrap.
        if (! $reserved instanceof RedisJob) {
            return $reserved;
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
     * Retrieve the next job from the queue, reserving it with the hardened pop script.
     *
     * Mirrors {@see RedisQueue::retrieveNextJob()} (Laravel 11/12) apart from the script.
     *
     * @param  string  $queue
     * @param  bool  $block
     * @return array{0: mixed, 1: mixed}
     */
    protected function retrieveNextJob($queue, $block = true)
    {
        $nextJob = $this->evalScript(
            self::popScript(), 3, $queue, $queue.':reserved', $queue.':notify',
            $this->availableAt($this->retryAfter)
        );

        if (empty($nextJob)) {
            return [null, null];
        }

        [$job, $reserved] = $nextJob;

        if ($reserved !== null && $reserved !== false) {
            $this->lastReservation = [$job, $reserved];
        }

        // Strict test: an empty-string element is a (poison) job, not an empty queue.
        if (($job === null || $job === false) && ! is_null($this->blockFor) && $block &&
            $this->getConnection()->blpop([$queue.':notify'], $this->blockFor)) {
            return $this->retrieveNextJob($queue, false);
        }

        return [$job, $reserved];
    }

    /**
     * The reserve Lua: `KEYS = [queue, queue:reserved, queue:notify]`, `ARGV = [deadline]`.
     *
     * Same as Laravel's pop script when the element decodes to an object with a numeric
     * `attempts`; otherwise the raw element is reserved unchanged and returned as `{job, job}`.
     */
    public static function popScript(): string
    {
        return <<<'LUA'
-- Pop the first job off of the queue...
local job = redis.call('lpop', KEYS[1])
local reserved = false

if(job ~= false) then
    -- Not a decodable envelope (or one cjson cannot re-encode, e.g. an out-of-range number):
    -- reserve the raw element so it is not lost.
    reserved = job
    local ok, decoded = pcall(cjson.decode, job)
    if ok and type(decoded) == 'table' and type(decoded['attempts']) == 'number' then
        -- Increment the attempt count and place job on the reserved queue...
        decoded['attempts'] = decoded['attempts'] + 1
        local encoded_ok, encoded = pcall(cjson.encode, decoded)
        if encoded_ok then
            reserved = encoded
        end
    end
    redis.call('zadd', KEYS[2], ARGV[1], reserved)
    redis.call('lpop', KEYS[3])
end

return {job, reserved}
LUA;
    }

    /**
     * The "available at" score for a delay: `floor(now) + ceil(delay)` in whole epoch seconds
     * (broker-bindings.md section 1.1; conformance `redis_layout.delayed_score` / `redis_delay`).
     *
     * Laravel's stock implementation adds the delay to a Carbon instance and truncates, which for a
     * fractional delay lands up to one second *before* the contract score. Numeric delays are
     * therefore rounded up here (`later()` and release both go through this method); a `DateTimeInterface` /
     * `DateInterval` keeps Laravel's behaviour.
     *
     * @param  \DateTimeInterface|\DateInterval|int|float  $delay
     */
    protected function availableAt($delay = 0)
    {
        if (is_int($delay) || is_float($delay)) {
            return $this->currentTime() + (int) ceil($delay);
        }

        return parent::availableAt($delay);
    }

    /**
     * Remove the reserved job and put it back on the delayed set, `attempts` incremented.
     *
     * Laravel releases the *reserved member* (a Lua `cjson` re-encoding of the body), and cjson turns
     * an empty array / object into `{}` and writes large integers in exponent form, which would damage
     * a foreign producer's `data` (ADR-0031: unknown keys, their types and empty containers survive a
     * retry). The released body is therefore the original raw body with only the `attempts` literal
     * changed; the reserved member is used solely to ZREM. A body that cannot be rebuilt (it never
     * passed the reserve script's numeric `attempts` test) falls back to the stock release.
     *
     * @param  string  $queue
     * @param  \Illuminate\Queue\Jobs\RedisJob  $job
     * @param  \DateTimeInterface|\DateInterval|int|float  $delay
     * @return void
     */
    public function deleteAndRelease($queue, $job, $delay)
    {
        $released = self::releasedBody($job->getRawBody());

        if ($released === null) {
            parent::deleteAndRelease($queue, $job, $delay);

            return;
        }

        $queue = $this->getQueue($queue);

        $this->evalScript(
            self::releaseScript(), 2, $queue.':delayed', $queue.':reserved',
            $job->getReservedJob(), $this->availableAt($delay), $released
        );
    }

    /**
     * Run a Lua script on the queue's connection.
     *
     * phpredis takes `eval($script, $args, $numKeys)` while Predis takes `eval($script, $numKeys, ...$args)`;
     * the Laravel connection wrapper hides that for phpredis only, so the two drivers are told apart here
     * rather than relying on the wrapper's magic call.
     *
     * @return mixed
     */
    private function evalScript(string $script, int $numKeys, string|int ...$arguments)
    {
        $connection = $this->getConnection();

        if ($connection instanceof PhpRedisConnection) {
            return $connection->eval($script, $numKeys, ...$arguments);
        }

        return $connection->command('eval', [$script, $numKeys, ...$arguments]);
    }

    /**
     * The reservation recorded by {@see retrieveNextJob()} during the last pop, cleared on read.
     *
     * @return array{0: mixed, 1: mixed}|null
     */
    private function takeLastReservation(): ?array
    {
        $reservation = $this->lastReservation;
        $this->lastReservation = null;

        return $reservation;
    }

    /**
     * The raw body with `attempts` incremented by one and every other byte untouched, or null
     * when the body is not an object with a numeric `attempts`.
     *
     * Only the literal of the last top-level `attempts` member (the one a JSON parser keeps) is
     * rewritten in place, so nothing a decode/encode round-trip would damage can change: integers
     * beyond int64, float spelling, escapes, key order and empty containers all survive a retry.
     */
    public static function releasedBody(string $rawBody): ?string
    {
        try {
            $decoded = json_decode($rawBody, false, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            return null;
        }

        if (! $decoded instanceof \stdClass || ! (is_int($decoded->attempts ?? null) || is_float($decoded->attempts ?? null))) {
            return null;
        }

        $span = self::lastTopLevelAttemptsSpan($rawBody);

        if ($span === null) {
            return null;
        }

        [$start, $end] = $span;
        $literal = substr($rawBody, $start, $end - $start);

        if (preg_match('/^-?\d+$/', $literal) === 1) {
            $incremented = is_int($decoded->attempts) ? (string) ($decoded->attempts + 1) : null;
        } else {
            $incremented = json_encode($decoded->attempts + 1, JSON_PRESERVE_ZERO_FRACTION);
        }

        if ($incremented === null || $incremented === false) {
            return null;
        }

        return substr($rawBody, 0, $start).$incremented.substr($rawBody, $end);
    }

    /**
     * Byte offsets `[start, end)` of the value of the last top-level `attempts` member of a JSON object
     * that already decoded successfully, or null when there is none.
     *
     * @return array{0: int, 1: int}|null
     */
    private static function lastTopLevelAttemptsSpan(string $json): ?array
    {
        $length = strlen($json);
        $pos = strspn($json, " \t\r\n");

        if (($json[$pos] ?? '') !== '{') {
            return null;
        }

        $pos++;
        $span = null;

        while ($pos < $length) {
            $pos += strspn($json, " \t\r\n,", $pos);

            if (($json[$pos] ?? '') === '}') {
                break;
            }

            if (($json[$pos] ?? '') !== '"') {
                return null;
            }

            $keyEnd = self::stringEnd($json, $pos);
            $key = json_decode(substr($json, $pos, $keyEnd - $pos));
            $pos = $keyEnd;
            $pos += strspn($json, " \t\r\n", $pos);

            if (($json[$pos] ?? '') !== ':') {
                return null;
            }

            $pos++;
            $pos += strspn($json, " \t\r\n", $pos);
            $start = $pos;
            $pos = self::valueEnd($json, $pos);

            if ($key === 'attempts') {
                $span = [$start, $pos];
            }
        }

        return $span;
    }

    /** Offset just past the closing quote of the JSON string starting at `$pos`. */
    private static function stringEnd(string $json, int $pos): int
    {
        $length = strlen($json);

        for ($i = $pos + 1; $i < $length; $i++) {
            if ($json[$i] === '\\') {
                $i++;
            } elseif ($json[$i] === '"') {
                return $i + 1;
            }
        }

        return $length;
    }

    /** Offset just past the JSON value starting at `$pos`. */
    private static function valueEnd(string $json, int $pos): int
    {
        $length = strlen($json);
        $first = $json[$pos] ?? '';

        if ($first === '"') {
            return self::stringEnd($json, $pos);
        }

        if ($first === '{' || $first === '[') {
            $depth = 0;

            for ($i = $pos; $i < $length; $i++) {
                $char = $json[$i];

                if ($char === '"') {
                    $i = self::stringEnd($json, $i) - 1;
                } elseif ($char === '{' || $char === '[') {
                    $depth++;
                } elseif ($char === '}' || $char === ']') {
                    if (--$depth === 0) {
                        return $i + 1;
                    }
                }
            }

            return $length;
        }

        return $pos + strcspn($json, ",}] \t\r\n", $pos);
    }

    /**
     * The release Lua: `KEYS = [queue:delayed, queue:reserved]`, `ARGV = [reservedMember, score, newBody]`.
     */
    public static function releaseScript(): string
    {
        return <<<'LUA'
-- Remove the job from the reserved set...
redis.call('zrem', KEYS[2], ARGV[1])

-- Add the (attempts-incremented) body onto the "delayed" set...
redis.call('zadd', KEYS[1], ARGV[2], ARGV[3])

return true
LUA;
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
