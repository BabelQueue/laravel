<?php

declare(strict_types=1);

namespace BabelQueue\Consumer;

use BabelQueue\Consume\DeadLetterWriteFailed as CoreDeadLetterWriteFailed;
use Throwable;

/**
 * The dead-letter write itself failed (the DLQ broker is unreachable, the queue connection threw, ...).
 *
 * It is a distinct type so the Redis consume job can tell it from a handler failure: a handler exception
 * is released to `:delayed` for a retry, but a failed DLQ write must NOT acknowledge or release the
 * message (broker-bindings.md section 1.7, ADR-0036 decision 7). The reservation stays in `:reserved`,
 * where the `retry_after` recovery brings it back and the DLQ write is tried again.
 *
 * It extends the core's {@see CoreDeadLetterWriteFailed} (babelqueue/php-sdk 1.26), so code that handles the
 * core type handles this one too; the Laravel factory keeps the `reason` in the message.
 */
class DeadLetterWriteFailed extends CoreDeadLetterWriteFailed
{
    /**
     * `forQueue($queue, $reason, $previous)` names the dead-letter reason in the message; the core's
     * `forQueue($queue, $previous)` form is accepted too.
     */
    public static function forQueue(string $queue, string|Throwable $reason, ?Throwable $previous = null): self
    {
        if ($reason instanceof Throwable) {
            $previous = $reason;
            $reason = null;
        }

        if ($previous === null) {
            throw new \InvalidArgumentException('DeadLetterWriteFailed::forQueue() needs the exception that made the write fail.');
        }

        return new self(
            $reason === null
                ? sprintf('Writing the dead-letter record to [%s] failed: %s', $queue, $previous->getMessage())
                : sprintf('Writing the dead-letter record (reason "%s") to [%s] failed: %s', $reason, $queue, $previous->getMessage()),
            0,
            $previous,
        );
    }
}
