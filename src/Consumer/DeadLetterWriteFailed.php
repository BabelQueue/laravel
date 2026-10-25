<?php

declare(strict_types=1);

namespace BabelQueue\Consumer;

use BabelQueue\Exceptions\BabelQueueException;
use Throwable;

/**
 * The dead-letter write itself failed (the DLQ broker is unreachable, the queue connection threw, ...).
 *
 * It is a distinct type so the Redis consume job can tell it from a handler failure: a handler exception
 * is released to `:delayed` for a retry, but a failed DLQ write must NOT acknowledge or release the
 * message (broker-bindings.md section 1.7, ADR-0036 decision 7). The reservation stays in `:reserved`,
 * where the `retry_after` recovery brings it back and the DLQ write is tried again.
 */
final class DeadLetterWriteFailed extends BabelQueueException
{
    public static function forQueue(string $queue, string $reason, Throwable $previous): self
    {
        return new self(
            sprintf('Writing the dead-letter record (reason "%s") to [%s] failed: %s', $reason, $queue, $previous->getMessage()),
            0,
            $previous,
        );
    }
}
