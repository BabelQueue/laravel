<?php

declare(strict_types=1);

namespace BabelQueue\Facades;

use BabelQueue\Producer\Publisher;
use Illuminate\Support\Facades\Facade;

/**
 * Facade for the ergonomic producer API.
 *
 *     use BabelQueue\Facades\BabelQueue;
 *
 *     BabelQueue::publish('urn:babel:orders:created', ['order_id' => 1042]);
 *
 * Proxies to the singleton {@see Publisher}. This is sugar over the primary,
 * typed interface ({@see \BabelQueue\Contracts\ShouldQueuePolyglot}); both emit
 * the identical canonical envelope.
 *
 * @method static string publish(string $urn, array<string, mixed> $data, ?string $queue = null, ?string $traceId = null)
 * @method static string later(\DateTimeInterface|\DateInterval|int $delay, string $urn, array<string, mixed> $data, ?string $queue = null, ?string $traceId = null)
 * @method static \BabelQueue\Producer\Publisher onConnection(?string $connection)
 *
 * @see \BabelQueue\Producer\Publisher
 */
final class BabelQueue extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return Publisher::class;
    }
}
