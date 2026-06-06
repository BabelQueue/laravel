<?php

declare(strict_types=1);

namespace BabelQueue\Contracts;

use Illuminate\Contracts\Queue\Job;

/**
 * A Laravel queue job that wraps an inbound polyglot envelope on the consume side.
 *
 * It composes the framework-agnostic core {@see InboundMessage} decoded view
 * (URN, trace id, data, meta) with Laravel's {@see Job} — so the worker can
 * fire, delete, release and fail it. Both the Redis and RabbitMQ consume jobs
 * implement this, which lets the dispatcher stay transport-agnostic.
 */
interface PolyglotMessage extends InboundMessage, Job
{
}
