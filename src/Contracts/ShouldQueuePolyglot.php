<?php

declare(strict_types=1);

namespace BabelQueue\Contracts;

use Illuminate\Contracts\Queue\ShouldQueue;

/**
 * The Laravel-flavoured polyglot job contract.
 *
 * It composes the framework-agnostic core {@see PolyglotJob} (which provides the
 * URN via {@see HasBabelUrn::getBabelUrn()} and the payload via
 * `toPayload()`) with Laravel's {@see ShouldQueue}, so the familiar `dispatch()`
 * / `Dispatchable` pipeline keeps working. BabelQueue's queue driver detects this
 * contract and emits the canonical { job, trace_id, data, meta, attempts }
 * envelope — built by {@see \BabelQueue\Codec\EnvelopeCodec} — instead of a
 * serialized PHP object graph.
 *
 * Implementations MUST expose nothing but pure, JSON-encodable data from
 * `toPayload()`. Optionally implement {@see HasTraceId} to continue an existing
 * distributed trace.
 */
interface ShouldQueuePolyglot extends PolyglotJob, ShouldQueue
{
}
