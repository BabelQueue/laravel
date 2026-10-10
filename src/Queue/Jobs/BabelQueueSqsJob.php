<?php

declare(strict_types=1);

namespace BabelQueue\Queue\Jobs;

use Aws\Sqs\SqsClient;
use BabelQueue\Contracts\PolyglotMessage;
use BabelQueue\Queue\Concerns\ParsesPolyglotEnvelope;
use Illuminate\Queue\Jobs\SqsJob;

/**
 * An SQS-backed consume job for polyglot messages.
 *
 * It keeps all of {@see SqsJob}'s reservation/ack/release machinery (so retry
 * semantics and the visibility-timeout / ApproximateReceiveCount attempt counting
 * work exactly as in stock Laravel) and layers on top:
 *
 *   - URN / data / meta parsing of the raw JSON body (via the shared trait);
 *   - fire()/failed() routed through the {@see \BabelQueue\Consumer\BabelQueueDispatcher}
 *     instead of resolving payload['job'] as a PHP class.
 *
 * A message rejected by the §3.7 `bq-schema-version` gate is built with the rejected
 * version: its body is never decoded, and it presents a stub envelope (a reserved
 * sentinel URN and the rejected, non-integer `meta.schema_version`) so the dispatcher
 * takes its unsupported-schema-version path (DLQ quarantine or fail) and no handler runs.
 */
class BabelQueueSqsJob extends SqsJob implements PolyglotMessage
{
    use ParsesPolyglotEnvelope {
        envelope as private decodeEnvelope;
    }

    /**
     * Reserved routing URN prefix for a message rejected by the §3.7 version gate; the rejected
     * `bq-schema-version` value is appended. No handler is ever mapped to it.
     */
    public const UNSUPPORTED_SCHEMA_URN_PREFIX = 'babelqueue:unsupported-schema-version:';

    /**
     * @param  \Illuminate\Container\Container  $container
     * @param  array<string, mixed>  $job  the raw SQS message
     * @param  string  $connectionName
     * @param  string  $queue
     * @param  string|null  $rejectedSchemaVersion  the `bq-schema-version` attribute value when the
     *                                              §3.7 gate rejected the message, else null
     */
    public function __construct(
        $container,
        SqsClient $sqs,
        array $job,
        $connectionName,
        $queue,
        private readonly ?string $rejectedSchemaVersion = null,
    ) {
        parent::__construct($container, $sqs, $job, $connectionName, $queue);
    }

    /**
     * The decoded envelope, or — for a gate-rejected message — a stub that never touches the body.
     *
     * @return array<string, mixed>
     */
    protected function envelope(): array
    {
        if ($this->rejectedSchemaVersion !== null) {
            return [
                'job' => self::UNSUPPORTED_SCHEMA_URN_PREFIX . $this->rejectedSchemaVersion,
                'meta' => ['schema_version' => $this->rejectedSchemaVersion],
            ];
        }

        return $this->decodeEnvelope();
    }

    /**
     * Prefer the polyglot meta.id as the job identifier, falling back to the
     * SQS-assigned id the parent reads.
     */
    public function getJobId()
    {
        return $this->getMeta()['id'] ?? parent::getJobId();
    }
}
