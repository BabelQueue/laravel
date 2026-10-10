<?php

declare(strict_types=1);

namespace BabelQueue\Consumer;

use BabelQueue\Codec\EnvelopeCodec;
use BabelQueue\Contracts\ConsumedMessage;
use BabelQueue\Contracts\PolyglotMessage;
use BabelQueue\Exceptions\InvalidEnvelopeException;
use BabelQueue\Exceptions\UnknownUrnException;
use BabelQueue\Idempotency\ClaimingDispatch;
use BabelQueue\Idempotency\ClaimingStore;
use BabelQueue\Idempotency\Idempotent;
use BabelQueue\Idempotency\IdempotencyStore;
use BabelQueue\Validation\EnvelopeValidator;
use Illuminate\Contracts\Container\Container;
use Throwable;

/**
 * Routes a consumed polyglot message to the PHP class mapped to its URN and
 * invokes that class's handle() method.
 *
 * This is the single place where the wire identity (a URN) is translated into
 * a PHP type. Resolution and method invocation go through the container, so
 * handlers get full dependency injection and the producing service never needs
 * to share any PHP class with the consumer.
 */
final class BabelQueueDispatcher
{
    /**
     * @param  array<string, class-string>  $handlers  urn => handler class
     * @param  string  $onUnknownUrn  fail | delete | release | dead_letter
     * @param  IdempotencyStore|null  $idempotencyStore  When set, deliveries are deduped on
     *                                                    "meta.id" so a duplicate runs the handler
     *                                                    zero times (opt-in; null = unchanged).
     * @param  int  $idempotencyTtl  In-flight claim TTL (seconds) for a {@see ClaimingStore}.
     */
    public function __construct(
        private Container $container,
        private array $handlers = [],
        private string $onUnknownUrn = 'fail',
        private int $unknownUrnReleaseDelay = 0,
        private ?DeadLetterPublisher $deadLetter = null,
        private ?IdempotencyStore $idempotencyStore = null,
        private int $idempotencyTtl = ClaimingDispatch::DEFAULT_TTL,
    ) {
    }

    /**
     * Resolve the handler for the message URN and run it. On success the
     * message is acknowledged (deleted) unless the handler already did so.
     *
     * An envelope whose "meta.schema_version" is absent or not understood is never handed to a
     * handler (contract: consumers MUST reject or quarantine such versions), whatever
     * "on_unknown_urn" says: it is quarantined on the DLQ and acked when DLQ is enabled, else
     * rejected with an {@see InvalidEnvelopeException}. It is never silently dropped.
     *
     * @throws UnknownUrnException When no handler is mapped and the strategy is "fail".
     * @throws InvalidEnvelopeException When the schema version is unsupported and DLQ is not enabled.
     */
    public function dispatch(PolyglotMessage $message): void
    {
        $urn = $message->getUrn();

        // An empty URN (e.g. a non-JSON body) keeps its documented unknown-URN path
        // (error-handling.md section 7); every routable envelope is version-gated first.
        if ($urn !== '' && ! self::supportsSchemaVersion($message)) {
            $this->handleUnsupportedSchemaVersion($message);

            return;
        }

        $handlerClass = $urn === '' ? null : ($this->handlers[$urn] ?? null);

        if ($handlerClass === null) {
            $this->handleUnknownUrn($urn, $message);

            return;
        }

        $handler = $this->container->make($handlerClass);

        // The unit of work for this delivery: invoke the URN-mapped handler. When
        // idempotency is enabled this is what the php-sdk helper either runs once
        // (first delivery) or skips (a duplicate / a committed id).
        $run = function () use ($handler, $message): void {
            $this->container->call([$handler, 'handle'], [
                'data' => $message->getData(),
                'meta' => $message->getMeta(),
                'traceId' => $message->getTraceId(),
                'message' => $message,
                'job' => $message,
            ]);
        };

        $this->runIdempotent($message, $run);

        // Ack the delivery — whether the handler ran (first time) or was skipped as
        // a duplicate. A skipped duplicate must still be acked so the broker stops
        // redelivering it. A parked/in-flight claim throws before reaching here, so
        // it is NOT acked and the broker redelivers it later.
        if (! $message->isDeletedOrReleased()) {
            $message->delete();
        }
    }

    /**
     * Run the per-delivery unit of work, deduped on the envelope's "meta.id" when a store is
     * configured. Reuses the php-sdk helpers as the single source of dedup logic:
     *   - a {@see ClaimingStore} drives {@see ClaimingDispatch::wrap()} (atomic in-flight claim →
     *     park a concurrent duplicate via a thrown {@see \BabelQueue\Idempotency\ClaimParkedException});
     *   - any other {@see IdempotencyStore} drives {@see Idempotent::wrap()} (post-success dedupe).
     * With no store, the work runs unchanged.
     *
     * @param  callable(): void  $run
     */
    private function runIdempotent(PolyglotMessage $message, callable $run): void
    {
        if ($this->idempotencyStore === null) {
            $run();

            return;
        }

        // The helpers type-hint the core ConsumedMessage view and only read "meta.id" off it; the
        // adapter exposes the Laravel job's decoded envelope without touching its frozen contract.
        $consumed = new PolyglotConsumedMessage($message);
        $handler = static function (ConsumedMessage $ignored) use ($run): void {
            $run();
        };

        $wrapped = $this->idempotencyStore instanceof ClaimingStore
            ? ClaimingDispatch::wrap($this->idempotencyStore, $handler, $this->idempotencyTtl)
            : Idempotent::wrap($this->idempotencyStore, $handler);

        $wrapped($consumed);
    }

    /**
     * Forward a permanent failure to the handler's failed() hook, if it has one,
     * and route the message to the cross-language dead-letter queue (a no-op
     * unless DLQ is enabled). Called once the worker exhausts its retries.
     */
    public function fail(PolyglotMessage $message, ?Throwable $e): void
    {
        $this->deadLetter()?->publish($message, 'failed', $e);

        // An unsupported-version envelope never reached a handler, so there is no failed() hook to run.
        if (! self::supportsSchemaVersion($message)) {
            return;
        }

        $handlerClass = $this->handlers[$message->getUrn()] ?? null;

        if ($handlerClass === null) {
            return;
        }

        $handler = $this->container->make($handlerClass);

        if (! method_exists($handler, 'failed')) {
            return;
        }

        $this->container->call([$handler, 'failed'], [
            'data' => $message->getData(),
            'meta' => $message->getMeta(),
            'traceId' => $message->getTraceId(),
            'exception' => $e,
            'e' => $e,
            'message' => $message,
        ]);
    }

    /**
     * Whether the envelope may be processed: "meta.schema_version" MUST be present and equal the
     * one version this SDK understands. An absent version is unsupported, exactly as in the php-sdk
     * core ({@see EnvelopeValidator::check()}) and the other SDK cores.
     */
    private static function supportsSchemaVersion(PolyglotMessage $message): bool
    {
        return ($message->getMeta()['schema_version'] ?? null) === EnvelopeCodec::SCHEMA_VERSION;
    }

    /**
     * Reject (or quarantine) an envelope with an unsupported or absent "meta.schema_version".
     *
     * This is deliberately independent of "on_unknown_urn": that strategy governs URNs this
     * service has no handler for (error-handling.md section 3), and "delete"/"release" must not
     * be able to silently drop or loop a message whose URN may well be routable but whose
     * version we cannot read. Per error-handling.md section 7 the message is poison: it is
     * quarantined on the DLQ (reason "poison") and acked when DLQ is enabled; otherwise it is
     * rejected with a visible {@see InvalidEnvelopeException} so the worker's retry / failed_jobs
     * path records it. It is never deleted without a trace.
     */
    private function handleUnsupportedSchemaVersion(PolyglotMessage $message): void
    {
        $deadLetter = $this->deadLetter();

        if ($deadLetter !== null && $deadLetter->enabled()) {
            $deadLetter->publish($message, 'poison');
            $message->delete();

            return;
        }

        throw InvalidEnvelopeException::because(
            EnvelopeValidator::REASON_UNSUPPORTED_SCHEMA_VERSION,
            ['job' => $message->getUrn(), 'trace_id' => $message->getTraceId(), 'meta' => $message->getMeta()],
        );
    }

    /**
     * Apply the configured strategy for a URN with no mapped handler.
     */
    private function handleUnknownUrn(string $urn, PolyglotMessage $message): void
    {
        switch ($this->onUnknownUrn) {
            case 'delete':
                $message->delete();

                return;

            case 'release':
                $message->release($this->unknownUrnReleaseDelay);

                return;

            case 'dead_letter':
                // Quarantine the unroutable message on the DLQ, then ack it. If
                // DLQ is disabled the publish is a no-op and this degrades to a
                // silent delete.
                $this->deadLetter()?->publish($message, 'unknown_urn');
                $message->delete();

                return;

            case 'fail':
            default:
                throw new UnknownUrnException(sprintf(
                    'No handler is mapped for URN [%s]. Add it to the "handlers" map in config/babelqueue.php.',
                    $urn === '' ? '(empty)' : $urn,
                ));
        }
    }

    /**
     * Resolve the dead-letter publisher: the injected one, or one resolved from
     * the container if bound. Returns null when DLQ support is unavailable.
     */
    private function deadLetter(): ?DeadLetterPublisher
    {
        if ($this->deadLetter !== null) {
            return $this->deadLetter;
        }

        if ($this->container->bound(DeadLetterPublisher::class)) {
            return $this->deadLetter = $this->container->make(DeadLetterPublisher::class);
        }

        return null;
    }
}
