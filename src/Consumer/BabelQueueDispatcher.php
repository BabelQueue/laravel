<?php

declare(strict_types=1);

namespace BabelQueue\Consumer;

use BabelQueue\Contracts\PolyglotMessage;
use BabelQueue\Exceptions\UnknownUrnException;
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
     */
    public function __construct(
        private Container $container,
        private array $handlers = [],
        private string $onUnknownUrn = 'fail',
        private int $unknownUrnReleaseDelay = 0,
        private ?DeadLetterPublisher $deadLetter = null,
    ) {
    }

    /**
     * Resolve the handler for the message URN and run it. On success the
     * message is acknowledged (deleted) unless the handler already did so.
     *
     * @throws UnknownUrnException When no handler is mapped and the strategy is "fail".
     */
    public function dispatch(PolyglotMessage $message): void
    {
        $urn = $message->getUrn();

        $handlerClass = $urn === '' ? null : ($this->handlers[$urn] ?? null);

        if ($handlerClass === null) {
            $this->handleUnknownUrn($urn, $message);

            return;
        }

        $handler = $this->container->make($handlerClass);

        $this->container->call([$handler, 'handle'], [
            'data' => $message->getData(),
            'meta' => $message->getMeta(),
            'traceId' => $message->getTraceId(),
            'message' => $message,
            'job' => $message,
        ]);

        if (! $message->isDeletedOrReleased()) {
            $message->delete();
        }
    }

    /**
     * Forward a permanent failure to the handler's failed() hook, if it has one,
     * and route the message to the cross-language dead-letter queue (a no-op
     * unless DLQ is enabled). Called once the worker exhausts its retries.
     */
    public function fail(PolyglotMessage $message, ?Throwable $e): void
    {
        $this->deadLetter()?->publish($message, 'failed', $e);

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
