<?php

declare(strict_types=1);

return [

    /*
    |--------------------------------------------------------------------------
    | URN → Handler Map
    |--------------------------------------------------------------------------
    |
    | Maps an inbound message URN onto the PHP class that should handle it. The
    | URN (e.g. "urn:babel:orders:process") is the ONLY identity that travels
    | on the wire — a PHP class name never does — so producers (PHP, Go, Java...)
    | and consumers stay fully decoupled.
    |
    | The handler's handle() method is invoked through the service container, so
    | it may type-hint dependencies for injection and receive the parsed message:
    |
    |     public function handle(array $data, array $meta, string $traceId): void { ... }
    |
    | The following names are available to handle() by parameter name:
    |   $data     array   — the message "data" block
    |   $meta     array   — the message "meta" block
    |   $traceId  string  — the cross-service trace_id (correlation id)
    |   $message          — the BabelQueue job (for manual delete()/release())
    |
    */

    'handlers' => [
        // 'urn:babel:orders:process' => \App\Consumers\ProcessOrder::class,
        // 'urn:babel:notifications:email.welcome' => \App\Consumers\SendWelcomeEmail::class,
    ],

    /*
    |--------------------------------------------------------------------------
    | Unknown URN Strategy
    |--------------------------------------------------------------------------
    |
    | What to do when a message arrives whose URN is not in the map above:
    |
    |   'fail'        — throw (the worker retries, then moves it to failed_jobs)
    |   'delete'      — acknowledge and silently drop the message
    |   'release'     — put it back on the queue for later
    |   'dead_letter' — quarantine it on the dead-letter queue, then ack
    |                   (requires 'dead_letter.enabled' below; otherwise drops)
    |
    */

    'on_unknown_urn' => 'fail',

    // Delay, in seconds, applied when 'on_unknown_urn' is 'release'.
    'unknown_urn_release_delay' => 0,

    /*
    |--------------------------------------------------------------------------
    | Default Publisher Connection
    |--------------------------------------------------------------------------
    |
    | The queue connection BabelQueue\Facades\BabelQueue::publish() uses by
    | default. It MUST point at a "babelqueue-redis" / "babelqueue-rabbitmq"
    | connection in config/queue.php. null = the app's default queue connection.
    |
    */

    'connection' => env('BABELQUEUE_CONNECTION'),

    /*
    |--------------------------------------------------------------------------
    | Dead-Letter Queue (cross-language)
    |--------------------------------------------------------------------------
    |
    | When enabled, messages that fail permanently (retries exhausted) — and
    | unroutable messages when 'on_unknown_urn' is 'dead_letter' — are republished
    | onto a dead-letter queue as the SAME canonical envelope plus an additive,
    | optional top-level "dead_letter" block (reason, error, failed_at,
    | original_queue, attempts, lang). The DLQ is an ordinary queue, so any SDK
    | (Go, Python, ...) can consume and triage it. See ADR-0009.
    |
    |   enabled    — turn the cross-language DLQ on/off (off by default)
    |   connection — connection to publish the DLQ onto (null = the message's own)
    |   queue      — explicit DLQ name (null = original queue name + 'suffix')
    |   suffix     — appended to the original queue name when 'queue' is null
    |
    */

    'dead_letter' => [
        'enabled' => env('BABELQUEUE_DLQ_ENABLED', false),
        'connection' => null,
        'queue' => null,
        'suffix' => '.dlq',
    ],

];
