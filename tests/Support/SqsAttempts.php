<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Support;

use PHPUnit\Framework\Assert;

/**
 * Reads `attempts()` for the §3.6 reconciliation runner, translating exactly one known crash into a
 * conformance assertion.
 *
 * Illuminate's `SqsJob::attempts()` reads `Attributes.ApproximateReceiveCount` unguarded, so a message that
 * carries no count raises an `ErrorException` instead of falling back to `body.attempts`. For a case that
 * supplies no count that is the *wrong answer* the case is about, hence an assertion (a listed gap can then
 * record it). Any other `ErrorException` - another undefined key, or the same warning on a case that does
 * carry a count - is a regression or a broken runner and is rethrown untouched, so it breaks the test even
 * when every case of the section is a listed gap.
 */
final class SqsAttempts
{
    /**
     * @param  \Closure(): int  $attempts  reads `$job->attempts()`
     * @param  bool  $receiveCountAbsent  the case delivers the message without `ApproximateReceiveCount`
     */
    public static function read(\Closure $attempts, bool $receiveCountAbsent): int
    {
        try {
            return $attempts();
        } catch (\ErrorException $e) {
            if ($receiveCountAbsent && str_contains($e->getMessage(), 'ApproximateReceiveCount')) {
                Assert::fail('attempts() crashed instead of falling back to body.attempts: ' . $e->getMessage());
            }

            throw $e;
        }
    }
}
