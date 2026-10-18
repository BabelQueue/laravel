<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Unit;

use BabelQueue\Tests\Support\ConformanceResults;
use BabelQueue\Tests\Support\SqsAttempts;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\AssertionFailedError;
use PHPUnit\Framework\TestCase;

/**
 * Tests of the known-gaps mechanism itself ({@see ConformanceResults::each()}), not of the SDK.
 *
 * Every test runs the real `delay` section of the vendored manifest inside
 * {@see ConformanceResults::isolated()} with a stand-in register that lists *every* case of the
 * section as a gap - the shape in which a runner that hides a crash behind "all of these are gaps"
 * would otherwise stay green. Nothing recorded here reaches the run's `results.json`.
 */
final class ConformanceMechanismTest extends TestCase
{
    private const SECTION = 'delay';

    public function test_an_assertion_failure_on_a_listed_gap_is_the_expected_failure(): void
    {
        $run = ConformanceResults::isolated($this->allListed(), static function (): void {
            ConformanceResults::each(self::SECTION, static function (): void {
                Assert::fail('the API is missing');
            });
        });

        self::assertNull($run['error'], 'an expected gap must not break the test');
        self::assertNotSame([], $run['results'][self::SECTION]);
        foreach ($run['results'][self::SECTION] as $id => $result) {
            self::assertSame('fail', $result['status'], $id . ' is recorded as an honest fail, never a skip');
            self::assertSame('the API is missing', $result['reason'] ?? null);
        }
    }

    public function test_an_unexpected_exception_on_a_listed_gap_breaks_the_test(): void
    {
        $run = ConformanceResults::isolated($this->allListed(), static function (): void {
            ConformanceResults::each(self::SECTION, static function (): void {
                throw new \TypeError('wrong argument');
            });
        });

        self::assertInstanceOf(\Throwable::class, $run['error'], 'a crash is not a known gap, even when every case is one');
        self::assertStringContainsString('unexpected TypeError: wrong argument', $run['error']->getMessage());
        foreach ($run['results'][self::SECTION] as $result) {
            self::assertSame('fail', $result['status']);
            self::assertStringContainsString('TypeError', $result['reason'] ?? '');
        }
    }

    public function test_a_passing_listed_gap_is_stale(): void
    {
        $run = ConformanceResults::isolated($this->allListed(), static function (): void {
            ConformanceResults::each(self::SECTION, static function (): void {
            });
        });

        self::assertInstanceOf(\Throwable::class, $run['error']);
        self::assertStringContainsString('stale gap', $run['error']->getMessage());
        self::assertStringContainsString('passes', $run['error']->getMessage());
    }

    public function test_a_skipped_listed_gap_is_stale(): void
    {
        $run = ConformanceResults::isolated($this->allListed(), static function (): void {
            ConformanceResults::each(self::SECTION, static function (): void {
                ConformanceResults::skipCase('ADR-0010: not applicable here');
            });
        });

        self::assertInstanceOf(\Throwable::class, $run['error']);
        self::assertStringContainsString('stale gap', $run['error']->getMessage());
        self::assertStringContainsString('is skipped', $run['error']->getMessage());
    }

    public function test_a_phpunit_skip_or_incomplete_on_a_listed_gap_breaks_the_test_instead_of_counting_as_a_gap(): void
    {
        // PHPUnit's skip/incomplete exceptions extend AssertionFailedError; without their own catch they
        // would be recorded as the "expected" listed fail and slip an un-cited skip through the gate.
        foreach (['skipped' => static fn () => Assert::markTestSkipped('runner bailed'), 'incomplete' => static fn () => Assert::markTestIncomplete('runner bailed')] as $kind => $bail) {
            $run = ConformanceResults::isolated($this->allListed(), static function () use ($bail): void {
                ConformanceResults::each(self::SECTION, static function () use ($bail): void {
                    $bail();
                });
            });

            self::assertInstanceOf(\Throwable::class, $run['error'], $kind . ' must not pass as a known gap');
            self::assertStringContainsString('unexpected', $run['error']->getMessage(), $kind);
            self::assertStringContainsString('runner bailed', $run['error']->getMessage(), $kind);
            self::assertStringContainsString('ConformanceResults::skipCase()', $run['error']->getMessage(), $kind);
        }
    }

    public function test_a_phpunit_skip_on_an_unlisted_case_breaks_the_test(): void
    {
        $run = ConformanceResults::isolated([], static function (): void {
            ConformanceResults::each(self::SECTION, static function (): void {
                Assert::markTestSkipped('runner bailed');
            });
        });

        self::assertInstanceOf(\Throwable::class, $run['error']);
        self::assertStringContainsString('runner bailed', $run['error']->getMessage());
        // Only the SkippedTest|IncompleteTest catch adds this hint; the generic catch would record the same fail.
        self::assertStringContainsString('ConformanceResults::skipCase()', $run['error']->getMessage());
        foreach ($run['results'][self::SECTION] as $result) {
            self::assertSame('fail', $result['status'], 'never recorded as a skip');
        }
    }

    public function test_attempts_runner_turns_only_the_missing_receive_count_crash_into_an_assertion(): void
    {
        // The exact warning Illuminate's SqsJob::attempts() raises when the attribute is missing.
        $crash = self::warningAsException(static fn () => (int) []['ApproximateReceiveCount']);

        try {
            SqsAttempts::read(static function () use ($crash): int {
                throw $crash;
            }, true);
            self::fail('the missing-count crash must surface');
        } catch (AssertionFailedError $e) {
            self::assertStringContainsString('attempts() crashed instead of falling back to body.attempts', $e->getMessage());
            self::assertStringContainsString('ApproximateReceiveCount', $e->getMessage());
        }

        self::assertSame(3, SqsAttempts::read(static fn (): int => 3, true), 'a clean read passes through');
    }

    public function test_attempts_runner_rethrows_any_other_error_exception(): void
    {
        $unrelated = self::warningAsException(static fn () => []['attempts']);
        $wrongCase = self::warningAsException(static fn () => []['ApproximateReceiveCount']);

        // Another undefined key on a case that has no count: a regression, not the known gap.
        // The same warning on a case that does carry a count: also not the known gap.
        foreach ([[$unrelated, true], [$wrongCase, false]] as $i => [$error, $absent]) {
            try {
                SqsAttempts::read(static function () use ($error): int {
                    throw $error;
                }, $absent);
                self::fail('case ' . $i . ' must not be swallowed');
            } catch (AssertionFailedError $e) {
                self::fail('case ' . $i . ' was converted into an assertion: ' . $e->getMessage());
            } catch (\ErrorException $e) {
                self::assertSame($error, $e, 'case ' . $i . ' is rethrown untouched');
            }
        }
    }

    public function test_a_rethrown_attempts_crash_breaks_a_section_that_lists_every_case_as_a_gap(): void
    {
        $unrelated = self::warningAsException(static fn () => []['attempts']);

        $run = ConformanceResults::isolated($this->allListed(), static function () use ($unrelated): void {
            ConformanceResults::each(self::SECTION, static function () use ($unrelated): void {
                SqsAttempts::read(static function () use ($unrelated): int {
                    throw $unrelated;
                }, true);
            });
        });

        self::assertInstanceOf(\Throwable::class, $run['error'], 'a regression must not hide behind a fully listed section');
        self::assertStringContainsString('unexpected ErrorException', $run['error']->getMessage());
    }

    private static function warningAsException(callable $trigger): \ErrorException
    {
        set_error_handler(static function (int $no, string $message, string $file, int $line): never {
            throw new \ErrorException($message, 0, $no, $file, $line);
        });

        try {
            $trigger();
        } catch (\ErrorException $e) {
            return $e;
        } finally {
            restore_error_handler();
        }

        throw new \LogicException('the trigger did not raise a warning');
    }

    public function test_an_unlisted_failure_breaks_the_test(): void
    {
        $ids = array_keys(ConformanceResults::cases(self::SECTION));
        $run = ConformanceResults::isolated([], static function (): void {
            ConformanceResults::each(self::SECTION, static function (): void {
                Assert::fail('not implemented');
            });
        });

        self::assertInstanceOf(\Throwable::class, $run['error']);
        foreach ($ids as $id) {
            self::assertStringContainsString(self::SECTION . '/' . $id . ': not implemented', $run['error']->getMessage());
        }
    }

    public function test_only_the_listed_cases_may_fail(): void
    {
        $ids = array_map('strval', array_keys(ConformanceResults::cases(self::SECTION)));
        self::assertGreaterThan(1, count($ids));
        $listed = $ids[0];

        $run = ConformanceResults::isolated([self::SECTION . '/' . $listed => 'ADR-0010: stand-in'], static function (): void {
            ConformanceResults::each(self::SECTION, static function (): void {
                Assert::fail('not implemented');
            });
        });

        self::assertInstanceOf(\Throwable::class, $run['error']);
        self::assertStringNotContainsString(self::SECTION . '/' . $listed . ':', $run['error']->getMessage(), 'the listed case is tolerated');
        self::assertStringContainsString(self::SECTION . '/' . $ids[1] . ':', $run['error']->getMessage(), 'its unlisted neighbour is not');
    }

    public function test_an_unlisted_skip_with_an_adr_is_fine_and_one_without_is_flagged(): void
    {
        $run = ConformanceResults::isolated([], static function (): void {
            ConformanceResults::each(self::SECTION, static function (): void {
                ConformanceResults::skipCase('ADR-0010: framework workers own this');
            });
        });
        self::assertNull($run['error']);
        self::assertSame([], $run['skipProblems']);

        $run = ConformanceResults::isolated([], static function (): void {
            ConformanceResults::skipAll(self::SECTION, 'no reason');
        });
        self::assertInstanceOf(\Throwable::class, $run['error'], 'a skip must cite an ADR-NNNN');
    }

    public function test_the_scratch_book_does_not_leak_into_the_run(): void
    {
        $before = ConformanceResults::recorded();
        $gaps = ConformanceResults::knownGaps();

        ConformanceResults::isolated($this->allListed(), static function (): void {
            ConformanceResults::each(self::SECTION, static function (): void {
                Assert::fail('x');
            });
        });

        self::assertSame($before, ConformanceResults::recorded());
        self::assertSame($gaps, ConformanceResults::knownGaps());
    }

    public function test_a_gap_reason_must_reference_an_adr_or_a_plan(): void
    {
        self::assertSame(1, preg_match(ConformanceResults::GAP_REFERENCE, 'see ADR-0010'));
        self::assertSame(1, preg_match(ConformanceResults::GAP_REFERENCE, 'follow-up: .ssot/plans/2026-10-17-r1-conformance/DEVAM.md'));
        self::assertSame(0, preg_match(ConformanceResults::GAP_REFERENCE, 'r1 follow-up plan'));
        self::assertSame(0, preg_match(ConformanceResults::GAP_REFERENCE, 'broker-bindings.md section 3.6'));

        // The real register loads only if every reason passes the same check.
        foreach (ConformanceResults::knownGaps() as $key => $reason) {
            self::assertSame(1, preg_match(ConformanceResults::GAP_REFERENCE, $reason), $key);
        }
    }

    /**
     * @return array<string, string> every case of the section, as a stand-in gap register
     */
    private function allListed(): array
    {
        $gaps = [];
        foreach (array_keys(ConformanceResults::cases(self::SECTION)) as $id) {
            $gaps[self::SECTION . '/' . $id] = 'ADR-0010: stand-in gap';
        }

        return $gaps;
    }
}
