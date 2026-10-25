<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Feature;

use BabelQueue\Consumer\BabelQueueDispatcher;
use BabelQueue\Tests\Support\ConformanceResults;
use PHPUnit\Framework\TestCase;

/**
 * The version-gate settle rule of ADR-0037 decision 1 (conformance 1.8.0, `version_gate_<binding>` sections).
 *
 * The Laravel `babelqueue-sqs` and Artemis drivers fall under the Laravel exception (ADR-0037 decision 4, GR-6): a
 * version-gated delivery is handled by `BabelQueueDispatcher::handleUnsupportedSchemaVersion()` (an envelope with a
 * `dead_letter` block and `reason: "poison"` when the DLQ is enabled, an `InvalidEnvelopeException` for the worker's
 * retry and then `failed_jobs` when it is disabled; `on_unknown_urn` is ignored), not by decision 1. Every case of
 * both sections is therefore reported as `skip` with the reason `ADR-0037 decision 4`: an explicit, ADR-backed skip,
 * not an omission and not a known gap. `version_gate_pulsar` and `version_gate_asb` are absent from the results:
 * this package carries neither binding.
 */
final class VersionGateConformanceTest extends TestCase
{
    private const REASON = 'ADR-0037 decision 4';

    public function test_the_sqs_and_artemis_drivers_report_every_version_gate_case_as_a_skip(): void
    {
        self::assertTrue(
            method_exists(BabelQueueDispatcher::class, 'handleUnsupportedSchemaVersion'),
            'the Laravel version-gate path this exception covers must still exist; if it is gone, run the sections instead of skipping them',
        );

        foreach (['version_gate_sqs', 'version_gate_artemis'] as $section) {
            ConformanceResults::skipAll($section, self::REASON);

            $ids = array_keys(ConformanceResults::cases($section));
            self::assertNotEmpty($ids, $section);
            foreach ($ids as $id) {
                $recorded = ConformanceResults::recorded()[$section][(string) $id] ?? null;
                self::assertSame('skip', $recorded['status'] ?? null, $section . '/' . $id);
                self::assertSame(self::REASON, $recorded['reason'] ?? null, $section . '/' . $id);
                self::assertArrayNotHasKey($section . '/' . $id, ConformanceResults::knownGaps(), 'a skipped case is not a known gap');
            }
        }
    }
}
