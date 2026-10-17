<?php

declare(strict_types=1);

namespace BabelQueue\Tests\Unit;

use BabelQueue\Support\EnvelopeBody;
use PHPUnit\Framework\TestCase;

final class EnvelopeBodyTest extends TestCase
{
    public function test_a_urn_only_body_is_re_emitted_with_job(): void
    {
        $out = json_decode(EnvelopeBody::normalize(
            '{"urn":"urn:acme:a","trace_id":"t","data":{"k":1},"meta":{"schema_version":1},"attempts":0}',
        ));

        self::assertSame('urn:acme:a', $out->job);
        self::assertObjectNotHasProperty('urn', $out);
    }

    public function test_empty_objects_and_unknown_keys_survive(): void
    {
        $body = EnvelopeBody::normalize(
            '{"job":"urn:acme:a","trace_id":"t","data":{},"meta":{"schema_version":1,"ext":{}},"extra":{},"attempts":0}',
        );

        self::assertStringContainsString('"data":{}', $body);
        self::assertStringContainsString('"ext":{}', $body);
        self::assertStringContainsString('"extra":{}', $body);
    }

    public function test_a_poison_body_is_returned_unchanged(): void
    {
        self::assertSame('not json', EnvelopeBody::normalize('not json'));
        self::assertSame('{"job":"urn:a","data":[1,2]}', EnvelopeBody::normalize('{"job":"urn:a","data":[1,2]}'));
    }
}
