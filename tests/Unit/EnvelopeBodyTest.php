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

    public function test_a_body_without_urn_is_returned_byte_for_byte(): void
    {
        $body = '{ "job" : "urn:acme:a", "trace_id":"t","data":{"filters":{},"big":12345678901234567890,"f":1.0,"s":"urn x \\u00e9"},'
            . '"meta":{"schema_version":1,"ext":{"acme":{}}},"extra":{},"attempts":0 }';

        self::assertSame($body, EnvelopeBody::normalize($body));
    }

    public function test_the_alias_is_replaced_in_place_and_every_other_value_stays_byte_identical(): void
    {
        $body = '{"trace_id":"t","urn":"urn:acme:a","data":{"filters":{},"big":12345678901234567890,"n":"urn"},"meta":{"ext":{"a":{}}},"extra":{},"attempts":0}';

        self::assertSame(
            '{"trace_id":"t","job":"urn:acme:a","data":{"filters":{},"big":12345678901234567890,"n":"urn"},"meta":{"ext":{"a":{}}},"extra":{},"attempts":0}',
            EnvelopeBody::normalize($body),
        );
    }

    public function test_urn_is_dropped_when_job_is_already_set(): void
    {
        self::assertSame(
            '{"job":"urn:acme:a","data":{}}',
            EnvelopeBody::normalize('{"job":"urn:acme:a","urn":"urn:acme:b","data":{}}'),
        );
    }

    public function test_a_blank_job_takes_the_urn_value(): void
    {
        self::assertSame(
            '{"job":"urn:acme:b","data":{"k":{}}}',
            EnvelopeBody::normalize('{"job":" \\t","urn":"urn:acme:b","data":{"k":{}}}'),
        );
        self::assertSame(
            '{"job":"urn:acme:b","data":{}}',
            EnvelopeBody::normalize('{"job":null,"urn":"urn:acme:b","data":{}}'),
        );
    }

    public function test_an_unusable_alias_leaves_the_body_untouched(): void
    {
        foreach ([
            '{"urn":"   ","data":{}}',
            '{"urn":5,"data":{}}',
            '{"job":"","urn":"","data":{}}',
            '{"job":"urn:a","job":"urn:b","urn":"urn:c"}',
            '["urn"]',
            '{"urn":"urn:a"',
        ] as $body) {
            self::assertSame($body, EnvelopeBody::normalize($body), $body);
        }
    }
}
