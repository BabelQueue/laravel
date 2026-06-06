<?php

declare(strict_types=1);

namespace BabelQueue\Tests;

use BabelQueue\BabelQueueServiceProvider;
use Orchestra\Testbench\TestCase as Orchestra;

abstract class TestCase extends Orchestra
{
    /**
     * @param  \Illuminate\Foundation\Application  $app
     * @return array<int, class-string>
     */
    protected function getPackageProviders($app): array
    {
        return [BabelQueueServiceProvider::class];
    }
}
