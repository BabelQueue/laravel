<?php

declare(strict_types=1);

namespace BabelQueue\Queue\Connectors;

use Aws\Sqs\SqsClient;
use BabelQueue\Queue\BabelQueueSqsQueue;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\Connectors\SqsConnector;
use Illuminate\Support\Arr;

/**
 * Connector for the "babelqueue-sqs" queue driver.
 *
 * It mirrors {@see SqsConnector::connect()} (credential resolution, prefix/suffix,
 * after_commit) but hands back a {@see BabelQueueSqsQueue} instead of the stock
 * SqsQueue. The client construction and configuration are thus identical to
 * Laravel's native `sqs` driver — only the produced/consumed payload changes.
 */
class BabelQueueSqsConnector extends SqsConnector
{
    /**
     * @param  array<string, mixed>  $config
     */
    public function connect(array $config): Queue
    {
        $config = $this->getDefaultConfiguration($config);

        if ($credentials = $this->resolveCredentialProvider($config)) {
            $config['credentials'] = $credentials;
        } elseif (! empty($config['key']) && ! empty($config['secret'])) {
            $config['credentials'] = Arr::only($config, ['key', 'secret']);

            if (! empty($config['token'])) {
                $config['credentials']['token'] = $config['token'];
            }
        }

        return new BabelQueueSqsQueue(
            new SqsClient(Arr::except($config, ['token'])),
            $config['queue'],
            $config['prefix'] ?? '',
            $config['suffix'] ?? '',
            $config['after_commit'] ?? null,
        );
    }
}
