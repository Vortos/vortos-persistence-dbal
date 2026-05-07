<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Tracing;

use Doctrine\DBAL\Driver\Connection as DriverConnection;
use Doctrine\DBAL\Driver\Middleware\AbstractDriverMiddleware;
use Vortos\Tracing\Contract\TracingInterface;

/**
 * Wraps the DBAL Driver to inject a tracing-aware Connection on connect.
 *
 * @internal Used only by TracingDbalMiddleware
 */
final class TracingDbalDriver extends AbstractDriverMiddleware
{
    public function __construct(
        \Doctrine\DBAL\Driver $wrappedDriver,
        private readonly TracingInterface $tracer,
    ) {
        parent::__construct($wrappedDriver);
    }

    public function connect(#[\SensitiveParameter] array $params): DriverConnection
    {
        return new TracingDbalConnection(parent::connect($params), $this->tracer);
    }
}
