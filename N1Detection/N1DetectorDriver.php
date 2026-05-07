<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\N1Detection;

use Doctrine\DBAL\Driver\Connection as DriverConnection;
use Doctrine\DBAL\Driver\Middleware\AbstractDriverMiddleware;

/** @internal Used only by N1DetectorMiddleware */
final class N1DetectorDriver extends AbstractDriverMiddleware
{
    public function __construct(
        \Doctrine\DBAL\Driver $wrappedDriver,
        private readonly N1QueryTracker $tracker,
    ) {
        parent::__construct($wrappedDriver);
    }

    public function connect(#[\SensitiveParameter] array $params): DriverConnection
    {
        return new N1DetectorConnection(parent::connect($params), $this->tracker);
    }
}
