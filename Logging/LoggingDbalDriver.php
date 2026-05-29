<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Logging;

use Doctrine\DBAL\Driver\Connection as DriverConnection;
use Doctrine\DBAL\Driver\Middleware\AbstractDriverMiddleware;
use Psr\Log\LoggerInterface;

/**
 * @internal Used only by LoggingDbalMiddleware
 */
final class LoggingDbalDriver extends AbstractDriverMiddleware
{
    public function __construct(
        \Doctrine\DBAL\Driver $wrappedDriver,
        private readonly LoggerInterface $logger,
        private readonly int $slowQueryThresholdMs,
    ) {
        parent::__construct($wrappedDriver);
    }

    public function connect(array $params): DriverConnection
    {
        return new LoggingDbalConnection(
            parent::connect($params),
            $this->logger,
            $this->slowQueryThresholdMs,
        );
    }
}
