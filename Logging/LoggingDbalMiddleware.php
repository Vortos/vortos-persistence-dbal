<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Logging;

use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Driver\Middleware;
use Psr\Log\LoggerInterface;

/**
 * DBAL Middleware that logs slow queries and query errors.
 *
 * Registered in DbalPersistenceExtension alongside TracingDbalMiddleware.
 * Logs a WARNING for any query that exceeds the slow query threshold.
 * Logs an ERROR for any query that throws an exception.
 *
 * ## Configuration
 *
 * Threshold is set via the vortos.persistence.slow_query_threshold_ms parameter.
 * Default: 100ms. Set to 0 to log every query (dev only).
 *
 * ## What is logged
 *
 *   WARNING  Slow DBAL query detected  {sql, duration_ms, threshold_ms}
 *   ERROR    DBAL query failed          {sql, error}
 */
final class LoggingDbalMiddleware implements Middleware
{
    public function __construct(
        private readonly LoggerInterface $logger,
        private readonly int $slowQueryThresholdMs,
    ) {}

    public function wrap(Driver $driver): Driver
    {
        return new LoggingDbalDriver($driver, $this->logger, $this->slowQueryThresholdMs);
    }
}
