<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\N1Detection;

use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Driver\Middleware;

/**
 * DBAL Middleware entry point for N+1 query detection.
 *
 * Only registered in dev (kernel.env = dev) via N1DetectionCompilerPass.
 * Wraps every DBAL driver connection to intercept SQL before execution.
 */
final class N1DetectorMiddleware implements Middleware
{
    public function __construct(private readonly N1QueryTracker $tracker) {}

    public function wrap(Driver $driver): Driver
    {
        return new N1DetectorDriver($driver, $this->tracker);
    }
}
