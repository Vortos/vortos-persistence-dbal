<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Tracing;

use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Driver\Middleware;
use Vortos\Tracing\Contract\TracingInterface;

/**
 * Doctrine DBAL Middleware that wraps every query in a tracing span.
 *
 * Registered in DbalPersistenceExtension via the DBAL Configuration middleware list.
 * Respects VortosTracingConfig::disable(TracingModule::Persistence) — when disabled,
 * ModuleAwareTracer returns a NoOpSpan so this middleware becomes zero-overhead.
 *
 * ## Span names
 *
 *   db.query   — SELECT and other read queries
 *   db.execute — INSERT/UPDATE/DELETE via executeStatement()
 *   db.prepare — prepared statement execution (covers parameterized queries)
 */
final class TracingDbalMiddleware implements Middleware
{
    public function __construct(private readonly TracingInterface $tracer) {}

    public function wrap(Driver $driver): Driver
    {
        return new TracingDbalDriver($driver, $this->tracer);
    }
}
