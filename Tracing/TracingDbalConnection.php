<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Tracing;

use Doctrine\DBAL\Driver\Connection as DriverConnection;
use Doctrine\DBAL\Driver\Middleware\AbstractConnectionMiddleware;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement;
use Vortos\Tracing\Config\TracingModule;
use Vortos\Tracing\Contract\TracingInterface;

/**
 * Wraps the low-level DBAL driver Connection to capture query spans.
 *
 * query() and exec() are intercepted directly.
 * prepare() is intercepted to wrap the returned Statement.
 *
 * @internal Used only by TracingDbalDriver
 */
final class TracingDbalConnection extends AbstractConnectionMiddleware
{
    public function __construct(
        DriverConnection $wrappedConnection,
        private readonly TracingInterface $tracer,
    ) {
        parent::__construct($wrappedConnection);
    }

    public function query(string $sql): Result
    {
        $span = $this->tracer->startSpan('db.query', [
            'db.statement'   => $this->truncateSql($sql),
            'db.system'      => 'sql',
            'vortos.module'  => TracingModule::Persistence,
        ]);

        try {
            $result = parent::query($sql);
            $span->setStatus('ok');
            return $result;
        } catch (\Throwable $e) {
            $span->recordException($e);
            $span->setStatus('error');
            throw $e;
        } finally {
            $span->end();
        }
    }

    public function exec(string $sql): int
    {
        $span = $this->tracer->startSpan('db.execute', [
            'db.statement'   => $this->truncateSql($sql),
            'db.system'      => 'sql',
            'vortos.module'  => TracingModule::Persistence,
        ]);

        try {
            $rowCount = parent::exec($sql);
            $span->addAttribute('db.rows_affected', $rowCount);
            $span->setStatus('ok');
            return $rowCount;
        } catch (\Throwable $e) {
            $span->recordException($e);
            $span->setStatus('error');
            throw $e;
        } finally {
            $span->end();
        }
    }

    public function prepare(string $sql): Statement
    {
        return new TracingDbalStatement(parent::prepare($sql), $this->tracer, $sql);
    }

    private function truncateSql(string $sql): string
    {
        // Avoid storing massive SQL in span attributes — truncate at 512 chars
        return strlen($sql) > 512 ? substr($sql, 0, 509) . '...' : $sql;
    }
}
