<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Tracing;

use Doctrine\DBAL\Driver\Middleware\AbstractStatementMiddleware;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement;
use Vortos\Tracing\Config\TracingModule;
use Vortos\Tracing\Contract\TracingInterface;

/**
 * Wraps a prepared DBAL Statement so that execute() is captured in a span.
 *
 * @internal Used only by TracingDbalConnection
 */
final class TracingDbalStatement extends AbstractStatementMiddleware
{
    public function __construct(
        Statement $wrappedStatement,
        private readonly TracingInterface $tracer,
        private readonly string $sql,
    ) {
        parent::__construct($wrappedStatement);
    }

    public function execute(): Result
    {
        $span = $this->tracer->startSpan('db.prepare.execute', [
            'db.statement'   => strlen($this->sql) > 512 ? substr($this->sql, 0, 509) . '...' : $this->sql,
            'db.system'      => 'sql',
            'db.prepared'    => true,
            'vortos.module'  => TracingModule::Persistence,
        ]);

        try {
            $result = parent::execute();
            $span->addAttribute('db.rows_affected', $result->rowCount());
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
}
