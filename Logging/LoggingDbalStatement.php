<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Logging;

use Doctrine\DBAL\Driver\Middleware\AbstractStatementMiddleware;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement;
use Psr\Log\LoggerInterface;

/**
 * @internal Used only by LoggingDbalConnection
 */
final class LoggingDbalStatement extends AbstractStatementMiddleware
{
    public function __construct(
        Statement $wrappedStatement,
        private readonly LoggerInterface $logger,
        private readonly int $thresholdMs,
        private readonly string $sql,
    ) {
        parent::__construct($wrappedStatement);
    }

    public function execute(): Result
    {
        $start = hrtime(true);
        try {
            return parent::execute();
        } catch (\Throwable $e) {
            $this->logger->error('DBAL prepared statement failed', [
                'sql'   => $this->truncate($this->sql),
                'error' => $e->getMessage(),
            ]);
            throw $e;
        } finally {
            $ms = (hrtime(true) - $start) / 1_000_000;
            if ($ms >= $this->thresholdMs) {
                $this->logger->warning('Slow DBAL prepared statement detected', [
                    'sql'          => $this->truncate($this->sql),
                    'duration_ms'  => round($ms, 2),
                    'threshold_ms' => $this->thresholdMs,
                ]);
            }
        }
    }

    private function truncate(string $sql): string
    {
        return strlen($sql) > 512 ? substr($sql, 0, 509) . '...' : $sql;
    }
}
