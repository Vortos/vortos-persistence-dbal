<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Logging;

use Doctrine\DBAL\Driver\Connection as DriverConnection;
use Doctrine\DBAL\Driver\Middleware\AbstractConnectionMiddleware;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement;
use Psr\Log\LoggerInterface;

/**
 * @internal Used only by LoggingDbalDriver
 */
final class LoggingDbalConnection extends AbstractConnectionMiddleware
{
    public function __construct(
        DriverConnection $wrappedConnection,
        private readonly LoggerInterface $logger,
        private readonly int $thresholdMs,
    ) {
        parent::__construct($wrappedConnection);
    }

    public function query(string $sql): Result
    {
        $start = hrtime(true);
        try {
            return parent::query($sql);
        } catch (\Throwable $e) {
            $this->logger->error('DBAL query failed', ['sql' => $this->truncate($sql), 'error' => $e->getMessage()]);
            throw $e;
        } finally {
            $this->logIfSlow($sql, $start);
        }
    }

    public function exec(string $sql): int
    {
        $start = hrtime(true);
        try {
            return parent::exec($sql);
        } catch (\Throwable $e) {
            $this->logger->error('DBAL query failed', ['sql' => $this->truncate($sql), 'error' => $e->getMessage()]);
            throw $e;
        } finally {
            $this->logIfSlow($sql, $start);
        }
    }

    public function prepare(string $sql): Statement
    {
        return new LoggingDbalStatement(parent::prepare($sql), $this->logger, $this->thresholdMs, $sql);
    }

    private function logIfSlow(string $sql, int $start): void
    {
        $ms = (hrtime(true) - $start) / 1_000_000;
        if ($ms >= $this->thresholdMs) {
            $this->logger->warning('Slow DBAL query detected', [
                'sql'          => $this->truncate($sql),
                'duration_ms'  => round($ms, 2),
                'threshold_ms' => $this->thresholdMs,
            ]);
        }
    }

    private function truncate(string $sql): string
    {
        return strlen($sql) > 512 ? substr($sql, 0, 509) . '...' : $sql;
    }
}
