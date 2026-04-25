<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Transaction;

use Doctrine\DBAL\Connection;
use Vortos\Persistence\Transaction\UnitOfWorkInterface;

/**
 * DBAL implementation of UnitOfWorkInterface.
 *
 * Wraps Doctrine DBAL Connection with transaction management and
 * connection resilience for long-running PHP processes.
 *
 * ## Connection resilience
 *
 * Database connections go stale in long-running processes:
 *   - PostgreSQL default: tcp_keepalives_idle (varies by OS, often 2 hours)
 *   - MySQL default: wait_timeout = 8 hours
 *
 * FrankenPHP worker mode and Kafka consumers are long-running processes.
 * Without reconnect logic, the first request after an idle period throws
 * a "connection lost" error. ensureConnection() prevents this.
 *
 * ## DBAL 3.x note on reconnection
 *
 * DBAL 3.x removed Connection::ping(). The correct approach is:
 *   1. Try a lightweight query (SELECT 1)
 *   2. If it throws, call $connection->close()
 *   3. DBAL auto-reconnects on the next query after close()
 *
 * Do NOT call $connection->connect() directly — it is protected in DBAL 3.x.
 */
final class UnitOfWork implements UnitOfWorkInterface
{
    public function __construct(private Connection $connection) {}

    /**
     * Execute $work inside a transaction with connection resilience.
     *
     * Before opening the transaction, the connection is verified alive
     * and reconnected if stale. This prevents "MySQL server has gone away"
     * and equivalent PostgreSQL errors in long-running workers.
     *
     * {@inheritdoc}
     */
    public function run(callable $work): mixed
    {
        $this->ensureConnection();

        $this->connection->beginTransaction();

        try {
            $result = $work();
            $this->connection->commit();
            return $result;
        } catch (\Throwable $e) {
            if ($this->connection->isTransactionActive()) {
                $this->connection->rollBack();
            }
            throw $e;
        }
    }

    /**
     * {@inheritdoc}
     */
    public function isActive(): bool
    {
        return $this->connection->isTransactionActive();
    }

    /**
     * Verify the connection is alive. Reconnect if stale.
     *
     * Executes a lightweight query (SELECT 1) to test connectivity.
     * If it fails, calls close() to reset DBAL's internal connection state.
     * DBAL will automatically reconnect on the next query after close().
     *
     * This is called at the start of every run() — never externally.
     * The overhead is negligible: SELECT 1 takes ~0.1ms on a local connection.
     */
    private function ensureConnection(): void
    {
        try {
            $this->connection->executeQuery('SELECT 1');
        } catch (\Throwable) {
            $this->connection->close();
        }
    }
}
