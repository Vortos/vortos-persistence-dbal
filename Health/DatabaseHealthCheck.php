<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Health;

use Doctrine\DBAL\Connection;
use Vortos\Foundation\Health\Attribute\AsHealthCheck;
use Vortos\Foundation\Health\Contract\HealthCheckInterface;
use Vortos\Foundation\Health\HealthResult;

#[AsHealthCheck]
final class DatabaseHealthCheck implements HealthCheckInterface
{
    public function __construct(private readonly Connection $connection) {}

    public function name(): string
    {
        return 'database';
    }

    public function check(): HealthResult
    {
        $start = hrtime(true);

        try {
            $this->connection->executeQuery('SELECT 1');

            return new HealthResult($this->name(), true, $this->ms($start));
        } catch (\Throwable $e) {
            return new HealthResult($this->name(), false, $this->ms($start), $e->getMessage(), 'database_unreachable');
        }
    }

    private function ms(int $start): float
    {
        return round((hrtime(true) - $start) / 1_000_000, 2);
    }
}
