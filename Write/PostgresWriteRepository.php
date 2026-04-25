<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Write;

use Vortos\Domain\Aggregate\AggregateRoot;

/**
 * PostgreSQL-optimised write repository.
 *
 * Extends DbalWriteRepository with a PostgreSQL-specific batchUpdate()
 * implementation using UPDATE FROM VALUES — a single query that updates
 * all rows at once instead of one query per aggregate.
 *
 * ## When to use this instead of DbalWriteRepository
 *
 * Use PostgresWriteRepository when:
 *   - Your application runs on PostgreSQL (the default Vortos stack)
 *   - You have use cases that update large batches of aggregates at once
 *     (e.g. bulk status updates, competition result processing)
 *
 * Use DbalWriteRepository when:
 *   - You need database portability (MySQL, SQLite, SQL Server)
 *   - Your batch sizes are small (< 50 aggregates) — the difference is negligible
 *
 * ## All other methods are inherited from DbalWriteRepository
 *
 * Only batchUpdate() is overridden. findById(), save(), delete(),
 * batchInsert(), batchUpsert(), batchDelete() behave identically.
 */
abstract class PostgresWriteRepository extends DbalWriteRepository
{
    /**
     * Update multiple aggregates using PostgreSQL's UPDATE FROM VALUES syntax.
     *
     * Executes a single SQL statement regardless of how many aggregates are passed:
     *
     *   UPDATE users SET
     *       email = v.email,
     *       name  = v.name,
     *       version = users.version + 1
     *   FROM (VALUES
     *       ('id-1', 'a@example.com', 'Alice', 1),
     *       ('id-2', 'b@example.com', 'Bob',   2)
     *   ) AS v(id, email, name, version)
     *   WHERE users.id = v.id
     *   AND   users.version = v.version
     *
     * ## Optimistic locking
     *
     * The WHERE clause includes version = v.version — the expected version
     * from each aggregate. If any aggregate has a version mismatch, its row
     * is silently skipped (zero rows affected for that aggregate).
     *
     * Unlike the single save() path, this does NOT throw OptimisticLockException
     * per aggregate — detecting which specific aggregates conflicted requires
     * a follow-up SELECT. For strict conflict detection on batch updates,
     * fall back to batchUpdate() from DbalWriteRepository (calls save() per aggregate).
     *
     * After execution, incrementVersion() is called on each aggregate.
     *
     * @param AggregateRoot[] $aggregates
     */
    public function batchUpdate(array $aggregates): void
    {
        if (empty($aggregates)) {
            return;
        }

        $rows = array_map(fn(AggregateRoot $a) => $this->toRow($a), $aggregates);
        $columns = array_keys($rows[0]);

        $updateColumns = array_filter(
            $columns,
            fn(string $col) => !in_array($col, ['id', 'version'], true),
        );

        $setClauses = array_map(
            fn(string $col) => $col . ' = v.' . $col,
            $updateColumns,
        );
        $setClauses[] = 'version = ' . $this->tableName() . '.version + 1';

        $placeholder = '(' . implode(', ', array_fill(0, count($columns), '?')) . ')';
        $valuePlaceholders = implode(', ', array_fill(0, count($rows), $placeholder));
        $columnAlias = implode(', ', $columns);

        $sql = sprintf(
            'UPDATE %s SET %s FROM (VALUES %s) AS v(%s) WHERE %s.id = v.id AND %s.version = v.version',
            $this->tableName(),
            implode(', ', $setClauses),
            $valuePlaceholders,
            $columnAlias,
            $this->tableName(),
            $this->tableName(),
        );

        $flatValues = array_merge(...array_map('array_values', $rows));

        $this->connection->executeStatement($sql, $flatValues);

        foreach ($aggregates as $aggregate) {
            $aggregate->incrementVersion();
        }
    }
}
