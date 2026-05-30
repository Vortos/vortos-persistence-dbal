<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Write;

use Doctrine\DBAL\Types\Types;
use Vortos\Domain\Aggregate\AggregateRoot;
use Vortos\Domain\Repository\Exception\OptimisticLockException;

/**
 * PostgreSQL-optimised DBAL store.
 *
 * Extends DbalStore with two PostgreSQL-specific batch operations:
 *
 *   batchUpdate() — overrides the base loop with a single UPDATE FROM VALUES query
 *   batchUpsert() — INSERT ... ON CONFLICT (id) DO UPDATE SET for projections and bulk imports
 *
 * ## When to use this instead of DbalStore
 *
 * Use PostgresStore when:
 *   - Your application runs on PostgreSQL (the default Vortos stack)
 *   - You have use cases that update or upsert large batches of aggregates at once
 *
 * Declare it via the storeClass parameter on the attribute:
 *   #[UsesDbalMapper(mapper: OrderMapper::class, store: PostgresStore::class)]
 *
 * ## All other methods are inherited from DbalStore
 *
 * save(), delete(), batchInsert(), batchDelete(), batchForceDeleteByIds(), find()
 * behave identically.
 */
class PostgresStore extends DbalStore
{
    /**
     * Update multiple aggregates using PostgreSQL's UPDATE FROM VALUES syntax.
     *
     * Executes a single SQL statement regardless of how many aggregates are passed.
     * Applies optimistic locking via WHERE lock_version = v.lock_version per row.
     *
     * Unlike the per-aggregate save() path, this does NOT throw per-aggregate
     * OptimisticLockException — it throws if the total affected count mismatches,
     * but does not identify which specific aggregate conflicted.
     * For strict per-aggregate detection, use DbalStore::batchUpdate() (loops save()).
     *
     * @param AggregateRoot[] $aggregates
     */
    public function batchUpdate(array $aggregates): void
    {
        if (empty($aggregates)) {
            return;
        }

        $types   = $this->columnMap();
        $rows    = array_map(fn(AggregateRoot $a) => $this->toRow($a), $aggregates);
        $columns = array_keys($rows[0]);

        $updateColumns = array_filter(
            $columns,
            fn(string $col) => !in_array($col, ['id', 'lock_version'], true),
        );

        $quotedTable = $this->connection()->quoteIdentifier($this->mapper()->tableName());

        $setClauses   = array_map(
            fn(string $col) => $this->connection()->quoteIdentifier($col) . ' = v.' . $this->connection()->quoteIdentifier($col),
            $updateColumns,
        );
        $setClauses[] = 'lock_version = ' . $quotedTable . '.lock_version + 1';

        $placeholder       = '(' . implode(', ', array_fill(0, count($columns), '?')) . ')';
        $valuePlaceholders = implode(', ', array_fill(0, count($rows), $placeholder));
        $columnAlias       = implode(', ', array_map(
            fn(string $c) => $this->connection()->quoteIdentifier($c),
            $columns,
        ));

        $sql = sprintf(
            'UPDATE %s SET %s FROM (VALUES %s) AS v(%s) WHERE %s.id = v.id AND %s.lock_version = v.lock_version',
            $quotedTable,
            implode(', ', $setClauses),
            $valuePlaceholders,
            $columnAlias,
            $quotedTable,
            $quotedTable,
        );

        $flatValues = [];
        $flatTypes  = [];
        foreach ($rows as $row) {
            foreach ($columns as $col) {
                $flatValues[] = $row[$col];
                $flatTypes[]  = $types[$col] ?? Types::STRING;
            }
        }

        $affected = $this->connection()->executeStatement($sql, $flatValues, $flatTypes);

        if ($affected !== count($aggregates)) {
            throw new OptimisticLockException(sprintf(
                'Batch update conflict: expected %d row(s) affected, got %d. Version mismatch on one or more aggregates.',
                count($aggregates),
                $affected,
            ));
        }

        foreach ($aggregates as $aggregate) {
            $aggregate->incrementVersion();
        }
    }

    /**
     * Insert or update multiple aggregates in a single SQL statement.
     *
     * Uses PostgreSQL's INSERT ... ON CONFLICT (id) DO UPDATE SET syntax.
     * On conflict, all columns except id are updated to the new values.
     *
     * WARNING: Does NOT apply optimistic locking.
     * Use only for read model projections, idempotent bulk imports, or test seeding.
     * Never use for commands where concurrent modification must be detected.
     *
     * @param AggregateRoot[] $aggregates
     */
    public function batchUpsert(array $aggregates): void
    {
        if (empty($aggregates)) {
            return;
        }

        $types   = $this->columnMap();
        $rows    = array_map(fn(AggregateRoot $a) => $this->toRow($a), $aggregates);
        $columns = array_keys($rows[0]);

        $placeholder  = '(' . implode(', ', array_fill(0, count($columns), '?')) . ')';
        $placeholders = implode(', ', array_fill(0, count($rows), $placeholder));

        $quotedTable   = $this->connection()->quoteIdentifier($this->mapper()->tableName());
        $quotedColumns = implode(', ', array_map(
            fn(string $c) => $this->connection()->quoteIdentifier($c),
            $columns,
        ));

        $setClauses = implode(', ', array_map(
            fn(string $col) => $this->connection()->quoteIdentifier($col) . ' = EXCLUDED.' . $this->connection()->quoteIdentifier($col),
            array_filter($columns, fn(string $col) => $col !== 'id'),
        ));

        $sql = sprintf(
            'INSERT INTO %s (%s) VALUES %s ON CONFLICT (id) DO UPDATE SET %s',
            $quotedTable,
            $quotedColumns,
            $placeholders,
            $setClauses,
        );

        $flatValues = [];
        $flatTypes  = [];
        foreach ($rows as $row) {
            foreach ($columns as $col) {
                $flatValues[] = $row[$col];
                $flatTypes[]  = $types[$col] ?? Types::STRING;
            }
        }

        $this->connection()->executeStatement($sql, $flatValues, $flatTypes);

        foreach ($aggregates as $aggregate) {
            $aggregate->incrementVersion();
        }
    }
}
