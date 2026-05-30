<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Write;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Types\Types;
use Vortos\Domain\Aggregate\AggregateRoot;
use Vortos\Domain\Identity\AggregateId;
use Vortos\Domain\Repository\Exception\AggregateNotFoundException;
use Vortos\Domain\Repository\Exception\OptimisticLockException;

/**
 * DBAL-backed persistence store.
 *
 * Contains all persistence logic for DBAL-backed write repositories.
 * Injected into user repositories by DbalRepositoryCompilerPass when
 * the repository declares #[UsesDbalMapper(YourMapper::class)].
 *
 * ## This is NOT an ORM
 *
 * No identity map. No change tracking. No lazy loading. No proxy objects.
 * All SQL is explicit. What you see is what executes.
 *
 * ## Required table structure
 *
 * Every table using this store MUST have these columns:
 *
 *   id           UUID or VARCHAR(36)  PRIMARY KEY
 *   lock_version INTEGER              NOT NULL DEFAULT 0
 *
 * ## Optimistic locking
 *
 * save() uses the lock_version column to detect concurrent modifications.
 * delete() uses a two-query failure path to distinguish AggregateNotFoundException
 * from OptimisticLockException — zero overhead on the happy path.
 *
 * ## Batch operations
 *
 * batchInsert()           — single INSERT with multiple VALUES rows; aggregates must be new
 * batchUpdate()           — loops save() per aggregate with optimistic locking on each
 * batchDelete()           — loops delete() per aggregate with optimistic locking on each
 * batchForceDeleteByIds() — single DELETE IN (:ids); bypasses version checks
 *
 * PostgresStore extends this class and overrides batchUpdate() with a single-query
 * UPDATE FROM VALUES implementation, and adds batchUpsert().
 */
class DbalStore
{
    private readonly \Closure $restoreLockVersion;

    public function __construct(
        private readonly Connection $connection,
        private readonly DbalMapper $mapper,
    ) {
        $this->restoreLockVersion = \Closure::bind(
            static function (AggregateRoot $agg, int $v): void {
                $agg->restoreVersion($v);
            },
            null,
            AggregateRoot::class,
        );
    }

    /**
     * Reconstruct an aggregate from a database row, restoring its lock_version automatically.
     * Use this in custom query methods instead of $this->mapper()->fromRow($row) directly.
     */
    public function hydrate(array $row): AggregateRoot
    {
        $aggregate = $this->mapper->fromRow($row);
        ($this->restoreLockVersion)($aggregate, (int) $row['lock_version']);
        return $aggregate;
    }

    /**
     * Returns a row array from the mapper with lock_version injected from the aggregate.
     * Used internally by batch operations in this class and PostgresStore.
     */
    protected function toRow(AggregateRoot $aggregate): array
    {
        return $this->mapper->toRow($aggregate) + ['lock_version' => $aggregate->getVersion()];
    }

    /**
     * Returns the column type map from the mapper with lock_version type injected.
     * Used internally by batch operations in this class and PostgresStore.
     */
    protected function columnMap(): array
    {
        return $this->mapper->columnMap() + ['lock_version' => Types::INTEGER];
    }

    /**
     * Persist an aggregate — handles both insert and update.
     *
     * Uses AggregateRoot::isNew() to detect insert vs update.
     * UPDATE applies an optimistic lock: WHERE lock_version = :expectedVersion.
     * If zero rows affected, throws OptimisticLockException.
     * Calls incrementVersion() on the aggregate after a successful save.
     */
    public function save(AggregateRoot $aggregate): void
    {
        if ($aggregate->isNew()) {
            $this->connection->insert(
                $this->mapper->tableName(),
                $this->toRow($aggregate),
                $this->columnMap(),
            );
            $aggregate->incrementVersion();
            return;
        }

        $expectedVersion = $aggregate->getVersion();
        $row   = $this->mapper->toRow($aggregate);
        $qb    = $this->connection->createQueryBuilder();
        $types = $this->mapper->columnMap();

        $qb->update($this->mapper->tableName());

        foreach ($row as $column => $value) {
            $qb->set($column, ':' . $column);
            $qb->setParameter($column, $value, $types[$column] ?? null);
        }

        $qb->set('lock_version', 'lock_version + 1')
            ->where('id = :id')
            ->andWhere('lock_version = :expectedVersion')
            ->setParameter('id', (string) $aggregate->getId())
            ->setParameter('expectedVersion', $expectedVersion);

        $affected = $qb->executeStatement();

        if ($affected === 0) {
            throw OptimisticLockException::forAggregate(
                get_class($aggregate),
                (string) $aggregate->getId(),
                $expectedVersion,
                -1,
            );
        }

        $aggregate->incrementVersion();
    }

    /**
     * Remove an aggregate from the store.
     *
     * Applies optimistic locking. On failure, a follow-up SELECT distinguishes
     * AggregateNotFoundException (row gone) from OptimisticLockException (version mismatch).
     * Zero overhead on the happy path — the follow-up query only runs on failure.
     */
    public function delete(AggregateRoot $aggregate): void
    {
        $affected = $this->connection->createQueryBuilder()
            ->delete($this->mapper->tableName())
            ->where('id = :id')
            ->andWhere('lock_version = :lock_version')
            ->setParameter('id', (string) $aggregate->getId())
            ->setParameter('lock_version', $aggregate->getVersion())
            ->executeStatement();

        if ($affected === 0) {
            $exists = (bool) $this->connection->createQueryBuilder()
                ->select('1')
                ->from($this->mapper->tableName())
                ->where('id = :id')
                ->setParameter('id', (string) $aggregate->getId())
                ->executeQuery()
                ->fetchOne();

            if (!$exists) {
                throw AggregateNotFoundException::for(get_class($aggregate), (string) $aggregate->getId());
            }

            throw OptimisticLockException::forAggregate(
                get_class($aggregate),
                (string) $aggregate->getId(),
                $aggregate->getVersion(),
                -1,
            );
        }
    }

    /**
     * Insert multiple aggregates in a single SQL statement.
     *
     * All aggregates must be new (isNew() === true).
     * Calls incrementVersion() on each aggregate after successful insert.
     *
     * @param AggregateRoot[] $aggregates
     */
    public function batchInsert(array $aggregates): void
    {
        if (empty($aggregates)) {
            return;
        }

        $types   = $this->columnMap();
        $rows    = array_map(fn(AggregateRoot $a) => $this->toRow($a), $aggregates);
        $columns = array_keys($rows[0]);

        $placeholder  = '(' . implode(', ', array_fill(0, count($columns), '?')) . ')';
        $placeholders = implode(', ', array_fill(0, count($rows), $placeholder));

        $quotedTable   = $this->connection->quoteIdentifier($this->mapper->tableName());
        $quotedColumns = implode(', ', array_map(
            fn(string $c) => $this->connection->quoteIdentifier($c),
            $columns,
        ));

        $sql = sprintf('INSERT INTO %s (%s) VALUES %s', $quotedTable, $quotedColumns, $placeholders);

        $flatValues = [];
        $flatTypes  = [];
        foreach ($rows as $row) {
            foreach ($columns as $col) {
                $flatValues[] = $row[$col];
                $flatTypes[]  = $types[$col] ?? Types::STRING;
            }
        }

        $this->connection->executeStatement($sql, $flatValues, $flatTypes);

        foreach ($aggregates as $aggregate) {
            $aggregate->incrementVersion();
        }
    }

    /**
     * Update multiple aggregates by calling save() on each.
     *
     * Applies optimistic locking individually per aggregate.
     * For PostgreSQL single-query bulk updates, use PostgresStore instead.
     *
     * @param AggregateRoot[] $aggregates
     */
    public function batchUpdate(array $aggregates): void
    {
        foreach ($aggregates as $aggregate) {
            $this->save($aggregate);
        }
    }

    /**
     * Delete multiple aggregates, applying optimistic locking on each.
     *
     * @param AggregateRoot[] $aggregates
     */
    public function batchDelete(array $aggregates): void
    {
        foreach ($aggregates as $aggregate) {
            $this->delete($aggregate);
        }
    }

    /**
     * Delete multiple aggregates by ID in a single SQL statement.
     *
     * Uses DELETE WHERE id IN (:ids). Does NOT apply optimistic locking.
     * Use only for cascading deletes, test teardown, or administrative bulk removal.
     *
     * @param AggregateId[] $ids
     */
    public function batchForceDeleteByIds(array $ids): void
    {
        if (empty($ids)) {
            return;
        }

        $stringIds = array_map(fn(AggregateId $id) => (string) $id, $ids);

        $this->connection->createQueryBuilder()
            ->delete($this->mapper->tableName())
            ->where('id IN (:ids)')
            ->setParameter('ids', $stringIds, ArrayParameterType::STRING)
            ->executeStatement();
    }

    /**
     * Load a single aggregate by ID. Returns null if not found.
     */
    public function find(AggregateId $id): ?AggregateRoot
    {
        $row = $this->connection->createQueryBuilder()
            ->select('*')
            ->from($this->mapper->tableName())
            ->where('id = :id')
            ->setParameter('id', (string) $id)
            ->executeQuery()
            ->fetchAssociative();

        return $row !== false ? $this->hydrate($row) : null;
    }

    /**
     * Returns a fresh QueryBuilder for custom query methods in the repository.
     */
    public function createQueryBuilder(): QueryBuilder
    {
        return $this->connection->createQueryBuilder();
    }

    /**
     * Returns the mapper. Use mapper()->tableName() in custom queries.
     * To hydrate an aggregate from a row, use hydrate($row) — not mapper()->fromRow($row) directly.
     */
    public function mapper(): DbalMapper
    {
        return $this->mapper;
    }

    /**
     * Returns the raw DBAL Connection for operations not covered by the QueryBuilder.
     */
    public function connection(): Connection
    {
        return $this->connection;
    }
}
