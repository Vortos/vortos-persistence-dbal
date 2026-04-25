<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Write;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Vortos\Domain\Aggregate\AggregateRoot;
use Vortos\Domain\Identity\AggregateId;
use Vortos\Domain\Repository\Exception\OptimisticLockException;
use Vortos\Domain\Repository\WriteRepositoryInterface;

/**
 * Abstract DBAL-backed write repository.
 *
 * Provides all standard persistence operations free.
 * You implement 4 methods describing your table shape.
 * The base handles insert, update, delete, batch operations, and optimistic locking.
 *
 * ## This is NOT an ORM
 *
 * No identity map. No change tracking. No lazy loading. No proxy objects.
 * All SQL is explicit. What you see is what executes.
 *
 * ## Required table structure
 *
 * Every table using this base MUST have these columns:
 *
 *   id      UUID or VARCHAR(36)  PRIMARY KEY
 *   version INTEGER              NOT NULL DEFAULT 0
 *
 * The version column is used for optimistic concurrency control.
 * If your table lacks these columns, save() will behave incorrectly.
 *
 * ## Implementing the 4 required methods
 *
 *   final class UserRepository extends DbalWriteRepository
 *   {
 *       protected function tableName(): string
 *       {
 *           return 'users';
 *       }
 *
 *       protected function columnMap(): array
 *       {
 *           return [
 *               'id'      => Types::STRING,
 *               'email'   => Types::STRING,
 *               'name'    => Types::STRING,
 *               'version' => Types::INTEGER,
 *           ];
 *       }
 *
 *       protected function toRow(AggregateRoot $aggregate): array
 *       {
 *           /** @var User $aggregate
 *           return [
 *               'id'      => (string) $aggregate->getId(),
 *               'email'   => (string) $aggregate->email,
 *               'name'    => $aggregate->name,
 *               'version' => $aggregate->getVersion(),
 *           ];
 *       }
 *
 *       protected function fromRow(array $row): AggregateRoot
 *       {
 *           return User::reconstruct(
 *               UserId::fromString($row['id']),
 *               $row['email'],
 *               $row['name'],
 *               (int) $row['version'],
 *           );
 *       }
 *   }
 *
 * ## Custom queries
 *
 * Use the protected connection() method for queries beyond findById():
 *
 *   public function findByEmail(Email $email): ?User
 *   {
 *       $row = $this->connection()->createQueryBuilder()
 *           ->select('*')
 *           ->from($this->tableName())
 *           ->where('email = :email')
 *           ->setParameter('email', (string) $email)
 *           ->executeQuery()
 *           ->fetchAssociative();
 *
 *       return $row ? $this->fromRow($row) : null;
 *   }
 *
 * ## Optimistic locking
 *
 * save() uses the version column to detect concurrent modifications.
 * If two processes load the same aggregate and both call save(),
 * the second save will throw OptimisticLockException because the
 * version in the database no longer matches the expected version.
 *
 * Your ApplicationService should catch OptimisticLockException and
 * either retry (with fresh load) or return a conflict error to the caller.
 */
abstract class DbalWriteRepository implements WriteRepositoryInterface
{
    public function __construct(protected Connection $connection) {}

    /**
     * The database table name for this repository.
     *
     * Return the plain table name without schema prefix.
     * Example: 'users', 'orders', 'competition_entries'
     */
    abstract protected function tableName(): string;

    /**
     * Map of column names to DBAL Types constants.
     *
     * Used for type-safe parameter binding in all generated queries.
     * MUST include 'version' => Types::INTEGER.
     *
     * Example:
     *   return [
     *       'id'      => Types::STRING,
     *       'email'   => Types::STRING,
     *       'version' => Types::INTEGER,
     *   ];
     */
    abstract protected function columnMap(): array;

    /**
     * Map an aggregate to a flat database row array.
     *
     * Keys must exactly match the column names in columnMap().
     * Include 'version' => $aggregate->getVersion() — the base class
     * uses this for optimistic lock checks, but never writes it directly
     * as a SET value in UPDATE queries (the DB does version + 1 instead).
     *
     * Do NOT call incrementVersion() here — the base handles that
     * after a successful save.
     */
    abstract protected function toRow(AggregateRoot $aggregate): array;

    /**
     * Reconstruct an aggregate from a flat database row array.
     *
     * Must restore the version field. Your aggregate needs a way to
     * accept a version on reconstruction — typically a static factory
     * method or a dedicated reconstruct() named constructor.
     *
     * Example:
     *   return User::reconstruct(
     *       UserId::fromString($row['id']),
     *       $row['email'],
     *       (int) $row['version'],
     *   );
     */
    abstract protected function fromRow(array $row): AggregateRoot;

    /**
     * Find an aggregate by its ID.
     *
     * Returns null if no row exists — never throws for missing records.
     * The returned aggregate has its version restored from the database,
     * ready for optimistic lock checks on the next save().
     *
     * {@inheritdoc}
     */
    public function findById(AggregateId $id): ?AggregateRoot
    {
        $row = $this->connection->createQueryBuilder()
            ->select('*')
            ->from($this->tableName())
            ->where('id = :id')
            ->setParameter('id', (string) $id)
            ->executeQuery()
            ->fetchAssociative();

        if ($row === false) {
            return null;
        }

        return $this->fromRow($row);
    }

    /**
     * Persist an aggregate — handles both insert and update.
     *
     * ## Insert vs Update detection
     *
     * version === 0 → new aggregate → INSERT
     * version  > 0 → existing aggregate → UPDATE with optimistic lock check
     *
     * ## Optimistic locking on UPDATE
     *
     * The UPDATE WHERE clause includes: AND version = :expectedVersion
     * If zero rows are affected, another process modified the aggregate
     * between your load and this save. OptimisticLockException is thrown.
     *
     * ## Version increment
     *
     * After a successful save (insert or update), incrementVersion() is called
     * on the aggregate. This keeps the in-memory aggregate in sync with the
     * database version, so subsequent saves in the same request work correctly.
     *
     * {@inheritdoc}
     */
    public function save(AggregateRoot $aggregate): void
    {
        $row = $this->toRow($aggregate);

        if ($aggregate->getVersion() === 0) {
            $this->connection->insert($this->tableName(), $row);
            $aggregate->incrementVersion();
            return;
        }

        $expectedVersion = $aggregate->getVersion();

        unset($row['version']);

        $qb = $this->connection->createQueryBuilder();
        $qb->update($this->tableName());

        foreach ($row as $column => $value) {
            $qb->set($column, ':' . $column);
            $qb->setParameter($column, $value);
        }

        $qb->set('version', 'version + 1')
            ->where('id = :id')
            ->andWhere('version = :expectedVersion')
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
     * Applies optimistic locking on delete — prevents deleting an aggregate
     * that has been modified since you loaded it.
     *
     * If zero rows are affected, throws OptimisticLockException.
     * This could mean the aggregate was already deleted or was modified
     * by another process.
     *
     * {@inheritdoc}
     */
    public function delete(AggregateRoot $aggregate): void
    {
        $qb = $this->connection->createQueryBuilder();

        $affected = $qb->delete($this->tableName())
            ->where('id = :id')
            ->andWhere('version = :version')
            ->setParameter('id', (string) $aggregate->getId())
            ->setParameter('version', $aggregate->getVersion())
            ->executeStatement();

        if ($affected === 0) {
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
     * More efficient than calling save() in a loop for bulk inserts.
     * Builds one INSERT with multiple VALUES rows and executes once.
     *
     * All aggregates must be new (version === 0). For updating existing
     * aggregates in bulk, use batchUpsert() or batchUpdate().
     *
     * After successful insert, incrementVersion() is called on each aggregate.
     */
    public function batchInsert(array $aggregates): void
    {
        if (empty($aggregates)) {
            return;
        }

        $rows = array_map(fn(AggregateRoot $a) => $this->toRow($a), $aggregates);
        $columns = array_keys($rows[0]);
        $placeholder = '(' . implode(', ', array_fill(0, count($columns), '?')) . ')';
        $placeholders = implode(', ', array_fill(0, count($rows), $placeholder));

        $sql = sprintf(
            'INSERT INTO %s (%s) VALUES %s',
            $this->tableName(),
            implode(', ', $columns),
            $placeholders,
        );

        $flatValues = array_merge(...array_map('array_values', $rows));

        $this->connection->executeStatement($sql, $flatValues);

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
     * WARNING: This method does NOT apply optimistic locking.
     * It will silently overwrite any version in the database.
     * Use only for:
     *   - Read model projections (eventual consistency is acceptable)
     *   - Idempotent bulk imports where last-write-wins is intentional
     *   - Seeding data in tests
     *
     * Never use this for commands where concurrent modification must be detected.
     */
    public function batchUpsert(array $aggregates): void
    {
        if (empty($aggregates)) {
            return;
        }

        // No optimistic lock — use only for projections or idempotent bulk writes.
        $rows = array_map(fn(AggregateRoot $a) => $this->toRow($a), $aggregates);
        $columns = array_keys($rows[0]);
        $placeholder = '(' . implode(', ', array_fill(0, count($columns), '?')) . ')';
        $placeholders = implode(', ', array_fill(0, count($rows), $placeholder));

        $setClauses = array_map(
            fn(string $col) => $col . ' = EXCLUDED.' . $col,
            array_filter($columns, fn(string $col) => $col !== 'id'),
        );

        $sql = sprintf(
            'INSERT INTO %s (%s) VALUES %s ON CONFLICT (id) DO UPDATE SET %s',
            $this->tableName(),
            implode(', ', $columns),
            $placeholders,
            implode(', ', $setClauses),
        );

        $flatValues = array_merge(...array_map('array_values', $rows));

        $this->connection->executeStatement($sql, $flatValues);
    }

    /**
     * Update multiple aggregates by calling save() on each.
     *
     * Each save() applies optimistic locking individually.
     * This is the safe generic implementation — it works across all databases.
     *
     * For PostgreSQL-specific bulk UPDATE FROM VALUES (more efficient at scale),
     * extend PostgresWriteRepository instead, which overrides this method
     * with a single-query implementation.
     */
    public function batchUpdate(array $aggregates): void
    {
        foreach ($aggregates as $aggregate) {
            $this->save($aggregate);
        }
    }

    /**
     * Delete multiple aggregates by ID in a single SQL statement.
     *
     * Uses DELETE WHERE id IN (:ids) — one query regardless of count.
     * Does NOT apply optimistic locking — use only when you are certain
     * the aggregates have not been modified since you loaded their IDs.
     */
    public function batchDelete(array $ids): void
    {
        if (empty($ids)) {
            return;
        }

        $stringIds = array_map(fn(AggregateId $id) => (string) $id, $ids);

        $this->connection->createQueryBuilder()
            ->delete($this->tableName())
            ->where('id IN (:ids)')
            ->setParameter('ids', $stringIds, ArrayParameterType::STRING)
            ->executeStatement();
    }

    /**
     * Exposes the DBAL Connection for custom queries in subclasses.
     *
     * Use the QueryBuilder for all custom queries — never raw SQL strings.
     * Raw SQL strings are not portable and bypass DBAL's parameter escaping.
     *
     * Example:
     *   $this->connection()->createQueryBuilder()
     *       ->select('*')
     *       ->from($this->tableName())
     *       ->where('email = :email')
     *       ->setParameter('email', $email)
     *       ->executeQuery()
     *       ->fetchAllAssociative();
     */
    protected function connection(): Connection
    {
        return $this->connection;
    }
}
