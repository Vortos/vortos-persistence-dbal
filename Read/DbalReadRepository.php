<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Read;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;
use Vortos\Domain\Repository\PageResult;
use Vortos\Domain\Repository\ReadRepositoryInterface;

/**
 * Abstract PostgreSQL-backed read repository.
 *
 * Use this when your read side lives in the same PostgreSQL database as the write side,
 * or in a dedicated PostgreSQL read replica. The read model is a flat table —
 * projections write to it directly; query handlers read from it.
 *
 * ## When to use this vs MongoReadRepository
 *
 * Use DbalReadRepository when:
 *   - Your read model needs complex joins or CTEs that MongoDB cannot express
 *   - Your team operates a single PostgreSQL database and doesn't want to run MongoDB
 *   - You need full-text search backed by pg_trgm or tsvector
 *
 * Use MongoReadRepository when:
 *   - Your read model is a self-contained document (a single aggregate's projection)
 *   - You expect very high read throughput and want a dedicated read store
 *   - Your projection shape changes frequently — schema migration is cheaper in MongoDB
 *
 * ## Implementing the two required methods
 *
 *   final class UserReadRepository extends DbalReadRepository
 *   {
 *       protected function tableName(): string
 *       {
 *           return 'user_read_models';
 *       }
 *
 *       protected function fromRow(array $row): array
 *       {
 *           return [
 *               'id'        => $row['id'],
 *               'email'     => $row['email'],
 *               'name'      => $row['name'],
 *               'createdAt' => $row['created_at'],
 *           ];
 *       }
 *   }
 *
 * ## Indexes
 *
 * Declare indexes in your SQL migration — they live alongside your CREATE TABLE statement.
 * There is no separate index management command for DBAL read repositories.
 *
 * ## Projections
 *
 * Write to the read model from your projection handlers using upsert():
 *
 *   public function __invoke(UserRegisteredEvent $event): void
 *   {
 *       $this->repository->upsert($event->aggregateId, [
 *           'email'      => $event->email,
 *           'name'       => $event->name,
 *           'created_at' => $event->occurredAt,
 *       ]);
 *   }
 *
 * ## Custom queries
 *
 * Use the protected query() method for anything findByCriteria() cannot express:
 *
 *   public function findActiveUsersWithRoles(): array
 *   {
 *       return $this->query()
 *           ->select('u.*, r.name AS role_name')
 *           ->join('u', 'user_roles', 'r', 'u.id = r.user_id')
 *           ->where("u.status = 'active'")
 *           ->fetchAllAssociative();
 *   }
 */
/**
 * @template T
 * @implements ReadRepositoryInterface<T>
 */
abstract class DbalReadRepository implements ReadRepositoryInterface
{
    public function __construct(protected readonly Connection $connection) {}

    abstract protected function tableName(): string;

    /**
     * Map a raw database row to your read model.
     *
     * @param array<string, mixed> $row
     * @return T
     */
    abstract protected function fromRow(array $row): mixed;

    /**
     * @return T|null
     */
    public function findById(string $id): mixed
    {
        $row = $this->query()
            ->where('id = :id')
            ->setParameter('id', $id)
            ->fetchAssociative();

        return $row === false ? null : $this->fromRow($row);
    }

    /**
     * @return list<T>
     */
    public function findByCriteria(
        array $criteria,
        array $sort = [],
        int $limit = 50,
        ?string $cursor = null,
    ): array {
        $qb = $this->query()->setMaxResults($limit);

        foreach ($criteria as $field => $value) {
            $param = 'p_' . $field;
            $qb->andWhere("{$field} = :{$param}")->setParameter($param, $value);
        }

        if ($cursor !== null) {
            $this->applyCursor($qb, $cursor, $sort);
        }

        foreach ($sort as $field => $direction) {
            $qb->addOrderBy($field, strtoupper($direction) === 'DESC' ? 'DESC' : 'ASC');
        }

        return array_map(
            fn(array $row) => $this->fromRow($row),
            $qb->fetchAllAssociative(),
        );
    }

    public function findPage(
        array $criteria,
        int $limit,
        ?string $cursor = null,
        array $sort = [],
    ): PageResult {
        $items = $this->findByCriteria($criteria, $sort, $limit + 1, $cursor);

        if (empty($items)) {
            return PageResult::empty();
        }

        $hasMore = count($items) > $limit;

        if ($hasMore) {
            $items = array_slice($items, 0, $limit);
        }

        $lastItem   = end($items);
        $nextCursor = $hasMore ? $this->encodeCursor($lastItem, $sort) : null;

        return new PageResult(
            items: $items,
            nextCursor: $nextCursor,
            hasMore: $hasMore,
        );
    }

    public function countByCriteria(array $criteria): int
    {
        $qb = $this->connection->createQueryBuilder()
            ->select('COUNT(*)')
            ->from($this->tableName());

        foreach ($criteria as $field => $value) {
            $param = 'p_' . $field;
            $qb->andWhere("{$field} = :{$param}")->setParameter($param, $value);
        }

        return (int) $qb->fetchOne();
    }

    /**
     * Insert or replace a row by id.
     *
     * Uses PostgreSQL INSERT ... ON CONFLICT (id) DO UPDATE SET.
     * The $data array must NOT include the id — pass it as the first argument.
     *
     * @param array<string, mixed> $data Column values excluding id
     */
    public function upsert(string $id, array $data): void
    {
        if (empty($data)) {
            return;
        }

        $columns = array_keys($data);
        $allColumns = array_merge(['id'], $columns);

        $placeholders = array_map(fn(string $col) => ":{$col}", $columns);
        $updatePairs  = array_map(fn(string $col) => "{$col} = EXCLUDED.{$col}", $columns);

        $sql = sprintf(
            'INSERT INTO %s (%s) VALUES (:id, %s) ON CONFLICT (id) DO UPDATE SET %s',
            $this->tableName(),
            implode(', ', $allColumns),
            implode(', ', $placeholders),
            implode(', ', $updatePairs),
        );

        $this->connection->executeStatement($sql, array_merge(['id' => $id], $data));
    }

    /**
     * Delete a row by id.
     */
    public function delete(string $id): void
    {
        $this->connection->executeStatement(
            'DELETE FROM ' . $this->tableName() . ' WHERE id = :id',
            ['id' => $id],
        );
    }

    /**
     * Returns a QueryBuilder pre-configured to SELECT * FROM this table.
     * Use for custom queries that findByCriteria() cannot express.
     */
    protected function query(): QueryBuilder
    {
        return $this->connection->createQueryBuilder()
            ->select('*')
            ->from($this->tableName());
    }

    private function encodeCursor(array $lastItem, array $sort): string
    {
        $position = [];
        foreach (array_keys($sort) as $field) {
            $position[$field] = $lastItem[$field] ?? null;
        }
        $position['id'] = $lastItem['id'] ?? null;

        return base64_encode(json_encode($position, JSON_THROW_ON_ERROR));
    }

    private function applyCursor(QueryBuilder $qb, string $cursor, array $sort): void
    {
        $position = json_decode(base64_decode($cursor), true, 512, JSON_THROW_ON_ERROR);

        if (!is_array($position)) {
            throw new \InvalidArgumentException('Invalid pagination cursor.');
        }

        foreach ($sort as $field => $direction) {
            if (!array_key_exists($field, $position)) {
                continue;
            }

            $op    = strtoupper($direction) === 'DESC' ? '<' : '>';
            $param = 'cursor_' . $field;
            $qb->andWhere("{$field} {$op} :{$param}")->setParameter($param, $position[$field]);
        }

        if (isset($position['id'])) {
            $qb->andWhere('id > :cursor_id')->setParameter('cursor_id', $position['id']);
        }
    }
}
