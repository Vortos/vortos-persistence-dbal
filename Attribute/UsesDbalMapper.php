<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Attribute;

use Vortos\PersistenceDbal\Write\DbalStore;

/**
 * Declares which DbalMapper a repository uses.
 *
 * DbalRepositoryCompilerPass reads this attribute at compile time, creates a
 * named DbalStore service wired with the specified mapper, and injects it as
 * the $store constructor argument of the repository. No services.php entry needed.
 *
 * ## Usage
 *
 *   #[UsesDbalMapper(UserMapper::class)]
 *   final class UserWriteRepository implements UserRepositoryInterface
 *   {
 *       public function __construct(private readonly DbalStore $store) {}
 *
 *       public function save(User $user): void   { $this->store->save($user); }
 *       public function delete(User $user): void { $this->store->delete($user); }
 *
 *       public function findByEmail(Email $email): ?User
 *       {
 *           $row = $this->store->createQueryBuilder()
 *               ->select('*')
 *               ->from($this->store->mapper()->tableName())
 *               ->where('email = :email')
 *               ->setParameter('email', (string) $email)
 *               ->executeQuery()->fetchAssociative();
 *
 *           return $row !== false ? $this->store->mapper()->fromRow($row) : null;
 *       }
 *   }
 *
 * ## PostgreSQL-specific batch operations
 *
 * To use PostgresStore (adds batchUpsert and single-query batchUpdate):
 *
 *   #[UsesDbalMapper(mapper: OrderMapper::class, store: PostgresStore::class)]
 *   final class OrderWriteRepository implements OrderRepositoryInterface { ... }
 */
#[\Attribute(\Attribute::TARGET_CLASS)]
final class UsesDbalMapper
{
    public function __construct(
        public readonly string $mapperClass,
        public readonly string $storeClass = DbalStore::class,
    ) {}
}
