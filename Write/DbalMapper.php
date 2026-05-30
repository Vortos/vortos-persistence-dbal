<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Write;

use Vortos\Domain\Aggregate\AggregateRoot;

/**
 * Describes the mapping between an aggregate and a database table.
 *
 * Implement this interface in a dedicated mapper class, then declare
 * which mapper your repository uses via #[UsesDbalMapper(YourMapper::class)].
 * The framework auto-wires a DbalStore with your mapper into the repository.
 *
 * ## lock_version is handled automatically
 *
 * Do NOT include 'lock_version' in columnMap(), toRow(), or fromRow().
 * DbalStore injects it on INSERT, applies it in WHERE on UPDATE/DELETE,
 * and restores it on the aggregate after find() via Closure::bind().
 *
 * ## Example
 *
 *   final class UserMapper implements DbalMapper
 *   {
 *       public function tableName(): string { return 'users'; }
 *
 *       public function columnMap(): array {
 *           return ['id' => Types::STRING, 'email' => Types::STRING];
 *       }
 *
 *       public function toRow(AggregateRoot $aggregate): array {
 *           assert($aggregate instanceof User);
 *           return ['id' => (string) $aggregate->getId(), 'email' => (string) $aggregate->getEmail()];
 *       }
 *
 *       public function fromRow(array $row): AggregateRoot {
 *           return User::reconstruct(UserId::fromString($row['id']), new Email($row['email']));
 *       }
 *   }
 *
 * ## Custom queries
 *
 * Use $store->hydrate($row) instead of $store->mapper()->fromRow($row) in custom query methods.
 * hydrate() calls fromRow() and then restores lock_version automatically.
 */
interface DbalMapper
{
    /**
     * The plain table name without schema prefix.
     */
    public function tableName(): string;

    /**
     * Map of column names to DBAL Types constants.
     * Do NOT include 'lock_version' — DbalStore injects it automatically.
     *
     * @return array<string, string>
     */
    public function columnMap(): array;

    /**
     * Map an aggregate to a flat database row array.
     * Keys must exactly match columnMap().
     * Do NOT include 'lock_version' and do NOT call incrementVersion() — DbalStore handles both.
     *
     * @return array<string, mixed>
     */
    public function toRow(AggregateRoot $aggregate): array;

    /**
     * Reconstruct an aggregate from a flat database row array (without restoring lock_version).
     * DbalStore calls hydrate() which applies lock_version after fromRow() returns.
     * Do NOT call restoreVersion() here.
     *
     * @param array<string, mixed> $row
     */
    public function fromRow(array $row): AggregateRoot;
}
