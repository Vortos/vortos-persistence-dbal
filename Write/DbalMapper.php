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
 * ## Example
 *
 *   final class UserMapper implements DbalMapper
 *   {
 *       public function tableName(): string { return 'users'; }
 *
 *       public function columnMap(): array {
 *           return ['id' => Types::STRING, 'email' => Types::STRING, 'version' => Types::INTEGER];
 *       }
 *
 *       public function toRow(AggregateRoot $aggregate): array {
 *           assert($aggregate instanceof User);
 *           return ['id' => (string) $aggregate->getId(), 'email' => (string) $aggregate->getEmail(), 'version' => $aggregate->getVersion()];
 *       }
 *
 *       public function fromRow(array $row): AggregateRoot {
 *           return User::reconstruct(UserId::fromString($row['id']), new Email($row['email']), (int) $row['version']);
 *       }
 *   }
 */
interface DbalMapper
{
    /**
     * The plain table name without schema prefix.
     */
    public function tableName(): string;

    /**
     * Map of column names to DBAL Types constants.
     * MUST include 'version' => Types::INTEGER.
     *
     * @return array<string, string>
     */
    public function columnMap(): array;

    /**
     * Map an aggregate to a flat database row array.
     * Keys must exactly match columnMap(). Include 'version'.
     * Do NOT call incrementVersion() here — DbalStore handles that.
     *
     * @return array<string, mixed>
     */
    public function toRow(AggregateRoot $aggregate): array;

    /**
     * Reconstruct an aggregate from a flat database row array.
     * Must restore the version field via a reconstruct() named constructor.
     *
     * @param array<string, mixed> $row
     */
    public function fromRow(array $row): AggregateRoot;
}
