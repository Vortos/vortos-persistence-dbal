<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Tests\Tenant;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Query\QueryBuilder;
use PHPUnit\Framework\TestCase;
use Vortos\PersistenceDbal\Read\DbalReadRepository;
use Vortos\Tenant\Attribute\TenantScoped;
use Vortos\Tenant\Exception\MissingTenantContextException;
use Vortos\Tenant\TenantContext;

#[TenantScoped]
final class ScopedReadRepoFixture extends DbalReadRepository
{
    protected function tableName(): string
    {
        return 'invoices';
    }

    protected function fromRow(array $row): mixed
    {
        return $row;
    }

    public function exposeQuerySql(): string
    {
        return $this->query()->getSQL();
    }
}

final class UnscopedReadRepoFixture extends DbalReadRepository
{
    protected function tableName(): string
    {
        return 'plans';
    }

    protected function fromRow(array $row): mixed
    {
        return $row;
    }

    public function exposeQuerySql(): string
    {
        return $this->query()->getSQL();
    }
}

final class DbalReadRepositoryTenantTest extends TestCase
{
    public function test_scoped_repo_adds_tenant_predicate(): void
    {
        $context = new TenantContext();
        $context->set('acme');

        $repo = new ScopedReadRepoFixture($this->connection(), $context);
        $sql  = $repo->exposeQuerySql();

        $this->assertStringContainsString('tenant_id = :__tenant_id', $sql);
    }

    public function test_system_scope_skips_predicate(): void
    {
        $context = new TenantContext();

        $sql = $context->runAsSystem(
            fn() => (new ScopedReadRepoFixture($this->connection(), $context))->exposeQuerySql(),
        );

        $this->assertStringNotContainsString('tenant_id', $sql);
    }

    public function test_scoped_repo_without_tenant_fails_closed(): void
    {
        $repo = new ScopedReadRepoFixture($this->connection(), new TenantContext());

        $this->expectException(MissingTenantContextException::class);
        $repo->exposeQuerySql();
    }

    public function test_scoped_repo_without_context_service_fails_closed(): void
    {
        $repo = new ScopedReadRepoFixture($this->connection(), null);

        $this->expectException(MissingTenantContextException::class);
        $repo->exposeQuerySql();
    }

    public function test_unscoped_repo_has_no_tenant_predicate(): void
    {
        $repo = new UnscopedReadRepoFixture($this->connection(), new TenantContext());
        $sql  = $repo->exposeQuerySql();

        $this->assertStringNotContainsString('tenant_id', $sql);
    }

    /**
     * A Connection whose createQueryBuilder() yields a real QueryBuilder.
     * getSQL() on a SELECT builds the string without touching the database.
     */
    private function connection(): Connection
    {
        $connection = $this->createMock(Connection::class);
        $connection->method('createQueryBuilder')->willReturnCallback(
            static fn() => new QueryBuilder($connection),
        );
        $connection->method('getDatabasePlatform')->willReturn(new PostgreSQLPlatform());

        return $connection;
    }
}
