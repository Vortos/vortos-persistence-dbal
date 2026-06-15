<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Tests\Tenant;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Vortos\PersistenceDbal\Write\DbalMapper;
use Vortos\PersistenceDbal\Write\DbalStore;
use Vortos\Tenant\Exception\MissingTenantContextException;
use Vortos\Tenant\TenantContext;

/**
 * Exposes DbalStore's protected tenant helpers for unit testing the
 * stamp/fail-closed logic without a live database.
 */
final class TenantStoreFixture extends DbalStore
{
    /** @param array<string,mixed> $row @return array<string,mixed> */
    public function stamp(array $row): array
    {
        return $this->stampTenant($row);
    }

    public function tenantId(): ?string
    {
        return $this->writeTenantId();
    }
}

final class DbalStoreTenantTest extends TestCase
{
    public function test_scoped_store_stamps_tenant_on_insert_row(): void
    {
        $store = $this->store('tenant_id', $this->contextFor('acme'));

        $row = $store->stamp(['id' => 'x', 'amount' => 10]);

        $this->assertSame('acme', $row['tenant_id']);
        $this->assertSame('acme', $store->tenantId());
    }

    public function test_unscoped_store_leaves_row_untouched(): void
    {
        $store = $this->store(null, new TenantContext());

        $row = $store->stamp(['id' => 'x']);

        $this->assertArrayNotHasKey('tenant_id', $row);
        $this->assertNull($store->tenantId());
    }

    public function test_write_in_system_scope_fails_closed(): void
    {
        $context = new TenantContext();

        $this->expectException(MissingTenantContextException::class);
        $context->runAsSystem(fn() => $this->store('tenant_id', $context)->tenantId());
    }

    public function test_write_without_tenant_fails_closed(): void
    {
        $store = $this->store('tenant_id', new TenantContext());

        $this->expectException(MissingTenantContextException::class);
        $store->tenantId();
    }

    private function store(?string $tenantColumn, TenantContext $context): TenantStoreFixture
    {
        return new TenantStoreFixture(
            $this->createMock(Connection::class),
            $this->createMock(DbalMapper::class),
            $context,
            $tenantColumn,
        );
    }

    private function contextFor(string $tenantId): TenantContext
    {
        $context = new TenantContext();
        $context->set($tenantId);

        return $context;
    }
}
