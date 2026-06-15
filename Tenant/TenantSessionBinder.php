<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Tenant;

use Doctrine\DBAL\Connection;
use Vortos\Tenant\Session\TenantGucBinderInterface;
use Vortos\Tenant\TenantContext;

/**
 * Binds the ambient tenant to the PostgreSQL session GUC that Row-Level Security
 * policies read — the glue between {@see TenantContext} (Layer 1) and RLS
 * (Layer 3, see {@see \Vortos\Tenant\Rls\TenantRlsPolicy}) on the DBAL path.
 *
 * Uses set_config(name, value, is_local): bindLocal() is transaction-scoped
 * (auto-cleared on commit/rollback); bindSession() is request/session-scoped and
 * resets to '' when no tenant, so a pooled/worker connection cannot leak the
 * previous request's tenant.
 *
 * Cross-tenant system work should run under a database role granted BYPASSRLS or
 * an explicit permissive policy.
 */
final class TenantSessionBinder implements TenantGucBinderInterface
{
    public function __construct(
        private readonly Connection $connection,
        private readonly TenantContext $tenantContext,
        private readonly string $setting = 'app.current_tenant',
    ) {}

    public function bindSession(): void
    {
        $this->bind($this->resolveTenant(), local: false);
    }

    public function bindLocal(): void
    {
        $this->bind($this->resolveTenant(), local: true);
    }

    private function resolveTenant(): string
    {
        $decision = $this->tenantContext->scopingDecision();

        return ($decision === null || $decision === TenantContext::SYSTEM) ? '' : $decision;
    }

    private function bind(string $value, bool $local): void
    {
        $this->connection->executeStatement(
            'SELECT set_config(?, ?, ?)',
            [$this->setting, $value, $local ? 1 : 0],
        );
    }
}
