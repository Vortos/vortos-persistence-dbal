<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Tenant;

use Doctrine\DBAL\Connection;
use Vortos\Tenant\TenantContext;

/**
 * Binds the ambient tenant to the PostgreSQL session GUC that Row-Level Security
 * policies read — the glue between {@see TenantContext} (Layer 1) and RLS
 * (Layer 3, see {@see \Vortos\Tenant\Rls\TenantRlsPolicy}).
 *
 * Uses set_config(name, value, is_local=true), so the value is scoped to the
 * current transaction and is cleared automatically on commit/rollback — safe for
 * pooled and worker-mode connections.
 *
 * When the context carries no concrete tenant (unset, or system scope) the GUC
 * is left unset; with `current_setting('app.current_tenant', true)` returning
 * NULL, the isolation policy then matches no rows — fail closed. Cross-tenant
 * system work should run under a database role that is granted BYPASSRLS or an
 * explicit permissive policy.
 */
final class TenantSessionBinder
{
    public function __construct(
        private readonly Connection $connection,
        private readonly TenantContext $tenantContext,
        private readonly string $setting = 'app.current_tenant',
    ) {}

    /**
     * Bind the current tenant for the duration of the active transaction.
     * Call this immediately after BEGIN.
     */
    public function bindLocal(): void
    {
        $decision = $this->tenantContext->scopingDecision();

        if ($decision === null || $decision === TenantContext::SYSTEM) {
            return;
        }

        $this->connection->executeStatement(
            'SELECT set_config(?, ?, true)',
            [$this->setting, $decision],
        );
    }
}
