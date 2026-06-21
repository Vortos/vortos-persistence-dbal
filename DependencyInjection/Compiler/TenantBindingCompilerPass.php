<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\DependencyInjection\Compiler;

use Doctrine\DBAL\Connection;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Reference;
use Vortos\PersistenceDbal\Tenant\TenantSessionBinder;
use Vortos\PersistenceDbal\Transaction\UnitOfWork;
use Vortos\Tenant\Session\TenantGucBinderInterface;
use Vortos\Tenant\TenantContext;

/**
 * Wires Postgres RLS tenant binding when the tenant package is installed.
 *
 * Lives in a compiler pass (DbalPersistencePackage::build) rather than
 * DbalPersistenceExtension::load because a has(TenantContext::class) check inside
 * load() runs against the isolated per-extension merge container, where
 * TenantContext (owned by vortos-tenant) is never present. A compiler pass sees
 * the fully merged container, so the check is valid.
 */
final class TenantBindingCompilerPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container): void
    {
        if (!$container->has(TenantContext::class)
            || !$container->hasDefinition(UnitOfWork::class)) {
            return;
        }

        $container->register(TenantSessionBinder::class, TenantSessionBinder::class)
            ->setArgument('$connection', new Reference(Connection::class))
            ->setArgument('$tenantContext', new Reference(TenantContext::class))
            ->setShared(true)
            ->setPublic(false);

        // Expose via the shared interface unless an ORM binder already claimed it.
        if (!$container->hasAlias(TenantGucBinderInterface::class)
            && !$container->hasDefinition(TenantGucBinderInterface::class)) {
            $container->setAlias(TenantGucBinderInterface::class, TenantSessionBinder::class)->setPublic(true);
        }

        $container->getDefinition(UnitOfWork::class)
            ->setArgument('$tenantBinder', new Reference(TenantSessionBinder::class));
    }
}
