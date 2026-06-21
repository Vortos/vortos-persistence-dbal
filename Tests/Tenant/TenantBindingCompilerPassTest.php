<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Tests\Tenant;

use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Vortos\PersistenceDbal\DependencyInjection\Compiler\TenantBindingCompilerPass;
use Vortos\PersistenceDbal\Tenant\TenantSessionBinder;
use Vortos\PersistenceDbal\Transaction\UnitOfWork;
use Vortos\Tenant\Session\TenantGucBinderInterface;
use Vortos\Tenant\TenantContext;

final class TenantBindingCompilerPassTest extends TestCase
{
    public function test_wires_binder_and_unit_of_work_when_tenant_context_present(): void
    {
        $container = new ContainerBuilder();
        $container->register(TenantContext::class, TenantContext::class);
        $container->register(UnitOfWork::class, UnitOfWork::class);

        (new TenantBindingCompilerPass())->process($container);

        $this->assertTrue($container->hasDefinition(TenantSessionBinder::class));
        $this->assertTrue($container->hasAlias(TenantGucBinderInterface::class));
        $this->assertSame(
            TenantSessionBinder::class,
            (string) $container->getDefinition(UnitOfWork::class)->getArgument('$tenantBinder'),
        );
    }

    public function test_no_op_when_tenant_context_absent(): void
    {
        $container = new ContainerBuilder();
        $container->register(UnitOfWork::class, UnitOfWork::class);

        (new TenantBindingCompilerPass())->process($container);

        $this->assertFalse($container->hasDefinition(TenantSessionBinder::class));
        $this->assertFalse($container->hasAlias(TenantGucBinderInterface::class));
    }

    public function test_yields_to_existing_guc_binder_alias(): void
    {
        $container = new ContainerBuilder();
        $container->register(TenantContext::class, TenantContext::class);
        $container->register(UnitOfWork::class, UnitOfWork::class);
        $container->setAlias(TenantGucBinderInterface::class, 'some.other.binder');

        (new TenantBindingCompilerPass())->process($container);

        $this->assertSame('some.other.binder', (string) $container->getAlias(TenantGucBinderInterface::class));
    }
}
