<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\DependencyInjection\Compiler;

use Doctrine\DBAL\Connection;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;
use Vortos\PersistenceDbal\Attribute\UsesDbalMapper;
use Vortos\Tenant\TenantContext;
use Vortos\Tenant\TenantScopeResolver;

/**
 * Auto-wires DbalStore into repositories that declare #[UsesDbalMapper].
 *
 * For each service definition whose class carries #[UsesDbalMapper]:
 *   1. Registers a named mapper service: vortos.dbal_mapper.<RepositoryClass>
 *   2. Registers a named DbalStore service: vortos.dbal_store.<RepositoryClass>
 *      — wired with the shared Connection and the mapper
 *   3. Sets the store as the $store constructor argument of the repository
 *
 * Users never configure DI manually — the attribute and the repository
 * constructor (accepting DbalStore $store) are the entire public API.
 *
 * Runs at TYPE_BEFORE_OPTIMIZATION priority 8 — before tracing/metrics passes.
 */
final class DbalRepositoryCompilerPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container): void
    {
        if (!$container->hasDefinition(Connection::class)) {
            return;
        }

        foreach ($container->getDefinitions() as $serviceId => $definition) {
            $className = $definition->getClass() ?? $serviceId;

            if (!class_exists($className)) {
                continue;
            }

            $reflClass = new \ReflectionClass($className);
            $attrs     = $reflClass->getAttributes(UsesDbalMapper::class);

            if (empty($attrs)) {
                continue;
            }

            /** @var UsesDbalMapper $attr */
            $attr        = $attrs[0]->newInstance();
            $mapperClass = $attr->mapperClass;
            $storeClass  = $attr->storeClass;

            $mapperId = 'vortos.dbal_mapper.' . $className;
            if (!$container->hasDefinition($mapperId)) {
                $container->setDefinition($mapperId, (new Definition($mapperClass))
                    ->setShared(true)
                    ->setPublic(false));
            }

            $storeId = 'vortos.dbal_store.' . $className;
            $storeDefinition = (new Definition($storeClass))
                ->setArgument('$connection', new Reference(Connection::class))
                ->setArgument('$mapper', new Reference($mapperId))
                ->setShared(true)
                ->setPublic(false);

            // Tenant scoping: when the repository is #[TenantScoped], the store
            // stamps and filters writes by the ambient tenant.
            $tenantColumn = TenantScopeResolver::columnFor($className);
            if ($tenantColumn !== null && $container->has(TenantContext::class)) {
                $storeDefinition
                    ->setArgument('$tenantContext', new Reference(TenantContext::class))
                    ->setArgument('$tenantColumn', $tenantColumn);
            }

            $container->setDefinition($storeId, $storeDefinition);

            $definition->setArgument('$store', new Reference($storeId));
        }
    }
}
