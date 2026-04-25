<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\DependencyInjection;

use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Extension\ExtensionInterface;
use Vortos\Foundation\Contract\PackageInterface;

/**
 * DBAL persistence package.
 *
 * Registers DbalPersistenceExtension with the container.
 * Include this package in Container.php when using PostgreSQL
 * (or any other DBAL-supported database) for the write side.
 *
 * If you use a different write-side adapter (e.g. custom MySQL adapter),
 * omit this package and register your own Connection and UnitOfWork instead.
 */
final class DbalPersistencePackage implements PackageInterface
{
    public function getContainerExtension(): ?ExtensionInterface
    {
        return new DbalPersistenceExtension();
    }

    public function build(ContainerBuilder $container): void
    {
        // No compiler passes needed.
    }
}
