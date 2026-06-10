<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Tests;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Vortos\PersistenceDbal\Connection\ConnectionFactory;
use Vortos\PersistenceDbal\DependencyInjection\DbalPersistenceExtension;

final class DbalPersistenceExtensionTest extends TestCase
{
    public function test_schema_mode_sets_dot_prefix(): void
    {
        $container = $this->makeContainer('pgsql://postgres:secret@write_db:5432/demo', 'schema');

        (new DbalPersistenceExtension())->load([], $container);

        $this->assertSame('vortos.', $container->getParameter('vortos.db.framework_table_prefix'));
    }

    public function test_prefix_mode_sets_underscore_prefix(): void
    {
        $container = $this->makeContainer('mysql://root:secret@db:3306/demo', 'prefix');

        (new DbalPersistenceExtension())->load([], $container);

        $this->assertSame('vortos_', $container->getParameter('vortos.db.framework_table_prefix'));
    }

    public function test_explicit_mode_is_not_overridden_by_dsn(): void
    {
        // PostgreSQL DSN would infer 'schema' via fromDsn, but explicit 'prefix' wins.
        $container = $this->makeContainer('pgsql://postgres:secret@write_db:5432/demo', 'prefix');

        (new DbalPersistenceExtension())->load([], $container);

        $this->assertSame('vortos_', $container->getParameter('vortos.db.framework_table_prefix'));
    }

    public function test_throws_with_actionable_message_when_mode_not_set(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('vortos.persistence.write_dsn', 'pgsql://postgres:secret@db:5432/app');
        // framework_table_mode intentionally not set

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('frameworkTableMode');

        (new DbalPersistenceExtension())->load([], $container);
    }

    public function test_throws_when_mode_is_null(): void
    {
        $container = $this->makeContainer('pgsql://postgres:secret@db:5432/app', null);

        $this->expectException(\RuntimeException::class);
        (new DbalPersistenceExtension())->load([], $container);
    }

    public function test_empty_dsn_is_allowed_for_setup_bootstrap(): void
    {
        // The setup wizard may compile the container before the DSN env var exists.
        // The mode is still required — setup always knows it.
        $container = $this->makeContainer('', 'schema');

        (new DbalPersistenceExtension())->load([], $container);

        $this->assertSame('', $container->getDefinition(Connection::class)->getArgument(0));
        $this->assertSame('vortos.', $container->getParameter('vortos.db.framework_table_prefix'));
    }

    public function test_connection_factory_requires_vortos_write_db_dsn_at_runtime(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('VORTOS_WRITE_DB_DSN');

        ConnectionFactory::fromDsn('');
    }

    public function test_uses_vortos_write_db_dsn_parameter_for_connection_factory(): void
    {
        $container = $this->makeContainer('pgsql://postgres:secret@write_db:5432/demo', 'schema');

        (new DbalPersistenceExtension())->load([], $container);

        $this->assertSame(
            'pgsql://postgres:secret@write_db:5432/demo',
            $container->getDefinition(Connection::class)->getArgument(0),
        );
    }

    private function makeContainer(string $dsn, ?string $mode): ContainerBuilder
    {
        $container = new ContainerBuilder();
        $container->setParameter('vortos.persistence.write_dsn', $dsn);
        $container->setParameter('vortos.persistence.framework_table_mode', $mode);
        return $container;
    }
}
