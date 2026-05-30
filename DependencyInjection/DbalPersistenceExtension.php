<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\DependencyInjection;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Extension\Extension;
use Symfony\Component\DependencyInjection\Reference;
use Psr\Log\LoggerInterface;
use Vortos\Persistence\Transaction\UnitOfWorkInterface;
use Vortos\PersistenceDbal\Connection\ConnectionFactory;
use Vortos\PersistenceDbal\Health\DatabaseHealthCheck;
use Vortos\PersistenceDbal\Logging\LoggingDbalMiddleware;
use Vortos\PersistenceDbal\Schema\FrameworkPrefix;
use Vortos\PersistenceDbal\Tracing\TracingDbalMiddleware;
use Vortos\PersistenceDbal\Transaction\UnitOfWork;
use Vortos\Tracing\Contract\TracingInterface;

/**
 * Wires DBAL-specific services.
 *
 * Reads the write DSN from the vortos.persistence.write_dsn parameter
 * set by PersistenceExtension — which must be loaded first.
 *
 * ## What this extension registers
 *
 *   Connection::class      — shared DBAL connection, built via ConnectionFactory::fromDsn()
 *   UnitOfWork::class      — transaction boundary, injected with Connection
 *   UnitOfWorkInterface    — aliased to UnitOfWork::class
 *
 * ## Why Connection must be shared
 *
 * Connection::class is registered with setShared(true) (the DBAL default, explicit here).
 * OutboxWriter, DeadLetterWriter, TransactionalMiddleware, and UnitOfWork
 * must ALL use the SAME Connection instance. If they used different instances,
 * each would have its own transaction — the outbox write and aggregate save
 * would be in separate transactions and could not be atomic.
 *
 * Never change this to setShared(false).
 *
 * ## Why Connection is public
 *
 * Connection::class is registered as public so that MessagingExtension
 * can reference it via new Reference(Connection::class). Without public,
 * cross-package references fail at compile time.
 */
final class DbalPersistenceExtension extends Extension
{
    public function getAlias(): string
    {
        return 'vortos_persistence_dbal';
    }

    public function load(array $configs, ContainerBuilder $container): void
    {
        $dsn = (string) $container->getParameter('vortos.persistence.write_dsn');

        $container->setParameter('vortos.db.framework_table_prefix', FrameworkPrefix::fromDsn($dsn));

        if (!$container->hasParameter('vortos.persistence.slow_query_threshold_ms')) {
            $container->setParameter('vortos.persistence.slow_query_threshold_ms', 100);
        }

        // TracingDbalMiddleware — wraps every DBAL query in a span.
        // TracingInterface is always registered (defaults to NoOpTracer).
        $container->register(TracingDbalMiddleware::class, TracingDbalMiddleware::class)
            ->setArgument('$tracer', new Reference(TracingInterface::class))
            ->setShared(true)
            ->setPublic(false);

        // LoggingDbalMiddleware — logs slow queries and errors.
        // LoggerInterface is always available in Symfony.
        $container->register(LoggingDbalMiddleware::class, LoggingDbalMiddleware::class)
            ->setArgument('$logger', new Reference(LoggerInterface::class))
            ->setArgument('$slowQueryThresholdMs', '%vortos.persistence.slow_query_threshold_ms%')
            ->setShared(true)
            ->setPublic(false);

        // DBAL Configuration with tracing and logging middleware attached
        $container->register(Configuration::class, Configuration::class)
            ->addMethodCall('setMiddlewares', [[
                new Reference(TracingDbalMiddleware::class),
                new Reference(LoggingDbalMiddleware::class),
            ]])
            ->setShared(true)
            ->setPublic(false);

        $container->register(Connection::class, Connection::class)
            ->setFactory([ConnectionFactory::class, 'fromDsn'])
            ->setArguments([$dsn, new Reference(Configuration::class)])
            ->setShared(true)
            ->setPublic(true)
            ->setLazy(true);

        $container->register(UnitOfWork::class, UnitOfWork::class)
            ->setArgument('$connection', new Reference(Connection::class))
            ->setPublic(false);

        $container->setAlias(UnitOfWorkInterface::class, UnitOfWork::class)
            ->setPublic(false);

        $container->register(DatabaseHealthCheck::class, DatabaseHealthCheck::class)
            ->setArgument('$connection', new Reference(Connection::class))
            ->setPublic(false);
    }
}
