<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\DependencyInjection\Compiler;

use Doctrine\ORM\EntityManager;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Reference;
use Vortos\PersistenceDbal\Logging\LoggingDbalMiddleware;
use Vortos\PersistenceDbal\Tracing\TracingDbalMiddleware;

/**
 * Carries the DBAL query middlewares across to the ORM's connection.
 *
 * DbalPersistenceExtension attaches TracingDbalMiddleware and LoggingDbalMiddleware to the DBAL
 * `Configuration` service. That is the right home for them — but when vortos-persistence-orm is
 * also installed it re-registers `Doctrine\DBAL\Connection` as `EntityManager::getConnection()`,
 * so the connection every query actually runs through is built by the ORM and never sees that
 * Configuration. The DBAL one ends up referenced by nothing and is dropped from the compiled
 * container, taking both middlewares with it.
 *
 * The symptom is a trace that looks fine until you need it. Requests produced an `http.GET` span
 * and cache spans and nothing else: no `db.query` children at all, so "which query made this
 * endpoint slow" was unanswerable on an installation running ~1300 queries a minute. Slow-query
 * logging was equally silent, for the same reason and with no error to hint at it.
 *
 * Appending rather than assigning is deliberate: PersistenceMetricsCompilerPass and
 * N1DetectionCompilerPass inject into the same argument, and an assignment means whichever pass
 * runs last silently discards the others.
 */
final class DbalMiddlewareOrmBridgePass implements CompilerPassInterface
{
    /** EntityManagerFactory::fromDsn($dsn, $entityPaths, $devMode, $metadataCache, $middlewares) */
    private const ORM_MIDDLEWARE_ARG_INDEX = 4;

    public function process(ContainerBuilder $container): void
    {
        if (!$container->hasDefinition(EntityManager::class)) {
            return;
        }

        $middlewares = [];

        foreach ([TracingDbalMiddleware::class, LoggingDbalMiddleware::class] as $id) {
            if ($container->hasDefinition($id)) {
                $middlewares[] = new Reference($id);
            }
        }

        if ($middlewares === []) {
            return;
        }

        $emDef = $container->getDefinition(EntityManager::class);
        $args = $emDef->getArguments();

        while (count($args) < self::ORM_MIDDLEWARE_ARG_INDEX) {
            $args[] = null;
        }

        $existing = $args[self::ORM_MIDDLEWARE_ARG_INDEX] ?? [];

        if (!is_array($existing)) {
            $existing = [];
        }

        $args[self::ORM_MIDDLEWARE_ARG_INDEX] = array_merge($existing, $middlewares);
        $emDef->setArguments($args);
    }
}
