<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\N1Detection;

use Doctrine\DBAL\Configuration;
use Doctrine\ORM\EntityManager;
use Psr\Log\LoggerInterface;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Reference;

/**
 * Registers N+1 query detection services and injects the DBAL middleware.
 *
 * ONLY runs when kernel.env = dev — all services are absent in production.
 * No overhead whatsoever outside of dev.
 *
 * Handles two exclusive persistence paths:
 *   DBAL path — appends to Doctrine\DBAL\Configuration::setMiddlewares()
 *   ORM path  — injects middlewares into EntityManagerFactory::fromDsn() args
 *
 * Registered from both DbalPersistencePackage and PersistenceOrmPackage —
 * only the relevant branch executes depending on which package is active.
 */
final class N1DetectionCompilerPass implements CompilerPassInterface
{
    private const THRESHOLD = 3;

    public function process(ContainerBuilder $container): void
    {
        $env = $container->hasParameter('kernel.env')
            ? $container->getParameter('kernel.env')
            : 'prod';

        if ($env !== 'dev') {
            return;
        }

        $hasDbal = $container->hasDefinition(Configuration::class);
        $hasOrm  = $container->hasDefinition(EntityManager::class);

        if (!$hasDbal && !$hasOrm) {
            return;
        }

        $this->registerSharedServices($container);

        if ($hasDbal) {
            $this->injectIntoDbal($container);
        } else {
            $this->injectIntoOrm($container);
        }
    }

    private function registerSharedServices(ContainerBuilder $container): void
    {
        // Idempotent — both packages may call this pass; only register once.
        if ($container->hasDefinition(N1QueryTracker::class)) {
            return;
        }

        $container->register(N1QueryTracker::class, N1QueryTracker::class)
            ->setShared(true)
            ->setPublic(false);

        $container->register(N1DetectorMiddleware::class, N1DetectorMiddleware::class)
            ->setArgument('$tracker', new Reference(N1QueryTracker::class))
            ->setShared(true)
            ->setPublic(false);

        $container->register(N1DetectionListener::class, N1DetectionListener::class)
            ->setArguments([
                new Reference(N1QueryTracker::class),
                new Reference(LoggerInterface::class),
                self::THRESHOLD,
            ])
            ->addTag('kernel.event_subscriber')
            ->setPublic(false);
    }

    private function injectIntoDbal(ContainerBuilder $container): void
    {
        $configDef = $container->getDefinition(Configuration::class);
        $calls     = $configDef->getMethodCalls();

        foreach ($calls as $i => [$method, $args]) {
            if ($method === 'setMiddlewares' && isset($args[0]) && is_array($args[0])) {
                $args[0][] = new Reference(N1DetectorMiddleware::class);
                $calls[$i] = [$method, $args];
                $configDef->setMethodCalls($calls);
                return;
            }
        }

        $configDef->addMethodCall('setMiddlewares', [[new Reference(N1DetectorMiddleware::class)]]);
    }

    private function injectIntoOrm(ContainerBuilder $container): void
    {
        // EntityManagerFactory::fromDsn($dsn, $entityPaths, $devMode, $metadataCache, $middlewares)
        // The middlewares array is the 5th argument (index 4).
        $emDef = $container->getDefinition(EntityManager::class);
        $args  = $emDef->getArguments();

        // Pad to index 4 if metadataCache (index 3) was omitted
        while (count($args) < 4) {
            $args[] = null;
        }

        $args[4] = [new Reference(N1DetectorMiddleware::class)];
        $emDef->setArguments($args);
    }
}
