<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Tests\DependencyInjection;

use Doctrine\ORM\EntityManager;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Reference;
use Vortos\PersistenceDbal\DependencyInjection\Compiler\DbalMiddlewareOrmBridgePass;
use Vortos\PersistenceDbal\Logging\LoggingDbalMiddleware;
use Vortos\PersistenceDbal\Tracing\TracingDbalMiddleware;

final class DbalMiddlewareOrmBridgePassTest extends TestCase
{
    private function container(bool $withMiddlewares = true): ContainerBuilder
    {
        $c = new ContainerBuilder();

        if ($withMiddlewares) {
            $c->register(TracingDbalMiddleware::class, TracingDbalMiddleware::class);
            $c->register(LoggingDbalMiddleware::class, LoggingDbalMiddleware::class);
        }

        return $c;
    }

    /** @return list<string> */
    private function ormMiddlewares(ContainerBuilder $c): array
    {
        $mw = $c->getDefinition(EntityManager::class)->getArguments()[4] ?? [];

        return array_map(static fn (Reference $r): string => (string) $r, is_array($mw) ? $mw : []);
    }

    /**
     * The regression: without this, a request produced an http span and cache spans and no
     * `db.query` children at all, so "which query made this slow" could not be answered.
     */
    public function test_carries_the_dbal_middlewares_onto_the_orm_connection(): void
    {
        $c = $this->container();
        $c->register(EntityManager::class, EntityManager::class)->setArguments(['dsn', [], false, null]);

        (new DbalMiddlewareOrmBridgePass())->process($c);

        $mw = $this->ormMiddlewares($c);
        self::assertContains(TracingDbalMiddleware::class, $mw);
        self::assertContains(LoggingDbalMiddleware::class, $mw);
    }

    public function test_appends_rather_than_replacing_middleware_another_pass_added(): void
    {
        $c = $this->container();
        $c->register(EntityManager::class, EntityManager::class)
            ->setArguments(['dsn', [], false, null, [new Reference('metrics.middleware')]]);

        (new DbalMiddlewareOrmBridgePass())->process($c);

        $mw = $this->ormMiddlewares($c);
        self::assertContains('metrics.middleware', $mw);
        self::assertContains(TracingDbalMiddleware::class, $mw);
    }

    public function test_pads_the_argument_list_when_optional_arguments_were_omitted(): void
    {
        $c = $this->container();
        $c->register(EntityManager::class, EntityManager::class)->setArguments(['dsn', []]);

        (new DbalMiddlewareOrmBridgePass())->process($c);

        self::assertContains(TracingDbalMiddleware::class, $this->ormMiddlewares($c));
    }

    public function test_does_nothing_without_the_orm(): void
    {
        $c = $this->container();

        (new DbalMiddlewareOrmBridgePass())->process($c);

        self::assertFalse($c->hasDefinition(EntityManager::class));
    }

    public function test_does_nothing_when_no_middleware_is_registered(): void
    {
        $c = $this->container(withMiddlewares: false);
        $c->register(EntityManager::class, EntityManager::class)->setArguments(['dsn', [], false, null]);

        (new DbalMiddlewareOrmBridgePass())->process($c);

        self::assertSame([], $this->ormMiddlewares($c));
    }
}
