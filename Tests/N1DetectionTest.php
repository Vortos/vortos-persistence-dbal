<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Tests;

use Doctrine\DBAL\Configuration;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Reference;
use Vortos\Http\Request;
use Vortos\Http\Response;
use Vortos\PersistenceDbal\DependencyInjection\DbalPersistenceExtension;
use Vortos\PersistenceDbal\N1Detection\N1DetectionCompilerPass;
use Vortos\PersistenceOrm\DependencyInjection\PersistenceOrmExtension;
use Vortos\PersistenceDbal\N1Detection\N1DetectionListener;
use Vortos\PersistenceDbal\N1Detection\N1DetectorMiddleware;
use Vortos\PersistenceDbal\N1Detection\N1QueryTracker;
use Vortos\Tracing\Contract\TracingInterface;
use Vortos\Tracing\NoOpTracer;

final class N1DetectionTest extends TestCase
{
    // ── N1QueryTracker ─────────────────────────────────────────────────────

    public function test_tracker_normalizes_values_to_same_signature(): void
    {
        $tracker = new N1QueryTracker();

        $tracker->track("SELECT * FROM orders WHERE id = 1");
        $tracker->track("SELECT * FROM orders WHERE id = 2");
        $tracker->track("SELECT * FROM orders WHERE id = 3");

        $violations = $tracker->getViolations(threshold: 3);

        $this->assertCount(1, $violations);
        $this->assertSame(3, $violations[0]['count']);
        $this->assertStringContainsString('orders', $violations[0]['sql']);
    }

    public function test_tracker_does_not_flag_below_threshold(): void
    {
        $tracker = new N1QueryTracker();

        $tracker->track("SELECT * FROM orders WHERE id = 1");
        $tracker->track("SELECT * FROM orders WHERE id = 2");

        $this->assertSame([], $tracker->getViolations(threshold: 3));
    }

    public function test_tracker_treats_different_tables_as_distinct_signatures(): void
    {
        $tracker = new N1QueryTracker();

        foreach (range(1, 5) as $id) {
            $tracker->track("SELECT * FROM orders WHERE id = {$id}");
        }
        foreach (range(1, 5) as $id) {
            $tracker->track("SELECT * FROM users WHERE id = {$id}");
        }

        $violations = $tracker->getViolations(threshold: 3);

        $this->assertCount(2, $violations);
    }

    public function test_tracker_resets_between_requests(): void
    {
        $tracker = new N1QueryTracker();

        $tracker->track("SELECT * FROM orders WHERE id = 1");
        $tracker->track("SELECT * FROM orders WHERE id = 2");
        $tracker->track("SELECT * FROM orders WHERE id = 3");

        $tracker->reset();

        $this->assertSame([], $tracker->getViolations(threshold: 3));
    }

    public function test_tracker_normalizes_in_lists(): void
    {
        $tracker = new N1QueryTracker();

        $tracker->track("SELECT * FROM orders WHERE id IN (1, 2, 3)");
        $tracker->track("SELECT * FROM orders WHERE id IN (4, 5, 6)");
        $tracker->track("SELECT * FROM orders WHERE id IN (7, 8, 9)");

        $violations = $tracker->getViolations(threshold: 3);
        $this->assertCount(1, $violations);
    }

    public function test_tracker_sorts_violations_by_count_descending(): void
    {
        $tracker = new N1QueryTracker();

        foreach (range(1, 3) as $i) {
            $tracker->track("SELECT * FROM users WHERE id = {$i}");
        }
        foreach (range(1, 10) as $i) {
            $tracker->track("SELECT * FROM orders WHERE id = {$i}");
        }

        $violations = $tracker->getViolations(threshold: 3);

        $this->assertSame(10, $violations[0]['count']); // orders first (higher count)
        $this->assertSame(3, $violations[1]['count']);
    }

    // ── N1DetectionListener ────────────────────────────────────────────────

    public function test_listener_adds_response_header_when_violation_detected(): void
    {
        $tracker  = new N1QueryTracker();
        $listener = new N1DetectionListener($tracker, new NullLogger(), threshold: 3);

        $response = $listener->handle(
            Request::create('/'),
            function ($r) use ($tracker): Response {
                foreach (range(1, 5) as $i) {
                    $tracker->track("SELECT * FROM orders WHERE id = {$i}");
                }
                return new Response('', 200);
            },
        );

        $this->assertTrue($response->headers->has('X-Vortos-N1'));
        $this->assertStringContainsString('5x', $response->headers->get('X-Vortos-N1'));
    }

    public function test_listener_does_not_add_header_when_no_violations(): void
    {
        $tracker  = new N1QueryTracker();
        $listener = new N1DetectionListener($tracker, new NullLogger(), threshold: 3);

        $response = $listener->handle(
            Request::create('/'),
            fn($r) => new Response('', 200),
        );

        $this->assertFalse($response->headers->has('X-Vortos-N1'));
    }

    public function test_listener_resets_tracker_before_calling_next(): void
    {
        $tracker = new N1QueryTracker();

        foreach (range(1, 5) as $i) {
            $tracker->track("SELECT * FROM orders WHERE id = {$i}");
        }

        $listener = new N1DetectionListener($tracker, new NullLogger());

        // Capture tracker state inside next() — reset should have already happened
        $trackerAfterReset = null;
        $listener->handle(Request::create('/'), function ($r) use ($tracker, &$trackerAfterReset) {
            $trackerAfterReset = $tracker->getViolations(threshold: 3);
            return new Response();
        });

        $this->assertSame([], $trackerAfterReset);
    }

    public function test_listener_logs_warning_for_each_violation(): void
    {
        $tracker = new N1QueryTracker();

        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->once())
            ->method('warning')
            ->with('N+1 query detected', $this->arrayHasKey('count'));

        $listener = new N1DetectionListener($tracker, $logger, threshold: 3);
        $listener->handle(Request::create('/'), function ($r) use ($tracker): Response {
            foreach (range(1, 5) as $i) {
                $tracker->track("SELECT * FROM orders WHERE id = {$i}");
            }
            return new Response();
        });
    }

    // ── Compiler pass — dev-only gate ──────────────────────────────────────

    public function test_compiler_pass_registers_services_in_dev(): void
    {
        $container = $this->buildContainer('dev');

        $this->assertTrue($container->hasDefinition(N1QueryTracker::class));
        $this->assertTrue($container->hasDefinition(N1DetectorMiddleware::class));
        $this->assertTrue($container->hasDefinition(N1DetectionListener::class));
    }

    public function test_compiler_pass_does_not_register_services_in_prod(): void
    {
        $container = $this->buildContainer('prod');

        $this->assertFalse($container->hasDefinition(N1QueryTracker::class));
        $this->assertFalse($container->hasDefinition(N1DetectorMiddleware::class));
        $this->assertFalse($container->hasDefinition(N1DetectionListener::class));
    }

    public function test_compiler_pass_appends_middleware_to_dbal_configuration_in_dev(): void
    {
        $container = $this->buildContainer('dev');
        $calls     = $container->getDefinition(Configuration::class)->getMethodCalls();

        $middlewares = [];
        foreach ($calls as [$method, $args]) {
            if ($method === 'setMiddlewares') {
                $middlewares = array_merge($middlewares, $args[0]);
            }
        }

        $ids = array_map(static fn ($ref) => (string) $ref, $middlewares);
        $this->assertContains(N1DetectorMiddleware::class, $ids);
    }

    public function test_compiler_pass_does_not_modify_dbal_configuration_in_prod(): void
    {
        $container = $this->buildContainer('prod');
        $calls     = $container->getDefinition(Configuration::class)->getMethodCalls();

        $middlewares = [];
        foreach ($calls as [$method, $args]) {
            if ($method === 'setMiddlewares') {
                $middlewares = array_merge($middlewares, $args[0]);
            }
        }

        $ids = array_map(static fn ($ref) => (string) $ref, $middlewares);
        $this->assertNotContains(N1DetectorMiddleware::class, $ids);
    }

    // ── ORM path ───────────────────────────────────────────────────────────

    public function test_compiler_pass_registers_services_via_orm_path_in_dev(): void
    {
        $container = $this->buildOrmContainer('dev');

        $this->assertTrue($container->hasDefinition(N1QueryTracker::class));
        $this->assertTrue($container->hasDefinition(N1DetectorMiddleware::class));
        $this->assertTrue($container->hasDefinition(N1DetectionListener::class));
    }

    public function test_compiler_pass_injects_middleware_into_entity_manager_factory_args_in_dev(): void
    {
        $container = $this->buildOrmContainer('dev');

        $emDef = $container->getDefinition(\Doctrine\ORM\EntityManager::class);
        $args  = $emDef->getArguments();

        // Index 4 is the $middlewares array passed to EntityManagerFactory::fromDsn()
        $this->assertIsArray($args[4]);
        $this->assertNotEmpty($args[4]);
        $this->assertSame(N1DetectorMiddleware::class, (string) $args[4][0]);
    }

    public function test_compiler_pass_does_not_inject_middleware_into_orm_in_prod(): void
    {
        $container = $this->buildOrmContainer('prod');

        $emDef = $container->getDefinition(\Doctrine\ORM\EntityManager::class);
        $args  = $emDef->getArguments();

        $this->assertArrayNotHasKey(4, $args);
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private function buildOrmContainer(string $env): ContainerBuilder
    {
        $container = new ContainerBuilder();
        $container->setParameter('kernel.env', $env);
        $container->setParameter('kernel.project_dir', sys_get_temp_dir());
        $container->setParameter('vortos.persistence.write_dsn', 'sqlite:///:memory:');

        $container->register(LoggerInterface::class, NullLogger::class)->setPublic(true);

        (new PersistenceOrmExtension())->load([], $container);
        (new N1DetectionCompilerPass())->process($container);

        return $container;
    }

    private function buildContainer(string $env): ContainerBuilder
    {
        $container = new ContainerBuilder();
        $container->setParameter('kernel.env', $env);
        $container->setParameter('vortos.persistence.write_dsn', 'sqlite:///:memory:');
        $container->setParameter('vortos.persistence.framework_table_mode', 'prefix');

        $container->register(LoggerInterface::class, NullLogger::class)->setPublic(true);
        $container->register(TracingInterface::class, NoOpTracer::class)->setPublic(true);

        (new DbalPersistenceExtension())->load([], $container);
        (new N1DetectionCompilerPass())->process($container);

        return $container;
    }
}
