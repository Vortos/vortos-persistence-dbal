<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Tests\Schema;

use PHPUnit\Framework\TestCase;
use Vortos\PersistenceDbal\Schema\FrameworkPrefix;

final class FrameworkPrefixTest extends TestCase
{
    /** @dataProvider postgresDataProvider */
    public function test_postgres_dsns_produce_schema_prefix(string $dsn): void
    {
        $this->assertSame('vortos.', FrameworkPrefix::fromDsn($dsn));
    }

    public static function postgresDataProvider(): array
    {
        return [
            'pgsql scheme'    => ['pgsql://user:pass@localhost:5432/mydb'],
            'postgres scheme' => ['postgres://user:pass@localhost:5432/mydb'],
            'pgsql no auth'   => ['pgsql://localhost/mydb'],
        ];
    }

    public function test_postgres_with_vortos_prefix_true_produces_underscore_prefix(): void
    {
        $this->assertSame('vortos_', FrameworkPrefix::fromDsn('pgsql://user:pass@localhost/mydb?vortos_prefix=true'));
        $this->assertSame('vortos_', FrameworkPrefix::fromDsn('postgres://user:pass@localhost/mydb?vortos_prefix=true'));
    }

    public function test_postgres_with_vortos_prefix_false_still_produces_schema_prefix(): void
    {
        $this->assertSame('vortos.', FrameworkPrefix::fromDsn('pgsql://user:pass@localhost/mydb?vortos_prefix=false'));
    }

    public function test_postgres_with_other_query_params_still_produces_schema_prefix(): void
    {
        $this->assertSame('vortos.', FrameworkPrefix::fromDsn('pgsql://user:pass@localhost/mydb?sslmode=require'));
        $this->assertSame('vortos.', FrameworkPrefix::fromDsn('pgsql://user:pass@localhost/mydb?sslmode=require&vortos_prefix=false'));
    }

    /** @dataProvider nonPostgresDataProvider */
    public function test_non_postgres_dsns_produce_underscore_prefix(string $dsn): void
    {
        $this->assertSame('vortos_', FrameworkPrefix::fromDsn($dsn));
    }

    public static function nonPostgresDataProvider(): array
    {
        return [
            'mysql'   => ['mysql://user:pass@localhost/mydb'],
            'sqlite'  => ['sqlite:///tmp/test.db'],
            'sqlsrv'  => ['sqlsrv://user:pass@localhost/mydb'],
            'oci8'    => ['oci8://user:pass@localhost/mydb'],
        ];
    }

    public function test_apply_concatenates_prefix_and_table(): void
    {
        $this->assertSame('vortos.user_roles', FrameworkPrefix::apply('vortos.', 'user_roles'));
        $this->assertSame('vortos_user_roles', FrameworkPrefix::apply('vortos_', 'user_roles'));
    }

    public function test_apply_preserves_table_name_exactly(): void
    {
        $this->assertSame('vortos.outbox', FrameworkPrefix::apply('vortos.', 'outbox'));
        $this->assertSame('vortos_outbox', FrameworkPrefix::apply('vortos_', 'outbox'));
    }

    public function test_from_mode_schema_returns_dot_prefix(): void
    {
        $this->assertSame('vortos.', FrameworkPrefix::fromMode('schema'));
    }

    public function test_from_mode_prefix_returns_underscore_prefix(): void
    {
        $this->assertSame('vortos_', FrameworkPrefix::fromMode('prefix'));
    }

    public function test_from_mode_rejects_unknown_mode(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('"auto"');
        FrameworkPrefix::fromMode('auto');
    }
}
