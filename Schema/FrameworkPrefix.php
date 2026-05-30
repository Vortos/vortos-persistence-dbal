<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Schema;

/**
 * Resolves framework-owned table names to their platform-specific qualified form.
 *
 * PostgreSQL: vortos.<table>   — tables live in the 'vortos' schema
 * All others: vortos_<table>   — tables live in the default schema with a prefix
 *
 * The prefix is determined once from the DSN at container compile time via
 * DbalPersistenceExtension, which sets the 'vortos.db.framework_table_prefix'
 * container parameter. No runtime branching occurs.
 */
final class FrameworkPrefix
{
    private function __construct() {}

    /**
     * Derive the framework table prefix from a DSN string.
     *
     * PostgreSQL defaults to schema mode ('vortos.'). Add ?vortos_prefix=true
     * to the DSN to use underscore prefix mode instead ('vortos_'):
     *
     *   pgsql://user:pass@localhost/mydb?vortos_prefix=true
     *
     * All other engines always use 'vortos_'.
     */
    public static function fromDsn(string $dsn): string
    {
        if (str_starts_with($dsn, 'pgsql://') || str_starts_with($dsn, 'postgres://')) {
            parse_str(parse_url($dsn, PHP_URL_QUERY) ?? '', $params);
            return ($params['vortos_prefix'] ?? null) === 'true' ? 'vortos_' : 'vortos.';
        }

        return 'vortos_';
    }

    /**
     * Apply a prefix to a bare table name.
     */
    public static function apply(string $prefix, string $table): string
    {
        return $prefix . $table;
    }
}
