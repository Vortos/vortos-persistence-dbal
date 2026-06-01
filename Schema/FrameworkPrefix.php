<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Schema;

/**
 * Resolves framework-owned table names to their platform-specific qualified form.
 *
 * Two modes:
 *   'schema' — PostgreSQL schema: vortos.<table>  (tables live in the 'vortos' schema)
 *   'prefix' — Underscore prefix:  vortos_<table> (tables live in the default schema)
 *
 * The mode is set explicitly in config/persistence.php via
 * VortosPersistenceConfig::frameworkTableMode() and compiled into the
 * 'vortos.db.framework_table_prefix' container parameter once at build time.
 * No runtime branching or environment variable reading occurs.
 */
final class FrameworkPrefix
{
    private function __construct() {}

    /**
     * Resolve the framework prefix from an explicit mode string.
     *
     * This is the primary path. The mode is declared in config/persistence.php:
     *
     *   $config->frameworkTableMode('schema');  // PostgreSQL schema mode
     *   $config->frameworkTableMode('prefix');  // Underscore prefix mode
     */
    public static function fromMode(string $mode): string
    {
        return match ($mode) {
            'schema' => 'vortos.',
            'prefix' => 'vortos_',
            default  => throw new \InvalidArgumentException(
                sprintf('Unknown framework table mode "%s". Must be "schema" or "prefix".', $mode)
            ),
        };
    }

    /**
     * Derive the framework prefix from a DSN string.
     *
     * Used by tooling (e.g. vortos:setup) to suggest the correct mode when
     * generating config/persistence.php. Not called during container compilation.
     *
     * PostgreSQL defaults to schema mode ('vortos.'). Add ?vortos_prefix=true
     * to use underscore prefix mode instead:
     *
     *   pgsql://user:pass@localhost/mydb?vortos_prefix=true
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
