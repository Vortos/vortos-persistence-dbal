<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\Connection;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Exception as DbalException;
use Doctrine\DBAL\Tools\DsnParser;

/**
 * Builds Doctrine DBAL Connection instances from DSN strings or parameter arrays.
 *
 * This is a pure static factory — it cannot be instantiated.
 * It has one responsibility: construct a configured Connection object.
 * Connection resilience (ping, reconnect) is the UnitOfWork's responsibility, not this class.
 *
 * Connections are lazy — no TCP handshake occurs until the first query is executed.
 * This means constructing a Connection never fails due to the database being unreachable.
 *
 * ## Standard usage (12-factor apps)
 *
 * Set a single DATABASE_URL environment variable:
 *
 *   DATABASE_URL=pgsql://postgres:secret@write_db:5432/squaura
 *
 * Then in config/persistence.php:
 *
 *   $config->writeDsn($_ENV['DATABASE_URL']);
 *
 * ## Supported DSN drivers
 *
 * - pgsql://   → PostgreSQL via pdo_pgsql
 * - mysql://   → MySQL/MariaDB via pdo_mysql
 * - sqlite:/// → SQLite via pdo_sqlite
 * - sqlsrv://  → SQL Server via pdo_sqlsrv
 * - oci8://    → Oracle via oci8
 *
 * ## Advanced usage
 *
 * For SSL certificates, unix sockets, read replica routing, or custom
 * DBAL driver options, use fromParams() with the full DBAL params array.
 * See: https://www.doctrine-project.org/projects/doctrine-dbal/en/current/reference/configuration.html
 */
final class ConnectionFactory
{
    /**
     * Prevents instantiation — this class is a static factory only.
     */
    private function __construct() {}

    /**
     * Build a Connection from a DSN string.
     *
     * This is the standard path for all production environments.
     * The DSN is passed to DBAL's DriverManager under the 'url' key,
     * which is DBAL's documented convention for DSN-based configuration.
     *
     * Note: the key is 'url', not 'dsn' — a common mistake when using
     * DriverManager directly. This factory handles that detail for you.
     *
     * @param string $dsn Full DSN string, e.g. 'pgsql://user:pass@host:5432/dbname'
     *
     * @throws DbalException If the DSN is malformed or the driver is unsupported
     */
    public static function fromDsn(string $dsn): Connection
    {
        $parser = new DsnParser([
            'pgsql'    => 'pdo_pgsql',
            'postgres' => 'pdo_pgsql',
            'mysql'    => 'pdo_mysql',
            'sqlite'   => 'pdo_sqlite',
            'sqlsrv'   => 'pdo_sqlsrv',
            'oci8'     => 'oci8',
        ]);

        $params = $parser->parse($dsn);

        return DriverManager::getConnection($params);
    }

    /**
     * Build a Connection from a raw DBAL params array.
     *
     * Use this when a DSN string cannot express the full configuration.
     * Common cases:
     *   - SSL/TLS certificates for AWS RDS or Google Cloud SQL
     *   - Unix socket connections
     *   - Custom PDO attributes
     *   - Wrapper connections for read replica routing
     *
     * The params array is passed directly to DBAL's DriverManager::getConnection().
     * Full reference: https://www.doctrine-project.org/projects/doctrine-dbal/en/current/reference/configuration.html
     *
     * Example with SSL:
     *   ConnectionFactory::fromParams([
     *       'driver'  => 'pdo_pgsql',
     *       'host'    => 'db.example.com',
     *       'user'    => 'app',
     *       'password'=> 'secret',
     *       'dbname'  => 'myapp',
     *       'driverOptions' => [
     *           \PDO::MYSQL_ATTR_SSL_CA => '/path/to/ca.pem',
     *       ],
     *   ]);
     *
     * @param array $params DBAL DriverManager params array
     *
     * @throws DbalException If the params are invalid or the driver is unsupported
     */
    public static function fromParams(array $params): Connection
    {
        return DriverManager::getConnection($params);
    }
}
