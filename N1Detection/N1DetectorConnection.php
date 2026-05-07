<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\N1Detection;

use Doctrine\DBAL\Driver\Connection as DriverConnection;
use Doctrine\DBAL\Driver\Middleware\AbstractConnectionMiddleware;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement;

/** @internal Used only by N1DetectorDriver */
final class N1DetectorConnection extends AbstractConnectionMiddleware
{
    public function __construct(
        DriverConnection $wrappedConnection,
        private readonly N1QueryTracker $tracker,
    ) {
        parent::__construct($wrappedConnection);
    }

    public function query(string $sql): Result
    {
        $this->tracker->track($sql);
        return parent::query($sql);
    }

    public function exec(string $sql): int
    {
        $this->tracker->track($sql);
        return parent::exec($sql);
    }

    public function prepare(string $sql): Statement
    {
        // Track at execute() time (not prepare time) — the same prepared
        // statement may be executed multiple times; each execute is a DB round-trip.
        return new N1DetectorStatement(parent::prepare($sql), $this->tracker, $sql);
    }
}
