<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\N1Detection;

use Doctrine\DBAL\Driver\Middleware\AbstractStatementMiddleware;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement;

/** @internal Used only by N1DetectorConnection */
final class N1DetectorStatement extends AbstractStatementMiddleware
{
    public function __construct(
        Statement $wrappedStatement,
        private readonly N1QueryTracker $tracker,
        private readonly string $sql,
    ) {
        parent::__construct($wrappedStatement);
    }

    public function execute(): Result
    {
        // Track here (not at prepare) so each round-trip to the DB is counted.
        $this->tracker->track($this->sql);
        return parent::execute();
    }
}
