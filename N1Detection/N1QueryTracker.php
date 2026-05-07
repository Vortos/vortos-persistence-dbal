<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\N1Detection;

/**
 * Tracks normalized SQL signatures seen during the current request.
 *
 * Reset on every kernel.request — safe in FrankenPHP worker mode where
 * the same object persists across many requests.
 *
 * Only registered in dev (kernel.env = dev). Zero footprint in production.
 */
final class N1QueryTracker
{
    /** @var array<string, array{count: int, raw: string}> */
    private array $signatures = [];

    public function track(string $sql): void
    {
        $normalized = $this->normalize($sql);

        if (!isset($this->signatures[$normalized])) {
            $this->signatures[$normalized] = ['count' => 0, 'raw' => $normalized];
        }

        ++$this->signatures[$normalized]['count'];
    }

    /**
     * Returns signatures that were called at or above the threshold.
     *
     * @return list<array{sql: string, count: int}>
     */
    public function getViolations(int $threshold = 3): array
    {
        $violations = [];

        foreach ($this->signatures as $entry) {
            if ($entry['count'] >= $threshold) {
                $violations[] = ['sql' => $entry['raw'], 'count' => $entry['count']];
            }
        }

        usort($violations, static fn ($a, $b) => $b['count'] <=> $a['count']);

        return $violations;
    }

    public function reset(): void
    {
        $this->signatures = [];
    }

    private function normalize(string $sql): string
    {
        // Strip single-quoted string literals: 'foo' → ?
        $sql = preg_replace("/'(?:[^'\\\\]|\\\\.)*'/", '?', $sql) ?? $sql;

        // Strip numeric literals (but not inside identifiers)
        $sql = preg_replace('/(?<![.\w])\d+(?:\.\d+)?(?![.\w])/', '?', $sql) ?? $sql;

        // Collapse repeated placeholders in IN lists: IN (?, ?, ?) → IN (?)
        $sql = preg_replace('/\bIN\s*\(\s*\?(?:\s*,\s*\?)*\s*\)/i', 'IN (?)', $sql) ?? $sql;

        // Normalize whitespace
        $sql = preg_replace('/\s+/', ' ', trim($sql)) ?? $sql;

        return strtolower($sql);
    }
}
