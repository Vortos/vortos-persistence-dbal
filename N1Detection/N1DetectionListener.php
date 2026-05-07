<?php

declare(strict_types=1);

namespace Vortos\PersistenceDbal\N1Detection;

use Psr\Log\LoggerInterface;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\HttpKernel\Event\RequestEvent;
use Symfony\Component\HttpKernel\Event\ResponseEvent;
use Symfony\Component\HttpKernel\KernelEvents;

/**
 * Resets the N+1 tracker at the start of each request, adds a response
 * header if violations are detected, and logs a warning after each request.
 *
 * Only registered in dev (kernel.env = dev) via N1DetectionCompilerPass.
 *
 * ## Header format
 *
 *   X-Vortos-N1: orders.find called 23x; users.find called 11x
 *
 * Visible immediately in browser DevTools (Network tab → response headers)
 * or curl -i. No need to tail logs for quick feedback.
 *
 * ## Log format
 *
 *   [query.WARNING] N+1 query detected {"sql":"select …","count":23,"threshold":3}
 */
final class N1DetectionListener implements EventSubscriberInterface
{
    public function __construct(
        private readonly N1QueryTracker $tracker,
        private readonly LoggerInterface $logger,
        private readonly int $threshold = 3,
    ) {}

    public static function getSubscribedEvents(): array
    {
        return [
            KernelEvents::REQUEST  => ['onRequest', 100],
            KernelEvents::RESPONSE => ['onResponse', -100],
        ];
    }

    public function onRequest(RequestEvent $event): void
    {
        if (!$event->isMainRequest()) {
            return;
        }

        $this->tracker->reset();
    }

    public function onResponse(ResponseEvent $event): void
    {
        if (!$event->isMainRequest()) {
            return;
        }

        $violations = $this->tracker->getViolations($this->threshold);

        if ($violations === []) {
            return;
        }

        // Log each violation to the query channel
        foreach ($violations as $v) {
            $this->logger->warning('N+1 query detected', [
                'sql'       => $v['sql'],
                'count'     => $v['count'],
                'threshold' => $this->threshold,
            ]);
        }

        // Add a response header for instant visibility in DevTools / curl
        $summary = implode('; ', array_map(
            static fn (array $v): string => sprintf('"%s" called %dx', substr($v['sql'], 0, 60), $v['count']),
            array_slice($violations, 0, 3),
        ));

        $event->getResponse()->headers->set('X-Vortos-N1', $summary);
    }
}
