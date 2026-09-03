"""Deterministic discrete-event scheduler.

Simulated time only moves when you ask the scheduler to run. ``run_until(t)``
executes every callback due at or before ``t`` in due-time order, jumping the
clock to each callback's due time as it goes. Nothing sleeps and nothing runs
in the background, so a run given the same inputs always produces the same
result, and a simulated hour takes milliseconds of real time.
"""

import heapq
from dataclasses import dataclass, field
from typing import Callable

from .protocols import Cancel


@dataclass(order=True)
class _Event:
    when: int
    seq: int  # creation order; breaks ties so equal due times run first-in first-out
    cb: Callable[[], None] = field(compare=False)
    live: bool = field(default=True, compare=False)


class SimScheduler:
    """Implements the ``Scheduler`` protocol over a simulated clock."""

    def __init__(self) -> None:
        self._now = 0
        self._heap: list[_Event] = []
        self._seq = 0

    def now_ms(self) -> int:
        return self._now

    def call_later(self, ms: int, cb: Callable[[], None]) -> Cancel:
        if ms < 0:
            raise ValueError(f"call_later delay must be >= 0, got {ms}")
        self._seq += 1
        event = _Event(self._now + ms, self._seq, cb)
        heapq.heappush(self._heap, event)

        def cancel() -> None:
            event.live = False

        return cancel

    def run_until(self, t_ms: int) -> None:
        """Run every callback due at or before ``t_ms``, then set the clock to ``t_ms``."""
        while self._heap and self._heap[0].when <= t_ms:
            self._run_next()
        self._now = max(self._now, t_ms)

    def run_until_idle(self, max_ms: int) -> bool:
        """Run until no callbacks remain, or until the next one is due after ``max_ms``.

        Returns True if the scheduler went idle. Returns False if it stopped at
        ``max_ms`` with work still pending, which is what happens for algorithms
        that keep re-arming timers (heartbeats, retries, and the like).
        """
        while self._heap and self._heap[0].when <= max_ms:
            self._run_next()
        if self.pending():
            self._now = max(self._now, max_ms)
            return False
        return True

    def pending(self) -> list[tuple[int, str]]:
        """Live callbacks as ``(due_ms, callback_name)`` pairs, soonest first."""
        return sorted(
            (ev.when, getattr(ev.cb, "__qualname__", repr(ev.cb)))
            for ev in self._heap
            if ev.live
        )

    def _run_next(self) -> None:
        event = heapq.heappop(self._heap)
        if event.live:
            event.live = False
            self._now = event.when
            event.cb()
