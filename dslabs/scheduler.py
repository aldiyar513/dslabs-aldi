"""Deterministic discrete-event scheduler.

Simulated time only moves when you ask the scheduler to run. ``run_until(t)``
executes every callback due at or before ``t`` in due-time order, jumping the
clock to each callback's due time as it goes. Nothing sleeps and nothing runs
in the background, so a run given the same inputs always produces the same
result, and a simulated hour takes milliseconds of real time.

The scheduler also owns the run's ``Trace``. Timers that belong to a node (the
callback is one of the node's bound methods, or a closure over the node) are
recorded on that node's timeline as set, fired, or cancelled.
"""

import heapq
import inspect
from dataclasses import dataclass, field
from typing import Any, Callable

from .protocols import Cancel, Node
from .trace import Trace


@dataclass(order=True)
class _Event:
    when: int
    seq: int  # creation order; breaks ties so equal due times run first-in first-out
    cb: Callable[[], None] = field(compare=False)
    live: bool = field(default=True, compare=False)
    owner: Any = field(default=None, compare=False)  # the node this timer belongs to, if known
    timer_id: int | None = field(default=None, compare=False)


def _owner_of(cb: Callable[[], None]) -> Any:
    """The node a callback belongs to: its bound ``self``, or a node it closes over."""
    bound = getattr(cb, "__self__", None)
    if isinstance(bound, Node):
        return bound
    try:
        free = inspect.getclosurevars(cb).nonlocals
    except (TypeError, ValueError):
        return None
    return next((v for v in free.values() if isinstance(v, Node)), None)


def _name_of(cb: Callable[[], None]) -> str:
    return getattr(cb, "__name__", None) or type(cb).__name__


class SimScheduler:
    """Implements the ``Scheduler`` protocol over a simulated clock."""

    def __init__(self, verbose: bool = False) -> None:
        self._now = 0
        self._heap: list[_Event] = []
        self._seq = 0
        self.trace = Trace(now=self.now_ms, verbose=verbose)

    def now_ms(self) -> int:
        return self._now

    def call_later(self, ms: int, cb: Callable[[], None]) -> Cancel:
        if ms < 0:
            raise ValueError(f"call_later delay must be >= 0, got {ms}")
        self._seq += 1
        owner = _owner_of(cb)
        name = _name_of(cb)
        timer_id = self.trace.timer_set(owner.node_id, name, self._now + ms) if owner is not None else None
        event = _Event(self._now + ms, self._seq, cb, owner=owner, timer_id=timer_id)
        heapq.heappush(self._heap, event)

        def cancel() -> None:
            if event.live:
                event.live = False
                if timer_id is not None:
                    self.trace.timer_cancelled(owner.node_id, name, timer_id)

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
        return sorted((ev.when, getattr(ev.cb, "__qualname__", repr(ev.cb))) for ev in self._heap if ev.live)

    def _run_next(self) -> None:
        event = heapq.heappop(self._heap)
        if event.live:
            event.live = False
            self._now = event.when
            if event.timer_id is not None:
                self.trace.timer_fired(event.owner.node_id, _name_of(event.cb), event.timer_id)
            event.cb()
            if event.owner is not None:
                self.trace.snapshot(event.owner)
