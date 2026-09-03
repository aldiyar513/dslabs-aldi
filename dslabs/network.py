"""Deterministic network simulator with fault injection and a message trace.

How a message travels:

1. A node calls ``transport.send(to, msg)`` on its ``Endpoint``. The endpoint
   stamps ``msg["from"]`` and hands the message to the network.
2. The network turns it into one pending delivery, ``(latency_ms, msg)``, with
   a random latency drawn from the network's base range.
3. Every installed rule is applied in order. A rule maps the list of pending
   deliveries to a new list: it can add delay, drop deliveries, or add copies.
4. Each surviving delivery is scheduled on the scheduler. When it comes due,
   the message is JSON round-tripped, so the receiver gets its own copy exactly
   as it would over a real wire, and passed to the receiver's handler.

Every send, drop, and delivery is appended to ``network.trace`` with its
simulated timestamp and tallied in ``network.stats``.

All randomness comes from a ``random.Random(seed)`` owned by the network, so
the same seed always replays the same run.
"""

import json
import random
from dataclasses import dataclass
from typing import Callable, Iterable

from .protocols import Message, Scheduler

# One pending delivery: (milliseconds from now until delivery, message).
Delivery = tuple[int, Message]

# A rule rewrites the pending deliveries of one send. It is called as
# ``rule(frm, to, deliveries, rng)`` and returns the new list of deliveries.
Rule = Callable[[str, str, list[Delivery], random.Random], list[Delivery]]

Handler = Callable[[Message], None]


@dataclass
class TraceEvent:
    t_ms: int
    kind: str  # "send", "drop", or "deliver"
    frm: str
    to: str
    msg: Message

    def __str__(self) -> str:
        body = {k: v for k, v in self.msg.items() if k != "from"}
        return f"@{self.t_ms:>7} ms  {self.frm:>4} -> {self.to:<4}  {self.kind:<7}  {body}"


class Endpoint:
    """One node's connection to the network. Implements ``Transport``."""

    def __init__(self, network: "SimNetwork", node_id: str) -> None:
        self.node_id = node_id
        self._network = network

    def send(self, to: str, msg: Message) -> None:
        self._network._send(self.node_id, to, msg)


class SimNetwork:
    """Event-driven simulated network.

    - ``endpoint(node_id)``: the ``Transport`` a node uses to send.
    - ``register(node_id, handler)``: where to deliver that node's messages.
    - ``add_rule(rule)`` / ``remove_rule(rule)``: install or lift a fault rule.
    - ``trace`` / ``stats`` / ``print_trace()``: what happened.
    """

    def __init__(
        self,
        scheduler: Scheduler,
        seed: int = 0,
        latency_ms: tuple[int, int] = (30, 80),
        verbose: bool = False,
    ) -> None:
        self.scheduler = scheduler
        self.rng = random.Random(seed)
        self.latency_ms = latency_ms
        self.verbose = verbose
        self.rules: list[Rule] = []
        self.trace: list[TraceEvent] = []
        self.stats = {"sent": 0, "delivered": 0, "dropped": 0, "duplicated": 0}
        self._endpoints: dict[str, Endpoint] = {}
        self._handlers: dict[str, Handler] = {}

    def endpoint(self, node_id: str) -> Endpoint:
        """Return the transport for ``node_id``, creating it on first use."""
        if node_id not in self._endpoints:
            self._endpoints[node_id] = Endpoint(self, node_id)
        return self._endpoints[node_id]

    def register(self, node_id: str, handler: Handler) -> None:
        """Deliver messages addressed to ``node_id`` by calling ``handler(msg)``."""
        self.endpoint(node_id)
        self._handlers[node_id] = handler

    def add_rule(self, rule: Rule) -> None:
        self.rules.append(rule)

    def remove_rule(self, rule: Rule) -> None:
        self.rules.remove(rule)

    def print_trace(self) -> None:
        for event in self.trace:
            print(event)

    def _send(self, frm: str, to: str, msg: Message) -> None:
        if to not in self._endpoints:
            raise ValueError(
                f"{frm} sent a message to unknown node {to!r}; "
                f"known nodes are {sorted(self._endpoints)}"
            )
        msg = {**msg, "from": frm}
        try:
            json.dumps(msg)
        except (TypeError, ValueError) as e:
            raise TypeError(f"message from {frm} to {to} is not JSON-serialisable: {msg!r}") from e

        self.stats["sent"] += 1
        self._record(TraceEvent(self.scheduler.now_ms(), "send", frm, to, msg))

        deliveries: list[Delivery] = [(self.rng.randint(*self.latency_ms), msg)]
        for rule in list(self.rules):
            before = len(deliveries)
            deliveries = rule(frm, to, deliveries, self.rng)
            after = len(deliveries)
            if after < before:
                self.stats["dropped"] += before - after
                for _ in range(before - after):
                    self._record(TraceEvent(self.scheduler.now_ms(), "drop", frm, to, msg))
            elif after > before:
                self.stats["duplicated"] += after - before

        for at, payload in deliveries:
            self._schedule(at, frm, to, payload)

    def _schedule(self, at: int, frm: str, to: str, payload: Message) -> None:
        def deliver() -> None:
            handler = self._handlers.get(to)
            if handler is None:
                raise RuntimeError(
                    f"a message for {to} came due but no handler is registered; "
                    f"call network.register({to!r}, node.on_message)"
                )
            copy = json.loads(json.dumps(payload))
            self.stats["delivered"] += 1
            self._record(TraceEvent(self.scheduler.now_ms(), "deliver", frm, to, copy))
            handler(copy)

        deliver.__qualname__ = f"deliver({frm}->{to})"
        self.scheduler.call_later(at, deliver)

    def _record(self, event: TraceEvent) -> None:
        self.trace.append(event)
        if self.verbose:
            print(event)


# --- Fault rules -----------------------------------------------------------
#
# Each function below returns a rule. Rules are ordinary functions with the
# signature ``rule(frm, to, deliveries, rng) -> deliveries``, so you can write
# your own in a few lines; see the README for an example.


def delay(min_ms: int, max_ms: int) -> Rule:
    """Add a random extra delay in ``[min_ms, max_ms]`` to every delivery."""

    def rule(frm: str, to: str, deliveries: list[Delivery], rng: random.Random) -> list[Delivery]:
        return [(at + rng.randint(min_ms, max_ms), msg) for at, msg in deliveries]

    return rule


def drop(p: float) -> Rule:
    """Drop each delivery independently with probability ``p``."""

    def rule(frm: str, to: str, deliveries: list[Delivery], rng: random.Random) -> list[Delivery]:
        return [d for d in deliveries if rng.random() >= p]

    return rule


def duplicate(p: float) -> Rule:
    """With probability ``p``, deliver an extra copy of a message shortly after the first."""

    def rule(frm: str, to: str, deliveries: list[Delivery], rng: random.Random) -> list[Delivery]:
        extra = [(at + rng.randint(1, 100), msg) for at, msg in deliveries if rng.random() < p]
        return deliveries + extra

    return rule


def partition(*groups: Iterable[str]) -> Rule:
    """Drop every message between nodes in different groups.

    Nodes not named in any group together form one remaining group, so
    ``partition({"n1"})`` isolates n1 and ``partition({"n1", "n2"})`` splits
    those two off from the rest. Lift the partition with ``remove_rule``.
    """
    group_of = {node: i for i, group in enumerate(groups) for node in group}

    def rule(frm: str, to: str, deliveries: list[Delivery], rng: random.Random) -> list[Delivery]:
        same_side = group_of.get(frm, -1) == group_of.get(to, -1)
        return deliveries if same_side else []

    return rule
