# Distributed Systems Labs

A small, deterministic simulator for trying out distributed algorithms.

You write a *node*: a Python class that reacts to messages and timers. The
simulator runs a cluster of your nodes in a single process on a simulated clock,
with a network that delays, drops, duplicates, and partitions messages exactly
as you tell it to. Because time is simulated, a minute of cluster time takes
milliseconds, and a run with the same seed replays identically, so the failure
you are chasing happens the same way every time.

You will use this scaffold for the exercises in the course. Their purpose is to
bring the abstract concepts from the videos and readings to life. Nothing
teaches the challenges of distributed systems like experiencing, and then
fixing, the failures that occur in them.

## Install

Requires Python 3.10 or newer.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

## Quick start

```bash
python -m pytest
python -m dslabs.simulations.sim_send_many --drop 0.5 --verbose
```

The tests run against the starter node, `NodeMultiLeader`, which is
deliberately naive. Expect one pass and two failures in the replication tests.
Making them pass is the exercise.

## How it fits together

| Where                          | What                                                                 |
|--------------------------------|----------------------------------------------------------------------|
| `dslabs/protocols.py`          | The interfaces: `Transport`, `Scheduler`, and what a `Node` must provide |
| `dslabs/scheduler.py`          | `SimScheduler`: the simulated clock and timers                        |
| `dslabs/network.py`            | `SimNetwork`: message delivery, fault rules, and the message trace    |
| `dslabs/nodes/`                | Node implementations. Yours go here                                   |
| `dslabs/simulations/`          | Workloads that build a cluster and drive it                           |
| `tests/`                       | The properties your node is measured against                          |

A node sees the outside world through exactly two objects. Its `transport` has
one method, `send(to, msg)`. Its `scheduler` has `now_ms()` and
`call_later(ms, callback)`, which returns a function that cancels the timer.
That is the whole interface, and it is why algorithm code must never sleep,
spawn threads, or read the real clock.

## Writing a node

Copy `dslabs/nodes/node_multi_leader.py` and change the behaviour. The shape
is:

```python
from dataclasses import dataclass, field
from typing import Any

from dslabs.protocols import Message, Scheduler, Transport


@dataclass
class MyNode:
    node_id: str          # this node's name, e.g. "n2"
    peers: list[str]      # every node in the cluster, including this one
    transport: Transport
    scheduler: Scheduler
    store: dict[str, Any] = field(default_factory=dict)

    def client_put(self, key: str, value: Any) -> None:
        ...  # a client wants to write

    def client_get(self, key: str) -> Any:
        return self.store.get(key)

    def on_message(self, msg: Message) -> None:
        ...  # the network delivered a message; msg["from"] says who sent it
```

Messages are plain dicts and must be JSON-serialisable. The network stamps the
sender into `msg["from"]` for you, and every receiver gets its own copy, just
as it would over a real wire.

Export your class from `dslabs/nodes/__init__.py` and run the tests against
it:

```bash
python -m pytest --node MyNode
python -m pytest --node dslabs.nodes.my_node:MyNode   # without exporting
python -m pytest --node NodeMultiLeader --node MyNode  # compare two
```

## Driving the simulator yourself

Everything the tests do, you can do in a script or a notebook:

```python
from dslabs import SimNetwork, SimScheduler, drop, partition
from dslabs.nodes import NodeMultiLeader

scheduler = SimScheduler()
network = SimNetwork(scheduler, seed=1, latency_ms=(30, 80))
network.add_rule(drop(0.2))

ids = ["n1", "n2", "n3"]
nodes = {}
for nid in ids:
    nodes[nid] = NodeMultiLeader(nid, ids, network.endpoint(nid), scheduler)
    network.register(nid, nodes[nid].on_message)

nodes["n2"].client_put("x", 1)
scheduler.run_until(2000)                  # simulated time moves only here
print({nid: n.client_get("x") for nid, n in nodes.items()})
print(network.stats)
network.print_trace()                      # every send, drop, and delivery
```

`run_until(t)` runs every timer and delivery due by time `t`, in order.
`run_until_idle(max_ms)` runs until nothing is pending, or until `max_ms` if
your algorithm keeps re-arming timers. `scheduler.pending()` lists what is
still queued.

## Breaking the network

Rules are installed with `network.add_rule(rule)` and lifted with
`network.remove_rule(rule)`. The built-in ones are:

| Rule                              | Effect                                                    |
|-----------------------------------|-----------------------------------------------------------|
| `delay(min_ms, max_ms)`           | extra random latency on every message                      |
| `drop(p)`                         | each message is lost with probability `p`                  |
| `duplicate(p)`                    | each message arrives twice with probability `p`            |
| `partition({"n1"}, {"n2", "n3"})` | no traffic between groups; unlisted nodes form one group   |

A rule is a function `rule(frm, to, deliveries, rng)` that returns a new list
of `(deliver_in_ms, msg)` pairs, so your own rules are a few lines. For
example, a one-way lossy link:

```python
def lossy_link(frm: str, to: str, deliveries, rng):
    if (frm, to) == ("n1", "n2") and rng.random() < 0.5:
        return []
    return deliveries

network.add_rule(lossy_link)
```

And a partition that heals after five seconds:

```python
cut = partition({"n1"})
network.add_rule(cut)
scheduler.call_later(5000, lambda: network.remove_rule(cut))
```

Every run is reproducible from its seed. If a test fails, rerun with the same
seed and `SimNetwork(..., verbose=True)` to watch it happen message by message.
