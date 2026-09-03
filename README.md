# Distributed Systems Labs

A small, deterministic simulator for trying out distributed algorithms.

You write a *node*: a Python class that reacts to messages and timers. The
simulator runs a cluster of your nodes in a single process on a simulated clock,
with a network that delays, drops, duplicates, and partitions messages exactly
as you tell it to. Because time is simulated, a minute of cluster time takes
milliseconds, and a run with the same seed replays identically, so the failure
you are chasing happens the same way every time. And because every message,
timer, and state change is recorded, you can see exactly what happened and why.

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

For a guided tour in a notebook, open `examples/demo.ipynb`.

## How it fits together

| Where                          | What                                                                     |
|--------------------------------|--------------------------------------------------------------------------|
| `dslabs/protocols.py`          | The interfaces: `Transport`, `Scheduler`, and what a `Node` must provide |
| `dslabs/scheduler.py`          | `SimScheduler`: the simulated clock and timers                           |
| `dslabs/network.py`            | `SimNetwork`: message delivery and fault rules                           |
| `dslabs/trace.py`              | The record of everything that happened, and views over it               |
| `dslabs/diagram.py`            | Space-time diagrams of a trace                                           |
| `dslabs/cluster.py`            | `Cluster`: the pieces above wired together, ready to drive              |
| `dslabs/nodes/`                | Node implementations. Yours go here                                      |
| `dslabs/simulations/`          | Workloads that build a cluster and drive it                              |
| `tests/`                       | The properties your node is measured against                             |

A node sees the outside world through exactly two objects. Its `transport` has
`send(to, msg)`. Its `scheduler` has `now_ms()` and `call_later(ms, callback)`,
which returns a function that cancels the timer. That is the whole interface,
and it is why algorithm code must never sleep, spawn threads, or read the real
clock.

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

    def brief_state(self) -> dict[str, Any]:
        return dict(self.store)   # optional: what diagrams show as your state
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

`Cluster` builds a cluster of one node class and lets you play the client:

```python
from dslabs import Cluster, drop
from dslabs.nodes import NodeMultiLeader

cluster = Cluster(NodeMultiLeader, 3, seed=1)   # nodes n1, n2, n3
cluster.add_rule(drop(0.2))
cluster.put("n2", "x", 1)
cluster.run_until(2000)        # simulated time moves only here
cluster.values("x")            # {'n1': 1, 'n2': 1, 'n3': None}
```

`run_until(t)` runs every timer and delivery due by time `t`, in order.
`run_until_idle(max_ms)` runs until nothing is pending, or until `max_ms` if
your algorithm keeps re-arming timers. `cluster.scheduler.pending()` lists
what is still queued.

## Seeing what happened

Everything a run does is recorded, and you can look at it four ways. In a
notebook, each of these renders as a table or a picture when it is the last
expression in a cell. In a terminal, `print(...)` it.

```python
cluster.messages()       # one row per message: who, to whom, delivered after how long, or dropped by which rule
cluster.timeline()       # every event in time order: sends, deliveries, drops, client requests, timers, state, notes
cluster.diagram()        # a space-time diagram: lifelines per node, arrows per message, drawn to scale
cluster.explain("n3")    # what one node sent and received, and its own timeline
```

Or take the run one event at a time, which turns it into a predict-then-check
exercise:

```python
cluster.pending()        # what is in flight or armed, soonest first
cluster.peek()           # the next event, without running it
cluster.step()           # run exactly one event and show everything it caused
while cluster.step(): pass   # and on to the end
```

The diagram is the picture from the readings, drawn from a real run: a
dropped message ends in a red cross, a duplicate is a second orange arrow, and
a slow message overtaken by a fast one is visibly crossed. Client requests are
green dots, timers are dotted lines from when they were set to when they fired,
and the node's state appears under its lifeline whenever it changes. Hover
over anything for the full detail. `cluster.diagram(nodes=["n1", "n2"],
t_range=(0, 1000))` zooms in; `.save("run.svg")` writes it out.

Two optional hooks make your own node's story visible:

- `brief_state()`: return a dict, and the simulator snapshots it after every
  delivery, timer, and client request, showing changes on the timeline.
- `self.transport.note("holding seq 3, waiting for 1")`: write onto your
  node's timeline. Debugging only; it must never affect the algorithm.

Timers show up on a node's timeline when their callback is one of the node's
methods or a closure inside one, which is the normal case. Pass
`verbose=True` to `Cluster` to see events printed as they happen.

## Breaking the network

Rules are installed with `cluster.add_rule(rule)` and lifted with
`cluster.remove_rule(rule)`. The built-in ones are:

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

cluster.add_rule(lossy_link)
```

And a partition that heals after five seconds:

```python
cut = partition({"n1"})
cluster.add_rule(cut)
cluster.scheduler.call_later(5000, lambda: cluster.remove_rule(cut))
```

Every run is reproducible from its seed. If a test fails, rerun with the same
seed and look at the diagram.

## Under the hood

`Cluster` is only the three simulator objects wired together. If you want to
assemble them yourself:

```python
from dslabs import SimNetwork, SimScheduler
from dslabs.nodes import NodeMultiLeader

scheduler = SimScheduler()
network = SimNetwork(scheduler, seed=1, latency_ms=(30, 80))
ids = ["n1", "n2", "n3"]
nodes = {}
for nid in ids:
    nodes[nid] = NodeMultiLeader(nid, ids, network.endpoint(nid), scheduler)
    network.register(nid, nodes[nid].on_message)

nodes["n2"].client_put("x", 1)
scheduler.run_until(2000)
scheduler.trace.messages()     # the trace lives on the scheduler
```
