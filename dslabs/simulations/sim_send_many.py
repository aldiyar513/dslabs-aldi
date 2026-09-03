"""Workload: clients write a stream of values to randomly chosen nodes.

``num_messages`` writes of ``x = 0, 1, 2, ...`` are issued ``interval_ms``
apart, each to a random node. Once the network goes quiet, every node is asked
for its value of ``x``. A correct replication algorithm leaves every node with
the same value; a good one leaves them all with the last value written.

Run it from the command line to watch a scenario play out::

    python -m dslabs.simulations.sim_send_many --drop 0.5 --verbose
    python -m dslabs.simulations.sim_send_many --node solutions:NodeEagerBroadcast
"""

import argparse
import random
from typing import Any, Callable

from dslabs.network import SimNetwork, drop
from dslabs.nodes import load_node_class
from dslabs.protocols import Node
from dslabs.scheduler import SimScheduler


class SimSendMany:
    def __init__(
        self,
        node_class: Callable[..., Node],
        num_nodes: int = 10,
        num_messages: int = 10,
        interval_ms: int = 10,
        drop_prob: float = 0.0,
        seed: int | None = None,
        verbose: bool = False,
    ) -> None:
        self.seed = random.randrange(2**32) if seed is None else seed
        self.rng = random.Random(self.seed)
        self.num_messages = num_messages
        self.interval_ms = interval_ms

        self.scheduler = SimScheduler()
        self.network = SimNetwork(self.scheduler, seed=self.seed, latency_ms=(50, 200), verbose=verbose)
        if drop_prob > 0:
            self.network.add_rule(drop(drop_prob))

        node_ids = [f"n{i}" for i in range(num_nodes)]
        self.nodes: dict[str, Node] = {}
        for nid in node_ids:
            node = node_class(nid, node_ids, self.network.endpoint(nid), self.scheduler)
            if not isinstance(node, Node):
                raise TypeError(
                    f"{type(node).__name__} does not implement the Node interface "
                    "(needs node_id, peers, on_message, client_put, client_get)"
                )
            self.network.register(nid, node.on_message)
            self.nodes[nid] = node

        self.values: dict[str, Any] = {}
        self.idle = True

    def run(self, max_ms: int = 60_000) -> dict[str, Any]:
        """Issue the writes, run the cluster until quiet, and return each node's ``x``."""
        node_ids = list(self.nodes)
        for i in range(self.num_messages):
            node = self.nodes[self.rng.choice(node_ids)]

            def client_put(node: Node = node, value: int = i) -> None:
                node.client_put("x", value)

            self.scheduler.call_later(i * self.interval_ms, client_put)

        self.idle = self.scheduler.run_until_idle(max_ms)
        self.values = {nid: node.client_get("x") for nid, node in self.nodes.items()}
        return self.values


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--node", default="NodeMultiLeader", help="node class (default: %(default)s)")
    parser.add_argument("--nodes", type=int, default=5, help="cluster size (default: %(default)s)")
    parser.add_argument("--messages", type=int, default=10, help="number of writes (default: %(default)s)")
    parser.add_argument("--interval", type=int, default=1000, help="ms between writes (default: %(default)s)")
    parser.add_argument("--drop", type=float, default=0.0, help="probability of dropping a message (default: %(default)s)")
    parser.add_argument("--seed", type=int, default=None, help="random seed (default: random)")
    parser.add_argument("--verbose", action="store_true", help="print every network event as it happens")
    args = parser.parse_args()

    sim = SimSendMany(
        load_node_class(args.node),
        num_nodes=args.nodes,
        num_messages=args.messages,
        interval_ms=args.interval,
        drop_prob=args.drop,
        seed=args.seed,
        verbose=args.verbose,
    )
    values = sim.run()
    print(f"seed: {sim.seed}")
    print(f"network: {sim.network.stats}")
    print(f"final x per node: {values}")
    if not sim.idle:
        print(f"note: stopped at {sim.scheduler.now_ms()} ms with timers still pending")


if __name__ == "__main__":
    main()
