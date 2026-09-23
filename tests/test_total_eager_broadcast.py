import random

from dslabs.simulations.sim_send_many import SimSendMany
from dslabs.nodes.node_total_order_eager_broadcast import (
    NodeTotalOrderEagerBroadcast,
)


def _run(seed: int, **kwargs):
    random.seed(seed)
    return SimSendMany(
        NodeTotalOrderEagerBroadcast,
        seed=seed,
        **kwargs,
    )


def test_converges_when_writes_are_spaced_out():
    """With no loss and time between writes, every node should see the last write."""
    sim = _run(
        num_nodes=5,
        num_messages=10,
        interval_ms=1000,
        drop_prob=0.0,
        seed=7,
    )
    values = sim.run()
    assert set(values.values()) == {9}, values


def test_nodes_agree_under_fast_writes():
    """Fast writes can arrive in different orders, but replicas should converge."""
    sim = _run(
        num_nodes=6,
        num_messages=30,
        interval_ms=2,
        drop_prob=0.0,
        seed=42,
    )
    values = sim.run()
    assert len(set(values.values())) == 1, values


def test_converges_despite_dropped_messages():
    """Eager forwarding can give a dropped message another route to each node."""
    sim = _run(
        num_nodes=12,
        num_messages=12,
        interval_ms=1000,
        drop_prob=0.5,
        seed=12345,
    )
    values = sim.run()
    assert len(set(values.values())) == 1, values