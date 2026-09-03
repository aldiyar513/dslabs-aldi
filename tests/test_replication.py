"""Replication properties every node implementation is measured against.

Each test builds a cluster of the chosen node class, has clients write
``x = 0, 1, 2, ...`` to random nodes, waits for the network to go quiet, and
then checks what each node believes ``x`` is. Run against your implementation
with ``pytest --node YourClass``.
"""

import pytest

from dslabs.simulations.sim_send_many import SimSendMany


def test_converges_when_writes_are_spaced_out(node_class):
    """Baseline: no message loss, and writes 1 s apart, far longer than any
    network delay. Every node must end up with the last value written."""
    sim = SimSendMany(node_class, num_nodes=5, num_messages=10, interval_ms=1000, drop_prob=0.0, seed=7)
    values = sim.run()
    assert set(values.values()) == {9}, values


def test_converges_despite_dropped_messages(node_class):
    """The network drops half of all messages. Every node must still end up
    with the last value written, which takes some form of reliable delivery."""
    sim = SimSendMany(node_class, num_nodes=12, num_messages=12, interval_ms=1000, drop_prob=0.5, seed=12345)
    values = sim.run()
    assert set(values.values()) == {11}, values


def test_nodes_agree_under_fast_writes(node_class):
    """Writes arrive far faster than the network delivers them, so replication
    messages overtake one another. Nodes must still agree on a single final
    value. Which value wins is up to the algorithm."""
    sim = SimSendMany(node_class, num_nodes=6, num_messages=30, interval_ms=2, drop_prob=0.0, seed=42)
    values = sim.run()
    assert len(set(values.values())) == 1, values


@pytest.mark.xfail(
    reason="agreeing on an order is not the same as agreeing on real-time order; "
    "without synchronised clocks no node can tell which of two concurrent writes came last"
)
def test_last_write_wins_under_fast_writes(node_class):
    """Like the previous test, but demanding that the agreed value is the one
    written last in real time, on every one of ten runs. Expected to fail for
    every algorithm in this course. It is here as a discussion point: some runs
    will happen to get it right, so what would it take to guarantee it?"""
    for seed in range(10):
        sim = SimSendMany(node_class, num_nodes=6, num_messages=30, interval_ms=2, drop_prob=0.0, seed=seed)
        values = sim.run()
        assert set(values.values()) == {29}, f"seed {seed}: {values}"
