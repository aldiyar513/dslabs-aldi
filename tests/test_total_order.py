"""Recovery and full-log agreement for the total-order implementation."""

from dslabs.cluster import Cluster
from dslabs.network import drop, duplicate
from dslabs.nodes import NodeTotalOrder
from dslabs.simulations.sim_send_many import SimSendMany


def test_recovers_lost_requests_replications_and_acknowledgments():
    cluster = Cluster(NodeTotalOrder, ["a", "b", "c"], latency_ms=(10, 10))
    dropped = set()

    def lose_first(frm, to, deliveries, rng):
        msg = deliveries[0][1]
        kind = msg["type"]
        # Hide the first ordered write from c until the leader retries it.
        if kind == "replicate" and msg["seq"] == 1 and to == "c":
            if cluster.scheduler.now_ms() < 200:
                dropped.add("replicate")
                return []
        if kind in {"client_write", "request_ack", "replication_ack"} and kind not in dropped:
            dropped.add(kind)
            return []
        return deliveries

    cluster.add_rule(lose_first)
    cluster.put("a", "x", 1)
    cluster.put("a", "x", 2)
    cluster.put("b", "y", 3)
    cluster.run_until(100)
    assert cluster["c"].log == []
    assert 2 in cluster["c"].buffer
    assert cluster.run_until_idle()
    assert dropped == {"client_write", "request_ack", "replication_ack", "replicate"}
    for node in cluster.nodes.values():
        assert node.log == [("x", 1), ("x", 2), ("y", 3)]
        assert node.store == {"x": 2, "y": 3}
        assert not node.buffer
        assert not node.pending_requests
        assert not node.pending_replications


def test_full_logs_agree_with_loss_duplicates_and_fast_writes():
    sim = SimSendMany(NodeTotalOrder, num_nodes=6, num_messages=30,
                      interval_ms=2, seed=42)
    sim.cluster.add_rule(drop(0.5))
    sim.cluster.add_rule(duplicate(1.0))
    sim.run()
    assert sim.idle
    reference = sim.nodes["n1"]
    assert len(reference.log) == 30
    assert sorted(value for key, value in reference.log) == list(range(30))
    for node in sim.nodes.values():
        assert node.log == reference.log
        assert node.store == reference.store
