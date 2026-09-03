import pytest

from dslabs import SimNetwork, SimScheduler, delay, drop, duplicate, partition


def cluster(*node_ids: str, seed: int = 0, latency_ms=(30, 80)):
    """A network whose nodes just record what they receive."""
    scheduler = SimScheduler()
    network = SimNetwork(scheduler, seed=seed, latency_ms=latency_ms)
    inbox = {nid: [] for nid in node_ids}
    for nid in node_ids:
        network.register(nid, inbox[nid].append)
    return scheduler, network, inbox


def test_sender_is_stamped_and_receiver_gets_its_own_copy():
    scheduler, network, inbox = cluster("n1", "n2")
    original = {"type": "hello", "items": [1, 2]}
    network.endpoint("n1").send("n2", original)
    scheduler.run_until(1000)

    (received,) = inbox["n2"]
    assert received == {"type": "hello", "items": [1, 2], "from": "n1"}
    assert "from" not in original, "send must not mutate the caller's dict"
    received["items"].append(3)
    assert original["items"] == [1, 2], "receiver must not share state with sender"


def test_latency_is_within_the_configured_range():
    scheduler, network, inbox = cluster("n1", "n2", latency_ms=(30, 80))
    for _ in range(50):
        network.endpoint("n1").send("n2", {"type": "ping"})
    scheduler.run_until(1000)
    deliveries = [ev for ev in network.trace if ev.kind == "deliver"]
    assert len(deliveries) == 50
    assert all(30 <= ev.t_ms <= 80 for ev in deliveries)


def test_same_seed_replays_the_same_run():
    def run(seed: int):
        scheduler, network, inbox = cluster("n1", "n2", "n3", seed=seed)
        network.add_rule(delay(0, 500))
        network.add_rule(drop(0.3))
        network.add_rule(duplicate(0.3))
        for i in range(30):
            network.endpoint("n1").send("n2" if i % 2 else "n3", {"i": i})
        scheduler.run_until(5000)
        return [(ev.t_ms, ev.kind, ev.to, ev.msg["i"]) for ev in network.trace]

    assert run(seed=1) == run(seed=1)
    assert run(seed=1) != run(seed=2)


def test_drop_and_duplicate_are_counted_and_traced():
    scheduler, network, inbox = cluster("n1", "n2")
    network.add_rule(drop(1.0))
    network.endpoint("n1").send("n2", {"type": "lost"})
    scheduler.run_until(1000)
    assert inbox["n2"] == []
    assert network.stats == {"sent": 1, "delivered": 0, "dropped": 1, "duplicated": 0}
    assert [ev.kind for ev in network.trace] == ["send", "drop"]

    network.rules.clear()
    network.add_rule(duplicate(1.0))
    network.endpoint("n1").send("n2", {"type": "twice"})
    scheduler.run_until(2000)
    assert [m["type"] for m in inbox["n2"]] == ["twice", "twice"]
    assert network.stats == {"sent": 2, "delivered": 2, "dropped": 1, "duplicated": 1}


def test_partition_blocks_cross_group_traffic_until_lifted():
    scheduler, network, inbox = cluster("n1", "n2", "n3")
    cut = partition({"n1"})  # n1 alone; n2 and n3 form the other side
    network.add_rule(cut)
    network.endpoint("n1").send("n2", {"type": "blocked"})
    network.endpoint("n2").send("n1", {"type": "blocked"})
    network.endpoint("n2").send("n3", {"type": "ok"})
    scheduler.run_until(1000)
    assert inbox["n1"] == [] and inbox["n2"] == []
    assert [m["type"] for m in inbox["n3"]] == ["ok"]

    network.remove_rule(cut)
    network.endpoint("n1").send("n2", {"type": "healed"})
    scheduler.run_until(2000)
    assert [m["type"] for m in inbox["n2"]] == ["healed"]


def test_sending_to_an_unknown_node_fails_loudly():
    scheduler, network, inbox = cluster("n1", "n2")
    with pytest.raises(ValueError, match="unknown node 'n9'"):
        network.endpoint("n1").send("n9", {"type": "typo"})


def test_messages_must_be_json_serialisable():
    scheduler, network, inbox = cluster("n1", "n2")
    with pytest.raises(TypeError, match="JSON"):
        network.endpoint("n1").send("n2", {"ids": {"a", "b"}})
