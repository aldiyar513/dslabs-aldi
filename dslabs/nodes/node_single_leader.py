from dataclasses import dataclass, field
from typing import Any

from dslabs.protocols import Message, Scheduler, Transport


@dataclass
class NodeSingleLeader:
    """Naive single-leader replication.

    Only the leader accepts writes. The leader applies each write locally
    and replicates it to all followers. Followers apply replication
    messages in the order they arrive.
    """


    node_id: str
    peers: list[str]
    transport: Transport
    scheduler: Scheduler
    # Key-value store
    store: dict[str, Any] = field(default_factory=dict)
    # Append-only log of everything delivered, in delivery order
    log: list[tuple[str, Any]] = field(default_factory=list)
    leader_id: str = 'n1'


    # Client-facing API
    def client_put(self, key: str, value: Any) -> None:
        if self.node_id == self.leader_id: 
            self.receive_and_replicate(key, value)

    def client_get(self, key: str) -> Any:
        return self.store.get(key)

    # Node internals
    def receive_and_replicate(self, key: str, value: Any) -> None:
        self.receive(key, value)
        for peer in self.peers:
            if peer != self.node_id:
                self.transport.send(peer, {"type": "replicate", "key": key, "value": value})

    def receive(self, key: str, value: Any) -> None:
        # Received messages are delivered immediately
        self.deliver(key, value)

    def deliver(self, key: str, value: Any) -> None:
        """Apply a message to this node: update the store, append to the log."""
        self.store[key] = value
        self.log.append((key, value))

    # Network handler
    def on_message(self, msg: Message) -> None:
        if msg["type"] == "replicate":
            self.receive(msg["key"], msg["value"])
        else:
            raise ValueError(f"Unknown message type in {msg!r}")

    # What the trace and diagrams show as this node's state
    def brief_state(self) -> dict[str, Any]:
        return dict(self.store)
