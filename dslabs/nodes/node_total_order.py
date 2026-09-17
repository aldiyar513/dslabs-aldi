from dataclasses import dataclass, field
from typing import Any
import uuid

from dslabs.protocols import Message, Scheduler, Transport


@dataclass
class NodeTotalOrder:
    """A fixed leader assigns sequence numbers; every node delivers in that order.

    Requests and ordered messages are retried until acknowledged. Progress
    requires a live leader and eventual message delivery to every peer.
    """

    node_id: str
    peers: list[str]
    transport: Transport
    scheduler: Scheduler
    store: dict[str, Any] = field(default_factory=dict)
    log: list[tuple[str, Any]] = field(default_factory=list)
    leader_id: str | None = None
    next_seq: int = 1
    expected_seq: int = 1
    buffer: dict[int, Message] = field(default_factory=dict)
    seen: set[str] = field(default_factory=set)
    sequenced: set[str] = field(default_factory=set)
    pending_requests: dict[str, Message] = field(default_factory=dict)
    pending_replications: dict[str, tuple[Message, set[str]]] = field(default_factory=dict)
    retry_ms: int = 250
    retry_scheduled: bool = False

    def __post_init__(self) -> None:
        if self.leader_id is None:
            self.leader_id = min(self.peers)

    def client_put(self, key: str, value: Any) -> None:
        msg = {
            "type": "client_write", "id": str(uuid.uuid4()),
            "key": key, "value": value,
        }
        if self.node_id == self.leader_id:
            self.receive_and_replicate(key, value, msg["id"])
        else:
            self.pending_requests[msg["id"]] = msg
            self.transport.send(self.leader_id, msg)
            self.schedule_retry()

    def client_get(self, key: str) -> Any:
        return self.store.get(key)

    def schedule_retry(self) -> None:
        if not self.retry_scheduled and (self.pending_requests or self.pending_replications):
            self.retry_scheduled = True
            self.scheduler.call_later(self.retry_ms, self.retry_pending)

    def retry_pending(self) -> None:
        self.retry_scheduled = False
        for msg in self.pending_requests.values():
            self.transport.send(self.leader_id, msg)
        for msg, waiting in self.pending_replications.values():
            for peer in sorted(waiting):
                self.transport.send(peer, msg)
        self.schedule_retry()

    def receive_and_replicate(self, key: str, value: Any, msg_id: str | None = None) -> None:
        msg_id = msg_id if msg_id is not None else str(uuid.uuid4())
        # A retried client request must retain its original position in the log.
        if msg_id in self.sequenced:
            return
        self.sequenced.add(msg_id)
        msg = {
            "type": "replicate", "id": msg_id, "key": key,
            "value": value, "seq": self.next_seq,
        }
        self.next_seq += 1
        waiting = set(self.peers) - {self.node_id}
        if waiting:
            self.pending_replications[msg_id] = (msg, waiting)
        self.handle_replication(msg)
        self.schedule_retry()

    def handle_replication(self, msg: Message) -> None:
        msg_id = msg["id"]
        # Acknowledge duplicates too: the previous acknowledgment may be lost.
        if self.node_id != self.leader_id:
            self.transport.send(self.leader_id, {"type": "replication_ack", "id": msg_id})
        if msg_id in self.seen:
            return
        self.seen.add(msg_id)
        for peer in self.peers:
            if peer != self.node_id:
                self.transport.send(peer, msg)
        self.process_ordered(msg)

    def receive(self, key: str, value: Any) -> None:
        self.deliver(key, value)

    def deliver(self, key: str, value: Any) -> None:
        """Apply a message only after all preceding sequence numbers."""
        self.store[key] = value
        self.log.append((key, value))

    def process_ordered(self, msg: Message) -> None:
        seq = msg["seq"]
        if seq < self.expected_seq:
            return
        self.buffer[seq] = msg
        while self.expected_seq in self.buffer:
            ordered = self.buffer.pop(self.expected_seq)
            self.receive(ordered["key"], ordered["value"])
            self.expected_seq += 1

    def on_message(self, msg: Message) -> None:
        if msg["type"] == "client_write":
            if self.node_id == self.leader_id:
                self.receive_and_replicate(msg["key"], msg["value"], msg["id"])
                self.transport.send(msg["from"], {"type": "request_ack", "id": msg["id"]})
        elif msg["type"] == "request_ack":
            self.pending_requests.pop(msg["id"], None)
        elif msg["type"] == "replicate":
            self.handle_replication(msg)
        elif msg["type"] == "replication_ack":
            pending = self.pending_replications.get(msg["id"])
            if pending is not None:
                pending[1].discard(msg["from"])
                if not pending[1]:
                    del self.pending_replications[msg["id"]]
        else:
            raise ValueError(f"Unknown message type in {msg!r}")

    def brief_state(self) -> dict[str, Any]:
        return dict(self.store)
