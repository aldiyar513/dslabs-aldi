from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
import uuid

from dslabs.protocols import Message, Scheduler, Transport

@dataclass
class NodeTotalOrderEagerBroadcast:
    '''
    Lamport total ordering with eager broadcasting.

    Assumes fixed membership and non-malicious nodes.

    Progress requires every member to respond and all required
    messages eventually to arrive. Eager broadcasting provides
    redundant paths, but does not guarantee recovery from every loss.
    '''
    node_id: str
    peers: list[str]
    transport: Transport
    scheduler: Scheduler
    
    store: dict[str, Any] = field(default_factory=dict)
    log: list[tuple[str, Any]] = field(default_factory=list) 

    # Lamport clock
    clock: int = 0

    # Sequence number for messages created by this node
    next_seq: int = 0

    # IDs of network messages all nodes have seen
    seen: set[str] = field(default_factory=set)

    # Incoming messages waiting for an earlier message from the sender
    incoming: dict[str, dict[int, dict[str, Any]]] = field(
        default_factory=dict
    )

    # Next sequence number to process for each incoming message
    expected_seq: dict[str, int] = field(default_factory=dict)

    # Writes waiting for total-order delivery
    buffer: dict[str, dict[str, Any]] = field(default_factory=dict)

    # For each write, the nodes whose acknowledgments are processed
    acknowledgements: dict[str, set[str]] = field(default_factory=dict)

    # Client-facing API
    def client_put(self, key: str, value: Any) -> None:
        # Every node can originate a write. There is no fixed leader.
        self._publish("replicate", key=key, value=value)

    def client_get(self, key: str) -> Any:
        return self.store.get(key)
    
    def _publish(self, message_type: str, **payload: Any) -> None:
        """Create a new timestamped message at this node."""
        self.clock += 1
        self.next_seq += 1

        msg = {
            "type": message_type,
            "uid": str(uuid.uuid4()),
            "sender": self.node_id,
            "seq": self.next_seq,
            "timestamp": self.clock,
            **payload,
        }

        # Process locally and eagerly broadcast to peers.
        self.receive_and_replicate(msg)

    def receive_and_replicate(self, msg: Message) -> None:
        """Accept and forward each unique network message once."""
        uid = msg["uid"]

        if uid in self.seen:
            return

        self.seen.add(uid)

        # A relay preserves the original sender, sequence and timestamp.
        if msg["sender"] != self.node_id:
            self.clock = max(self.clock, msg["timestamp"]) + 1

        for peer in self.peers:
            if peer != self.node_id:
                self.transport.send(peer, dict(msg))

        sender = msg["sender"]
        seq = msg["seq"]

        pending = self.incoming.setdefault(sender, {})
        pending[seq] = dict(msg)

        self.expected_seq.setdefault(sender, 1)

        # Process this sender's messages in their creation order.
        while self.expected_seq[sender] in pending:
            expected = self.expected_seq[sender]
            next_msg = pending.pop(expected)

            # Advance before processing because processing a write
            # creates a local acknowledgement.
            self.expected_seq[sender] += 1

            self._process_message(next_msg)

        self._try_deliver()

    def _process_message(self, msg: dict[str, Any]) -> None:
        """Process a message after its sender's earlier messages."""
        if msg["type"] == "replicate":
            uid = msg["uid"]

            self.buffer[uid] = msg
            self.acknowledgements.setdefault(uid, set())

            # ACK has its own UID, timestamp and sender sequence.
            self._publish("ack", write_uid=uid)

        elif msg["type"] == "ack":
            write_uid = msg["write_uid"]

            # An ACK may arrive before the write through another path.
            self.acknowledgements.setdefault(
                write_uid, set()
            ).add(msg["sender"])

        else:
            raise ValueError(f"Unknown message type in {msg!r}")
        
    def _try_deliver(self) -> None:
        """Deliver acknowledged writes in Lamport total order."""
        members = set(self.peers) | {self.node_id}

        while self.buffer:
            uid = min(
                self.buffer,
                key=lambda write_uid: (
                    self.buffer[write_uid]["timestamp"],
                    self.buffer[write_uid]["sender"],
                ),
            )

            # Never skip the smallest write to deliver a later one.
            if not members.issubset(
                self.acknowledgements.get(uid, set())
            ):
                return

            msg = self.buffer.pop(uid)
            self.deliver(msg["key"], msg["value"])
            self.acknowledgements.pop(uid, None)

    def deliver(self, key: str, value: Any) -> None:
        """Apply a write only once its position is safe."""
        self.store[key] = value
        self.log.append((key, value))

    # Network handler
    def on_message(self, msg: Message) -> None:
        self.receive_and_replicate(msg)

    def brief_state(self) -> dict[str, Any]:
        return dict(self.store)

    