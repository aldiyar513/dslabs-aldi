from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
import uuid

from dslabs.protocols import Message, Scheduler, Transport

@dataclass
class NodeCAS:
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
    
    # Operation UID -> result, populated only after ordered delivery.
    operation_results: dict[str, Any] = field(default_factory=dict)
    pending_transmissions: dict[
        tuple[str, str], dict[str, Any]
    ] = field(default_factory=dict)

    retry_ms: int = 200
    retry_scheduled: bool = False
    
    # Client-facing API
    def client_put(self, key: str, value: Any) -> str:
        return self._publish(
            "replicate",
            operation="put",
            key=key,
            value=value,
        )


    def client_get(self, key: str) -> str:
        return self._publish(
            "replicate",
            operation="get",
            key=key,
        )

    def client_cas(self, key: str, old: Any, new: Any) -> str:
        return self._publish(
            "replicate",
            operation="cas",
            key=key,
            old=old,
            new=new,
        )
    
    def _publish(self, message_type: str, **payload: Any) -> str:
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
        return msg["uid"]

    def receive_and_replicate(self, msg: Message) -> None:
        uid = msg["uid"]

        # "from" identifies the immediate network sender.
        # "sender" identifies the original message creator.
        immediate_sender = msg.get("from")

        # Send a receipt even for duplicates: the earlier receipt
        # might have been dropped.
        if (
            immediate_sender is not None
            and immediate_sender != self.node_id
        ):
            self.transport.send(
                immediate_sender,
                {
                    "type": "transport_receipt",
                    "received_uid": uid,
                },
            )

        # A duplicate receives a receipt but is not processed again.
        if uid in self.seen:
            return

        self.seen.add(uid)

        if msg["sender"] != self.node_id:
            self.clock = max(
                self.clock,
                msg["timestamp"],
            ) + 1

        # Reliably forward each unique message to the other nodes.
        for peer in self.peers:
            if peer != self.node_id:
                self._send_reliably(peer, msg)

        sender = msg["sender"]
        seq = msg["seq"]

        pending = self.incoming.setdefault(sender, {})
        pending[seq] = dict(msg)

        self.expected_seq.setdefault(sender, 1)

        while self.expected_seq[sender] in pending:
            expected = self.expected_seq[sender]
            next_msg = pending.pop(expected)

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
            self.deliver(msg)
            self.acknowledgements.pop(uid, None)

    def deliver(self, msg: dict[str, Any]) -> None:
        """Execute an operation only after total-order delivery."""
        uid = msg["uid"]
        key = msg["key"]
        operation = msg["operation"]

        if operation == "put":
            self.store[key] = msg["value"]
            self.log.append((key, msg["value"]))
            result = True

        elif operation == "get":
            result = self.store.get(key)

        elif operation == "cas":
            result = False

            if self.store.get(key) == msg["old"]:
                self.store[key] = msg["new"]
                self.log.append((key, msg["new"]))
                result = True

        else:
            raise ValueError(f"Unknown operation: {operation!r}")

        self.operation_results[uid] = result

    # Network handler
    def on_message(self, msg: Message) -> None:
        if msg["type"] == "transport_receipt":
            transmission = (
                msg["received_uid"],
                msg["from"],
            )

            self.pending_transmissions.pop(
                transmission,
                None,
            )
            return

        self.receive_and_replicate(msg)

    def brief_state(self) -> dict[str, Any]:
        return dict(self.store)

    def _send_reliably(
        self,
        peer: str,
        msg: dict[str, Any],
    ) -> None:
        transmission = (msg["uid"], peer)
        self.pending_transmissions[transmission] = dict(msg)

        self.transport.send(peer, dict(msg))

        if not self.retry_scheduled:
            self.retry_scheduled = True
            self.scheduler.call_later(
                self.retry_ms,
                self._retry_pending,
                )

    def _retry_pending(self) -> None:
        self.retry_scheduled = False

        # Resend the original messages with unchanged IDs,
        # timestamps, and sequence numbers.
        for (_, peer), msg in list(
            self.pending_transmissions.items()
        ):
            self.transport.send(peer, dict(msg))

        if self.pending_transmissions:
            self.retry_scheduled = True
            self.scheduler.call_later(
                self.retry_ms,
                self._retry_pending,
            )