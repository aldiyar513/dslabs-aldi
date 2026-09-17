from dataclasses import dataclass, field
from typing import Any

from dslabs.protocols import Message, Scheduler, Transport


@dataclass
class NodeTotalOrder:
    """Node Total OOrder
    The leader receives the messages, orders them, and broadcasts
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
    next_seq: int = 1
    expected_seq: int = 1
    buffer: dict[int, Message] = field(default_factory=dict)


    # Client-facing API
    def client_put(self, key: str, value: Any) -> None:
        if self.node_id == self.leader_id: 
            self.receive_and_replicate(key, value)
        else:
            self.transport.send(
                self.leader_id,
                {
                    "type": "client_write",
                    "key": key,
                    "value": value
                }
            )

    def client_get(self, key: str) -> Any:
        return self.store.get(key)

    # Node internals
    def receive_and_replicate(self, key: str, value: Any) -> None:
        seq = self.next_seq
        self.next_seq += 1

        msg = {
            "type": "replicate",
            "key": key,
            "value": value,
            "seq": seq
        }

        self.process_ordered(msg)

        for peer in self.peers:
            if peer != self.node_id:
                self.transport.send(peer, msg)

    def receive(self, key: str, value: Any) -> None:
        # Received messages are delivered immediately
        self.deliver(key, value)

    def deliver(self, key: str, value: Any) -> None:
        """Apply a message to this node: update the store, append to the log."""
        self.store[key] = value
        self.log.append((key, value))

    # Network handler
    def process_ordered(self, msg: Message) -> None:
        seq = msg["seq"]

        if seq == self.expected_seq:
            self.receive(msg["key"], msg["value"])
            self.expected_seq += 1

            while self.expected_seq in self.buffer:
                buffered_msg = self.buffer.pop(self.expected_seq)

                self.receive(
                    buffered_msg["key"],
                    buffered_msg["value"]
                )

                self.expected_seq += 1

        elif seq > self.expected_seq:
            self.buffer[seq] = msg
    
    def on_message(self, msg: Message) -> None:
        if msg["type"] == "client_write":

            if self.node_id == self.leader_id:
                self.receive_and_replicate(
                    msg["key"],
                    msg["value"]
                )

        elif msg["type"] == "replicate":
            self.process_ordered(msg)

        else:
            raise ValueError(f"Unknown message type in {msg!r}")
    
    # What the trace and diagrams show as this node's state
    def brief_state(self) -> dict[str, Any]:
        return dict(self.store)
