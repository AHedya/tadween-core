# Broker

The `broker` package implements a message bus for inter-stage communication and state notification within Tadween.

## Concepts

- **Message**: The fundamental unit of communication. Contains a `topic`, `payload` (dict), and optional `metadata`.
- **BaseMessageBroker**: An abstract interface for all broker implementations (`publish`, `subscribe`, `ack`, `nack`).
- **InMemoryBroker**: A thread-safe, in-memory implementation for development and managed short-lived workflows.
- **BrokerListener**: An observer pattern implementation for monitoring broker events (e.g., stats collection, logging).

## Statistics & Monitoring

The `broker` package includes a `StatsCollector` (a `BrokerListener`) to track:
- Message throughput (published, processed, failed).
- Queue sizes per topic.
- Active dispatch threads.

## Usage Example

```python
from tadween_core.broker import InMemoryBroker, Message

broker = InMemoryBroker()

# Subscribe to a topic
def my_handler(msg: Message):
    print(f"Received message: {msg.payload}")

broker.subscribe("audio.downloaded", my_handler)

# Publish a message
msg = Message(topic="audio.downloaded", payload={"local_path": "/tmp/audio.mp3"})
broker.publish(msg)

# Graceful shutdown
broker.close(timeout=5.0)
```

## Observer Pattern

Implement `BrokerListener` to hook into the broker's lifecycle:

- `on_publish`: Called when a message is published.
- `on_message_dispatched`: Called when a message is retrieved for processing.
- `on_message_processed`: Called when a message is successfully processed.
- `on_message_failed`: Called when processing fails.

---
*For more examples, see [examples/broker.py](../../../examples/broker.py) (if available).*
