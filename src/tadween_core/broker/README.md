# Broker

The `broker` subpackage implements a message bus for inter-stage communication and state notification within Tadween.

Typically, message bus should only deliver messages and doesn't do any work. In other words, make handlers return immediately (submit work)

## Concepts

- **Message**: The fundamental unit of communication. Contains a `topic`, `payload` (dict), and optional `metadata`.
- **BaseMessageBroker**: An abstract interface for all broker implementations (`publish`, `subscribe`, `ack`, `nack`).
- **InMemoryBroker**: A thread-safe, in-memory implementation for development and managed short-lived workflows.
- **BrokerListener**: An observer pattern implementation for monitoring broker events (e.g., stats collection, logging).

## Statistics & Monitoring

The `broker` subpackage includes a `StatsCollector` (a `BrokerListener`) to track:
- Message throughput (published, processed, failed).
- Queue sizes per topic.
- Active dispatch threads.

## Anatomy
The `broker` subpackage anatomy

```
└── broker
    ├── __init__.py
    ├── base.py         => defines broker contract, observer (or listener) contract, data models, and hinting.
    ├── listeners.py    => Implemented listeners for broker (observer pattern)
    ├── memory.py       => InMemoryBroker implementation
    └── README.md
```

## Usage Example

*For examples, see [examples/broker.py](../../../examples/broker/README.md)*

## InMemoryBroker

Must be closed after any submissions, lest dispatched threads will hang the main process.
Make sure to consider settings `timeout` in `broker.close`, unless you want to wait indefinitely and are sure there's no un-acknowledged message, they can hang the broker forever.

## Observer Pattern

Implement `BrokerListener` to hook into the broker's lifecycle:

- `on_publish`: Called when a message is published.
- `on_message_dispatched`: Called when a message is retrieved for processing.
- `on_message_processed`: Called when a message is successfully processed.
- `on_message_failed`: Called when processing fails.
