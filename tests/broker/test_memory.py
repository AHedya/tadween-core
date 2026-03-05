"""
Comprehensive test suite for InMemoryBroker.

Tests cover:
- One-to-one communication
- One-to-many communication
- Topic chaining (one-to-one chain)
- Chaining in one-to-many scenarios
- Subscription management
- Error handling
- Lifecycle management
- Concurrency and thread safety
- Edge cases
"""

import queue
import threading
import time  # noqa

import pytest

from tadween_core.broker.base import Message
from tadween_core.broker.memory import InMemoryBroker


@pytest.fixture
def broker():
    b = InMemoryBroker()
    yield b
    b.close(3)


class TestUsage:
    def test_no_interactions(self, broker: InMemoryBroker):
        broker  # noqa

    def test_no_publish(self, broker: InMemoryBroker):
        broker.subscribe("test.init", lambda x: print("received"))

    def test_topic_with_no_handler(self, broker: InMemoryBroker):
        """A message with no consumer (handler) would stay indefinitely in our topics queues.
        Unless settings timeout for joining/waiting topics"""

        for _ in range(3):
            broker.publish(Message(topic="test.init"))
        broker.close(1)


class TestInternals:
    def test_message_id_generation(self, broker: InMemoryBroker):
        """Test that messages get unique IDs."""
        messages = []

        def handler(msg: Message):
            messages.append(msg)

        broker.subscribe("test.topic", handler)

        for i in range(3):
            broker.publish(Message(topic="test.topic", payload={"data": i}))

        broker.join(timeout=2.0)

        assert len(messages) == 3
        assert all(msg.id is not None for msg in messages)
        # IDs should be unique
        ids = [msg.id for msg in messages]
        assert len(set(ids)) == 3


def test_one_to_one(broker: InMemoryBroker):
    # Can't reliably test with assertions in multi-threaded without using synchronization
    event = threading.Event()

    def handler(msg: Message):
        if msg.payload:
            event.set()

    broker.subscribe("test.topic", handler)
    broker.publish(Message(topic="test.topic", payload={"data": "hello"}))

    assert event.wait(timeout=1), "Message not received within timeout"


def test_one_to_many(broker: InMemoryBroker):
    """One topic, multiple handlers/listeners"""
    received = queue.Queue()

    def make_handler(handler_id: int):
        def handler(_: Message):
            received.put(handler_id)

        return handler

    for i in range(10):
        broker.subscribe("test.topic", make_handler(i))

    broker.publish(Message(topic="test.topic", payload={"data": "hello"}))

    handler_ids = set()
    for _ in range(10):
        try:
            handler_ids.add(received.get(timeout=1.0))
        except queue.Empty:
            break

    assert len(handler_ids) == 10, (
        f"Expected 10 handlers to fire, got {len(handler_ids)}"
    )


def test_subscription_chain(broker: InMemoryBroker):
    event = threading.Event()

    def make_handler(next_topic):
        def handler(_: Message):
            broker.publish(Message(topic=next_topic))

        return handler

    for i in range(0, 5):
        topic = f"test.{i}"
        next_topic = f"test.{i + 1}"
        # perform chain effect
        broker.subscribe(topic, make_handler(next_topic))

    broker.subscribe(next_topic, lambda x: event.set())
    broker.publish(Message(topic="test.0"))

    assert event.wait(2), "didn't chain"


def test_subscription_chain_with_sideeffect(broker: InMemoryBroker):
    received = queue.Queue()

    def make_handler(next_topic):
        def handler(_: Message):
            broker.publish(Message(topic=next_topic))

        return handler

    def make_second_handler(next_topic):
        def handler(_: Message):
            received.put(next_topic)

        return handler

    for i in range(0, 5):
        topic = f"test.{i}"
        next_topic = f"test.{i + 1}"
        # perform chain effect
        broker.subscribe(topic, make_handler(next_topic))
        # perform side effect
        broker.subscribe(topic, make_second_handler(next_topic))

    # never leave our message not acknowledged, or it will hang our broker
    broker.subscribe(next_topic, lambda x: None)

    broker.publish(Message(topic="test.0"))

    rec = []
    while True:
        try:
            rec.append(received.get(timeout=1.0))
        except queue.Empty:
            break
    assert len(rec) == 5
    assert rec == ["test.1", "test.2", "test.3", "test.4", "test.5"]
