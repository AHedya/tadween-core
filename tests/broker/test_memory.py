import queue
import threading
import time

import pytest

from tadween_core.broker.base import Message
from tadween_core.broker.memory import InMemoryBroker


@pytest.fixture
def broker():
    b = InMemoryBroker()
    yield b
    b.close(3)


def test_no_interactions(broker: InMemoryBroker):
    broker  # noqa


def test_no_publish(broker: InMemoryBroker):
    broker.subscribe("test.init", lambda x: print("received"))


def test_topic_with_no_handler(broker: InMemoryBroker):
    """A message with no consumer (handler) would stay indefinitely in our topics queues.
    Unless settings timeout for joining/waiting topics"""

    for _ in range(3):
        broker.publish(Message(topic="test.init"))
    broker.close(1)


def test_message_id_generation(broker: InMemoryBroker):
    """Test that messages get unique IDs."""
    messages = []

    def handler(msg: Message):
        messages.append(msg)

    broker.subscribe("test.topic", handler)

    for i in range(3):
        broker.publish(Message(topic="test.topic", payload={"data": i}))

    broker.join(timeout=5.0)

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

    assert event.wait(timeout=8), "Message not received within timeout"


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


@pytest.mark.xfail(reason="Flaky due to thread scheduling", strict=False)
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

    assert event.wait(timeout=5), "didn't chain"


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

    broker.subscribe(next_topic, lambda x: None)

    broker.publish(Message(topic="test.0"))

    rec = []
    while True:
        try:
            rec.append(received.get(timeout=1.0))
        except queue.Empty:
            break
    assert len(rec) == 5


def test_no_subscribers_no_hang():
    """Verify that publishing to a topic with no subscribers doesn't cause a hang on close."""
    broker = InMemoryBroker()
    broker.publish(Message(topic="no_one", payload={"data": 1}))

    # Functional check: does it terminate within a generous upper bound?
    # This proves the "hang forever" bug is gone without being brittle.
    broker.close(timeout=2.0)
    assert not broker._running


def test_multiple_messages_no_subscribers():
    broker = InMemoryBroker()
    for i in range(10):
        broker.publish(Message(topic="no_one", payload={"data": i}))

    broker.close(timeout=2.0)
    assert not broker._running


def test_force_close_quits_immediately():
    """Verify that force=True skips the wait entirely."""
    broker = InMemoryBroker()
    # Publish something that won't be ACKed (no subscribers)
    broker.publish(Message(topic="unresponsive", payload={"d": 1}))

    start = time.perf_counter()
    # Even though there's a pending message, force=True should quit now.
    broker.close(timeout=10.0, force=True)
    duration = time.perf_counter() - start

    # Should be nearly instantaneous (well under the timeout)
    assert duration < 1.0
    assert not broker._running


def test_close_raises_timeout_error():
    """Verify that close() raises TimeoutError if quiescence is not reached."""
    broker = InMemoryBroker()

    # We need a subscriber that doesn't ACK to prevent quiescence
    broker.subscribe("unresponsive", lambda m: None, auto_ack=False)
    broker.publish(Message(topic="unresponsive", payload={"d": 1}))

    with pytest.raises(TimeoutError):
        # Use a very small timeout
        broker.close(timeout=0.2)
    broker.close()


def test_robust_error_handling_in_dispatch():
    """Verify that if a listener fails, the message is still acked and doesn't cause hang."""
    broker = InMemoryBroker()

    class BadListener:
        def on_message_dispatched(self, **kwargs):
            raise RuntimeError("Disaster!")

        def __getattr__(self, name):
            return lambda **kwargs: None

    broker.add_listener(BadListener())

    # We need a subscriber to trigger on_message_dispatched
    received = []
    broker.subscribe("test.topic", lambda m: received.append(m))

    broker.publish(Message(topic="test.topic", payload={"data": "hello"}))

    # join() should not hang even though listener failed
    success = broker.join(timeout=1.0)
    assert success
    assert len(received) == 1
    broker.close(timeout=1.0)
