import logging
import threading
import time
import uuid
from collections.abc import Callable
from queue import Empty, Queue

from .base import (
    BaseMessageBroker,
    BrokerEvents,
    BrokerListener,
    Message,
)

logger = logging.getLogger(__name__)


class InMemoryBroker(BaseMessageBroker):
    """
    Simple in-memory broker for development/testing or even managed short-lived workflows.
    NOT suitable for production (no persistence, loses messages on crash).
    """

    def __init__(self):
        self._topics: dict[str, Queue[Message]] = {}
        # self._handlers: dict[str, tuple[Callable]] = {}
        self._handlers: dict[str, tuple[tuple[Callable, bool]]] = {}
        # subscription_id -> (topic, handler, auto_ack)
        self._subscriptions: dict[str, tuple[str, Callable, bool]] = {}
        self._lock = threading.Lock()
        self._running = True
        self._dispatch_threads: dict[str, threading.Thread] = {}

        # Quiescence detection
        self._pending_acks: dict[str, int] = {}
        self._quiescence_cond = threading.Condition(self._lock)
        # observer pattern
        self._listeners: list[BrokerListener] = []

    def publish(self, message: Message) -> None:
        topic_created = False
        with self._lock:
            if not self._running:
                logger.warning("Broker already closed")
                raise RuntimeError("Broker closed")

            self._pending_acks[message.id] = self._pending_acks.get(message.id, 0) + 1

            if message.topic not in self._topics:
                self._topics[message.topic] = Queue()
                topic_created = True
            self._topics[message.topic].put(message)

        if topic_created:
            self._notify_listeners("on_topic_created", topic=message.topic)
        self._notify_listeners("on_publish", message=message)

    def subscribe(
        self,
        topic: str,
        handler: Callable[[Message], None],
        auto_ack: bool = True,
    ) -> str:
        subscription_id = f"{topic}:{uuid.uuid4()}"
        thread_started = False

        with self._lock:
            # copy-on-write
            current = self._handlers.get(topic, ())
            self._handlers[topic] = current + ((handler, auto_ack),)

            self._subscriptions[subscription_id] = (topic, handler, auto_ack)

            # Start dispatch thread for this topic if not exists
            if topic not in self._dispatch_threads:
                thread = threading.Thread(
                    target=self._dispatch_loop, args=(topic,), daemon=False
                )
                thread.start()
                self._dispatch_threads[topic] = thread
                thread_started = True

        if thread_started:
            self._notify_listeners("on_dispatch_thread_started", topic=topic)
        self._notify_listeners(
            "on_subscribe",
            topic=topic,
            handler=handler,
            subscription_id=subscription_id,
        )
        return subscription_id

    def _dispatch_loop(self, topic: str):
        """Dispatch messages to subscribers"""
        while self._running:
            try:
                queue = self._topics.get(topic)
                if not queue:
                    time.sleep(0.1)
                    continue
                message = queue.get(timeout=0.1)
                if message is None:
                    break

                self._notify_listeners(
                    "on_message_dispatched", message=message, topic=topic
                )

                handlers = self._handlers.get(topic, ())
                num_handlers = len(handlers)
                with self._quiescence_cond:
                    if num_handlers == 0:
                        # No subscribers? Message is dead. Remove queue ref.
                        self._pending_acks[message.id] -= 1
                    else:
                        # If we have 3 handlers, we remove 1 (queue) and add 3 (handlers)
                        # Net change: +2
                        # If we have 1 handler, Net change: 0
                        self._pending_acks[message.id] += num_handlers - 1
                    if self._pending_acks[message.id] <= 0:
                        del self._pending_acks[message.id]
                        if not self._pending_acks:
                            self._quiescence_cond.notify_all()
                for handler_func, auto_ack in handlers:
                    try:
                        handler_func(message)
                        self._notify_listeners(
                            "on_message_processed", message=message, topic=topic
                        )
                    except Exception as e:
                        logger.error(f"Broker handler error: {e}", exc_info=True)
                        self._notify_listeners(
                            "on_message_failed", message=message, topic=topic, error=e
                        )
                    finally:
                        if auto_ack:
                            self.ack(message)
                queue.task_done()
            except Empty:
                continue
            except Exception as e:
                logger.error(f"Dispatch loop error: {e}", exc_info=True)
                continue

    def unsubscribe(self, subscription_id: str) -> None:
        with self._lock:
            if subscription_id not in self._subscriptions:
                return
            topic, handler, _ = self._subscriptions.pop(subscription_id)

            if topic in self._handlers:
                self._handlers[topic] = tuple(
                    [h for h in self._handlers[topic] if h is not handler]
                )
                if not self._handlers[topic]:
                    del self._handlers[topic]

        self._notify_listeners(
            "on_unsubscribe", topic=topic, subscription_id=subscription_id
        )

    def join(self, timeout: float | None = None) -> bool:
        """Blocks until pending_acks dictionary is empty."""
        with self._quiescence_cond:
            success = self._quiescence_cond.wait_for(
                lambda: len(self._pending_acks) == 0, timeout=timeout
            )
        return success

    def close(self, timeout: float | None = None) -> None:
        if not self._running:
            logger.warning("Broker already closed. Quit")
            return
        logger.info("Broker closing... waiting for pending tasks.")

        self.join(timeout=timeout)

        with self._lock:
            self._running = False
            threads = list(self._dispatch_threads.values())
        for t in threads:
            t.join(timeout=timeout)
        logger.info("Broker closed.")

    def ack(self, message: Message) -> None:
        """
        Decrements the reference count for a specific message.
        Validates that the message is actually pending.
        """
        with self._quiescence_cond:
            if message.id not in self._pending_acks:
                logger.warning(f"Double Ack or Unknown Message ID: {message.id}")
                return
            self._pending_acks[message.id] -= 1

            # Debug logging
            # logger.debug(f"Ack {message.id}. Remaining refs: {self._pending_acks[message.id]}")

            if self._pending_acks[message.id] <= 0:
                del self._pending_acks[message.id]
                # If Dictionary is empty, the entire system is idle
                if not self._pending_acks:
                    self._quiescence_cond.notify_all()

    def nack(self, message: Message, requeue: bool = True) -> None:
        self.ack(message)
        if requeue:
            self.publish(message)

    def add_listener(self, listener: BrokerListener) -> None:
        """Add a listener to receive broker events"""
        with self._lock:
            self._listeners.append(listener)

    def remove_listener(self, listener: BrokerListener) -> None:
        """Remove a listener"""
        with self._lock:
            self._listeners.remove(listener)

    def _notify_listeners(self, event: BrokerEvents, **kwargs) -> None:
        """Observer pattern. Notify all listeners of an event"""
        for listener in self._listeners:
            try:
                getattr(listener, event)(**kwargs)
            except Exception as e:
                logger.error(f"Listener error in {event}: {e}", exc_info=True)
