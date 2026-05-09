import logging
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from queue import Empty, Queue

from .base import (
    BaseMessageBroker,
    BrokerEvents,
    BrokerListener,
    Message,
)

THREAD_EXIT_GRACE: float = 0.5


class InMemoryBroker(BaseMessageBroker):
    """
    Simple in-memory broker for development/testing or even managed short-lived workflows.
    NOT suitable for production (no persistence, loses messages on crash).
    Uses a unified dispatcher architecture where messages from all topics are centrally
    queued and routed to a worker pool.
    """

    def __init__(
        self, max_workers: int | None = 4, logger: logging.Logger | None = None
    ):
        self._unified_queue: Queue[Message | None] = Queue()
        # Each entry: (callable, auto_ack, handler_timeout)
        self._handlers: dict[str, tuple[tuple[Callable, bool, float | None], ...]] = {}
        # subscription_id -> (topic, handler, auto_ack, handler_timeout)
        self._subscriptions: dict[str, tuple[str, Callable, bool, float | None]] = {}
        self._lock = threading.Lock()
        self._running = True
        self._dispatch_thread: threading.Thread | None = None
        self._known_topics: set[str] = set()
        self.logger = logger or logging.getLogger("tadween.broker.memory")

        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="BrokerWorker",
        )

        # Quiescence detection
        self._pending_acks: set[str] = set()
        self._quiescence_cond = threading.Condition(self._lock)
        # observer pattern
        self._listeners: list[BrokerListener] = []

        self._start_dispatch_thread()

    def publish(self, message: Message) -> None:
        topic_created = False
        with self._lock:
            if not self._running:
                raise RuntimeError("Broker closed")

            if not self._dispatch_thread.is_alive():
                raise RuntimeError(
                    "Dispatch thread has exited unexpectedly; broker is unusable"
                )

            self._pending_acks.add(message.id)

            if message.topic not in self._known_topics:
                self._known_topics.add(message.topic)
                topic_created = True

            self._unified_queue.put(message)

        if topic_created:
            self._notify_listeners("on_topic_created", topic=message.topic)
        self._notify_listeners("on_publish", message=message)

    def subscribe(
        self,
        topic: str,
        handler: Callable[[Message], None],
        auto_ack: bool = True,
        handler_timeout: float | None = None,
    ) -> str:
        subscription_id = f"{topic}:{uuid.uuid4().hex}"

        with self._lock:
            if not self._running:
                raise RuntimeError("Broker closed")

            if not self._dispatch_thread.is_alive():
                raise RuntimeError(
                    "Dispatch thread has exited unexpectedly; broker is unusable"
                )

            current = self._handlers.get(topic, ())
            self._handlers[topic] = current + ((handler, auto_ack, handler_timeout),)
            self._subscriptions[subscription_id] = (
                topic,
                handler,
                auto_ack,
                handler_timeout,
            )

            topic_created = False
            if topic not in self._known_topics:
                self._known_topics.add(topic)
                topic_created = True

        if topic_created:
            self._notify_listeners("on_topic_created", topic=topic)
        self._notify_listeners(
            "on_subscribe",
            topic=topic,
            handler=handler,
            subscription_id=subscription_id,
        )
        return subscription_id

    def unsubscribe(self, subscription_id: str) -> None:
        with self._lock:
            if subscription_id not in self._subscriptions:
                return
            topic, handler, *_ = self._subscriptions.pop(subscription_id)

            if topic in self._handlers:
                self._handlers[topic] = tuple(
                    h for h in self._handlers[topic] if h[0] is not handler
                )
                if not self._handlers[topic]:
                    del self._handlers[topic]
                    self._known_topics.discard(topic)

        self._notify_listeners(
            "on_unsubscribe", topic=topic, subscription_id=subscription_id
        )

    def join(self, timeout: float | None = None) -> bool:
        """Block until all pending acks are resolved (quiescence)."""
        with self._quiescence_cond:
            return self._quiescence_cond.wait_for(
                lambda: len(self._pending_acks) == 0, timeout=timeout
            )

    def close(self, timeout: float | None = None, force: bool = False) -> None:
        """
        Shut down the broker.

        Parameters
        ---
        timeout:
            Wall-clock budget (seconds) to wait for quiescence ONLY.
            Dispatch thread have their own graceful timeout

        force:
            Skip waiting for quiescence: cancel queued handler futures, discard
            all pending / un-acked messages, stop immediately.  Never raises.

        Raises
        ---
        TimeoutError
            Only on graceful close (force=False) when quiescence is not reached
            within `timeout`.  Dispatch threads are signalled to stop before
            the exception is raised so the broker is left in a clean state.
        """
        if not self._running:
            self.logger.warning("Broker already closed. Quit")
            return

        if force:
            self._force_close()
            return

        self.logger.info("Broker closing... waiting for pending tasks.")

        quiesced = self.join(timeout=timeout)
        if not quiesced:
            self._stop_dispatch_threads()
            self._executor.shutdown(wait=False, cancel_futures=False)
            raise TimeoutError(
                "Broker close timed out waiting for quiescence; "
                "un-acked messages remain. Use force=True to discard them."
            )

        self._stop_dispatch_threads()
        if self._dispatch_thread:
            self._dispatch_thread.join(timeout=THREAD_EXIT_GRACE)

        self._executor.shutdown(wait=False, cancel_futures=False)
        self.logger.info("Broker closed.")

    def ack(self, message_id: str) -> None:
        """
        Acknowledge message processing.
        Resilient to double-acks: a second call for the same ID is logged and
        ignored rather than raising.
        """
        with self._lock:
            if message_id not in self._pending_acks:
                self.logger.warning(f"Double Ack or Unknown Message ID: {message_id}")
                return
            self._pending_acks.discard(message_id)
            if not self._pending_acks:
                self._quiescence_cond.notify_all()

    def nack(self, message_id: str, requeue_message: Message | None = None) -> None:
        """
        Negative-acknowledge a message, and optionally requeueing new or the same message.
        """
        # requeue first because if ack-ed first there will be a short window of
        # quiescence being detected which will close the broker
        if isinstance(requeue_message, Message):
            self.publish(requeue_message)
        self.ack(message_id)

    def add_listener(self, listener: BrokerListener) -> None:
        with self._lock:
            if listener not in self._listeners:
                self._listeners.append(listener)

    def remove_listener(self, listener: BrokerListener) -> None:
        with self._lock:
            if listener in self._listeners:
                self._listeners.remove(listener)

    def _start_dispatch_thread(self) -> None:
        """Start the unified dispatch thread."""
        self._dispatch_thread = threading.Thread(
            target=self._dispatch_loop,
            name="InMemoryBrokerDispatcher",
            daemon=False,
        )
        self._dispatch_thread.start()

    def _stop_dispatch_threads(self) -> None:
        """
        Set _running=False and post a None sentinel to the unified queue.
        """
        with self._lock:
            if not self._running:
                return
            self._running = False
            self._unified_queue.put(None)

    def _force_close(self) -> None:
        """Discard all queued / pending-ack work and stop immediately. Never raises."""
        self.logger.warning(
            "Force-closing broker. Pending / un-acked messages will be dropped."
        )
        with self._quiescence_cond:
            self._running = False
            while True:
                try:
                    self._unified_queue.get_nowait()
                    self._unified_queue.task_done()
                except Empty:
                    break
            self._unified_queue.put(None)
            self._pending_acks.clear()
            self._quiescence_cond.notify_all()

        self._executor.shutdown(wait=False, cancel_futures=True)
        if self._dispatch_thread:
            self._dispatch_thread.join(timeout=THREAD_EXIT_GRACE)
        self.logger.info("Broker force-closed.")

    def _submit_handler(
        self,
        handler_func: Callable,
        message: Message,
        auto_ack: bool,
        handler_timeout: float | None,
        topic: str,
    ) -> None:
        """
        Submit handler to pool. Uses an inner task wrapper to measure *actual*
        execution time rather than including time spent waiting in the thread pool queue.
        """

        def task_wrapper() -> tuple[float, Exception | None]:
            start_time = time.monotonic()
            try:
                handler_func(message)
                return time.monotonic() - start_time, None
            except Exception as e:
                return time.monotonic() - start_time, e

        def done_callback(future: Future) -> None:
            # Cancelled futures arise during force-close (cancel_futures=True).
            if future.cancelled():
                self.ack(message.id)
                return

            try:
                elapsed, exc = future.result()
            except Exception as e:
                # Fallback if the wrapper itself somehow crashed
                elapsed, exc = 0.0, e

            timed_out = handler_timeout is not None and elapsed > handler_timeout

            if timed_out:
                self.logger.warning(
                    f"Handler {getattr(handler_func, '__qualname__', repr(handler_func))} "
                    f"exceeded timeout of {handler_timeout:.1f}s (actual: {elapsed:.1f}s) "
                    f"on message(id:{message.id}) (topic={topic}). Acking defensively.",
                )
                self._notify_listeners(
                    "on_message_failed",
                    message=message,
                    topic=topic,
                    error=TimeoutError(
                        f"Handler timed out after {elapsed:.1f}s "
                        f"(limit: {handler_timeout}s)"
                    ),
                )
            elif exc is not None:
                self.logger.error(f"Broker handler error: {exc}", exc_info=True)
                self._notify_listeners(
                    "on_message_failed", message=message, topic=topic, error=exc
                )
            else:
                self._notify_listeners(
                    "on_message_processed", message=message, topic=topic
                )

            if auto_ack or timed_out or exc is not None:
                self.ack(message.id)

        self._executor.submit(task_wrapper).add_done_callback(done_callback)

    def _dispatch_loop(self) -> None:
        """
        Dispatch messages to subscribers from the unified queue.
        """
        while self._running:
            try:
                try:
                    message = self._unified_queue.get(timeout=0.1)
                except Empty:
                    continue

                if message is None:  # stop sentinel
                    break

                topic = message.topic
                self._notify_listeners(
                    "on_message_dispatched", message=message, topic=topic
                )

                forked_payloads = []

                with self._lock:
                    handlers = self._handlers.get(topic, ())
                    # Fork messages and track them BEFORE removing the root message. Prevent premature quiescence detection in case of fan-out
                    for handler_func, auto_ack, handler_timeout in handlers:
                        forked_msg = message.fork()
                        forked_msg.metadata["parent_message_id"] = message.id
                        self._pending_acks.add(forked_msg.id)
                        forked_payloads.append(
                            (handler_func, auto_ack, handler_timeout, forked_msg)
                        )

                    # release the root message placeholder now that forks exist
                    self._pending_acks.discard(message.id)
                    if not self._pending_acks:
                        self._quiescence_cond.notify_all()

                # Dispatch outside the lock to keep lock time minimal
                for (
                    handler_func,
                    auto_ack,
                    handler_timeout,
                    forked_msg,
                ) in forked_payloads:
                    try:
                        self._submit_handler(
                            handler_func, forked_msg, auto_ack, handler_timeout, topic
                        )
                    except RuntimeError:
                        self.logger.warning(
                            "Executor shut down while submitting handler for "
                            f"message(id={forked_msg.id}) — ack-ing defensively."
                        )
                        self.ack(forked_msg.id)

            except Exception as e:
                self.logger.error(f"Dispatch loop critical error: {e}", exc_info=True)
                # ensure the message isn't permanently leaked if loop crashes
                if "message" in locals() and message is not None:
                    self.ack(message.id)

    def _notify_listeners(self, event: BrokerEvents, **kwargs) -> None:
        with self._lock:
            listeners = list(self._listeners)

        for listener in listeners:
            try:
                getattr(listener, event)(**kwargs)
            except Exception as e:
                self.logger.error(
                    f"Listener error in {event}: {e}",
                    exc_info=True,
                )
