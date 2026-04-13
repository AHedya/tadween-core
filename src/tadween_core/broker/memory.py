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

THREAD_EXIT_GRACE: float = 0.2


class InMemoryBroker(BaseMessageBroker):
    """
    Simple in-memory broker for development/testing or even managed short-lived workflows.
    NOT suitable for production (no persistence, loses messages on crash).
    """

    def __init__(
        self, max_workers: int | None = None, logger: logging.Logger | None = None
    ):
        self._topics: dict[str, Queue[Message]] = {}
        # Each entry: (callable, auto_ack, handler_timeout)
        self._handlers: dict[str, tuple[tuple[Callable, bool, float | None], ...]] = {}
        # subscription_id -> (topic, handler, auto_ack, handler_timeout)
        self._subscriptions: dict[str, tuple[str, Callable, bool, float | None]] = {}
        self._lock = threading.Lock()
        self._running = True
        self._dispatch_threads: dict[str, threading.Thread] = {}
        self.logger = logger or logging.getLogger("tadween.broker.memory")

        self._executor = ThreadPoolExecutor(max_workers=max_workers)

        # Quiescence detection
        self._pending_acks: dict[str, int] = {}
        self._quiescence_cond = threading.Condition(self._lock)
        # observer pattern
        self._listeners: list[BrokerListener] = []

    def publish(self, message: Message) -> None:
        topic_created = False
        thread_started = False
        with self._lock:
            if not self._running:
                self.logger.warning("Broker already closed")
                raise RuntimeError("Broker closed")

            self._pending_acks[message.id] = self._pending_acks.get(message.id, 0) + 1

            if message.topic not in self._topics:
                self._topics[message.topic] = Queue()
                topic_created = True
            self._topics[message.topic].put(message)

            if message.topic not in self._dispatch_threads:
                self._start_dispatch_thread(message.topic)
                thread_started = True

        if topic_created:
            self._notify_listeners("on_topic_created", topic=message.topic)
        if thread_started:
            self._notify_listeners("on_dispatch_thread_started", topic=message.topic)
        self._notify_listeners("on_publish", message=message)

    def subscribe(
        self,
        topic: str,
        handler: Callable[[Message], None],
        auto_ack: bool = True,
        handler_timeout: float | None = None,
    ) -> str:
        """
        Subscribe *handler* to *topic*.

        Parameters
        ---
        topic:
            Topic to subscribe to.
        handler:
            Callable invoked for each message.
        auto_ack:
            When True (default) the message is acked automatically once the
            handler completes or times out. Set to False for manual ack/nack.
        handler_timeout:
            Soft upper bound (seconds) on handler execution time. best-effort:
            if the handler is still running after handler_timeout seconds we
            ack defensively and log a warning, but the thread runs to
            completion in the pool. None means no timeout, Dangerous if
            the handler can block indefinitely.
        """
        subscription_id = f"{topic}:{uuid.uuid4()}"
        thread_started = False

        with self._lock:
            current = self._handlers.get(topic, ())
            self._handlers[topic] = current + ((handler, auto_ack, handler_timeout),)
            self._subscriptions[subscription_id] = (
                topic,
                handler,
                auto_ack,
                handler_timeout,
            )

            if topic not in self._dispatch_threads:
                self._start_dispatch_thread(topic)
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
        with self._lock:
            threads = list(self._dispatch_threads.values())
        self._join_threads(threads)

        # All handlers are already acked (quiescence confirmed above).
        self._executor.shutdown(wait=False, cancel_futures=False)
        self.logger.info("Broker closed.")

    def ack(self, message_id: str) -> None:
        """Decrement the pending-ack reference count for a message.

        FIXME: The current implementation is not resilient to double-acks (e.g.
        a manual-ack handler that also triggers auto-ack).  This is a known
        limitation to be addressed in a future pass.
        """
        with self._quiescence_cond:
            if message_id not in self._pending_acks:
                self.logger.warning(f"Double Ack or Unknown Message ID: {message_id}")
                return
            self._pending_acks[message_id] -= 1
            if self._pending_acks[message_id] <= 0:
                del self._pending_acks[message_id]
                if not self._pending_acks:
                    self._quiescence_cond.notify_all()

    def nack(self, message_id: str, requeue_message: Message | None = None) -> None:
        self.ack(message_id)
        if requeue_message:
            self.publish(requeue_message)

    def add_listener(self, listener: BrokerListener) -> None:
        with self._lock:
            self._listeners.append(listener)

    def remove_listener(self, listener: BrokerListener) -> None:
        with self._lock:
            self._listeners.remove(listener)

    def _start_dispatch_thread(self, topic: str) -> None:
        """Start a dispatch thread for *topic*. Must be called under self._lock."""
        if topic not in self._dispatch_threads:
            thread = threading.Thread(
                target=self._dispatch_loop, args=(topic,), daemon=False
            )
            thread.start()
            self._dispatch_threads[topic] = thread

    def _stop_dispatch_threads(self) -> None:
        """
        Set _running=False and post a None sentinel to every topic queue so
        dispatch threads wake immediately instead of waiting polling.
        """
        with self._lock:
            self._running = False
            for q in self._topics.values():
                q.put(None)

    def _force_close(self) -> None:
        """Discard all queued / pending-ack work and stop immediately. Never raises."""
        self.logger.warning(
            "Force-closing broker. Pending / un-acked messages will be dropped."
        )
        with self._quiescence_cond:
            self._running = False
            for q in self._topics.values():
                while True:
                    try:
                        q.get_nowait()
                        q.task_done()
                    except Empty:
                        break
                q.put(None)
            self._pending_acks.clear()
            self._quiescence_cond.notify_all()

        self._executor.shutdown(wait=False, cancel_futures=True)
        with self._lock:
            threads = list(self._dispatch_threads.values())
        self._join_threads(threads)
        self.logger.info("Broker force-closed.")

    def _join_threads(
        self, threads: list[threading.Thread], timeout: float = THREAD_EXIT_GRACE
    ) -> None:
        """
        Join every dispatch thread within a fixed grace window.
        """
        for t in threads:
            t.join(timeout=timeout)
            if t.is_alive():
                self.logger.warning(
                    f"Dispatch thread [{t.name}] did not stop within {timeout}s grace period. "
                    "It will self-terminate once its current handler returns.",
                )

    def _make_done_callback(
        self,
        handler_func: Callable,
        message: Message,
        auto_ack: bool,
        handler_timeout: float | None,
        topic: str,
        submitted_at: float,
    ) -> Callable[[Future], None]:
        """
        Build and return the done_callback for a single handler submission.

        Factored out of _submit_handler so each closure captures its own
        distinct set of variables — avoids the classic loop-variable capture bug.

        The callback runs in a pool worker thread (the same thread that ran
        the handler, or any available worker for cancelled futures).  It is
        responsible for:
            - Detecting timeout by comparing elapsed time to handler_timeout
            - Firing on_message_processed / on_message_failed notifications
            - Ack-ing the message (auto_ack or timed_out or cancelled)
        """

        def done_callback(future: Future) -> None:
            # Cancelled futures arise during force-close (cancel_futures=True).
            if future.cancelled():
                self.ack(message.id)
                return

            elapsed = time.monotonic() - submitted_at
            timed_out = handler_timeout is not None and elapsed > handler_timeout
            exc = future.exception()

            if timed_out:
                self.logger.warning(
                    f"Handler {getattr(handler_func, '__qualname__', repr(handler_func))} exceeded timeout of {handler_timeout:.1f}s (actual: {elapsed:.1f}s) "
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

            if auto_ack or timed_out:
                self.ack(message.id)

        return done_callback

    def _submit_handler(
        self,
        handler_func: Callable,
        message: Message,
        auto_ack: bool,
        handler_timeout: float | None,
        topic: str,
    ) -> None:
        """
        Submit *handler_func* to the executor pool and attach a done_callback.

        The dispatch loop calls this and moves on immediately. All post-execution logic
        (acking, notifications, timeout detection) lives in the callback.

        Raises
            RuntimeError
                if the executor has been shut down (broker closing).
                The dispatch loop catches this and acks defensively.
        """
        submitted_at = time.monotonic()
        callback = self._make_done_callback(
            handler_func, message, auto_ack, handler_timeout, topic, submitted_at
        )
        self._executor.submit(handler_func, message).add_done_callback(callback)

    def _dispatch_loop(self, topic: str) -> None:
        """
        Dispatch messages to subscribers for a single topic.

        This loop is now a pure submission loop — it pulls a message from the
        queue, adjusts pending_acks for fan-out, submits each handler to the
        executor pool, and immediately moves to the next message.  No blocking
        on handler execution.
        """
        while self._running:
            try:
                q = self._topics.get(topic)
                if not q:
                    time.sleep(0.1)
                    continue
                try:
                    message = q.get(timeout=0.1)
                except Empty:
                    continue

                if message is None:  # stop sentinel from _stop_dispatch_threads
                    break

                try:
                    self._notify_listeners(
                        "on_message_dispatched", message=message, topic=topic
                    )

                    with self._lock:
                        handlers = self._handlers.get(topic, ())
                        num_handlers = len(handlers)

                    with self._quiescence_cond:
                        if num_handlers == 0:
                            # No subscribers — message is dead, clear its slot.
                            self._pending_acks[message.id] -= 1
                        else:
                            # publish() added 1 for the queue slot.
                            # Fan out to num_handlers. Net: +(num_handlers - 1)
                            self._pending_acks[message.id] += num_handlers - 1
                        if self._pending_acks.get(message.id, 0) <= 0:
                            self._pending_acks.pop(message.id, None)
                            if not self._pending_acks:
                                self._quiescence_cond.notify_all()

                    for handler_func, auto_ack, handler_timeout in handlers:
                        try:
                            self._submit_handler(
                                handler_func, message, auto_ack, handler_timeout, topic
                            )
                        except RuntimeError:
                            # Executor was shut down (broker closing mid-dispatch).
                            # Ack defensively so this slot doesn't leak.
                            self.logger.warning(
                                "Executor shut down while submitting handler for "
                                f"message(id={message.id}) — ack-ing defensively.",
                            )
                            self.ack(message.id)

                except Exception as e:
                    self.logger.error(
                        f"Error processing message [{getattr(message, 'id', 'unknown')}]: {e}",
                        exc_info=True,
                    )
                    if message:
                        self.ack(message.id)
                finally:
                    q.task_done()

            except Exception as e:
                self.logger.error(
                    f"Dispatch loop critical error on topic {topic}: {e}",
                    exc_info=True,
                )
                continue

        self._notify_listeners("on_dispatch_thread_stopped", topic=topic)

    def _notify_listeners(self, event: BrokerEvents, **kwargs) -> None:
        for listener in self._listeners:
            try:
                getattr(listener, event)(**kwargs)
            except Exception as e:
                self.logger.error(
                    f"Listener error in {event}: {e}",
                    exc_info=True,
                )
