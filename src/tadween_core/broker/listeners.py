import threading
import time
from collections.abc import Callable

from .base import BrokerListener, BrokerStats, Message, TopicStats


class StatsCollector(BrokerListener):
    """
    Collects and maintains broker statistics from events.

    All statistics are derived from broker events - completely self-contained.

    NOTE ON METRICS:
    - 'messages_published': Counts actual messages entering the broker.
    - 'messages_processed': Counts successful handler executions.
    (If 1 message has 2 handlers, this increments by 2).
    - 'messages_failed': Counts failed handler executions.
    - 'queue_size': Approximate depth of the queue (messages waiting to be dispatched).
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._start_time = time.perf_counter()

        # State derived purely from events
        self._topic_counters: dict[
            str, dict
        ] = {}  # topic -> {published, processed, failed}
        self._queue_sizes: dict[str, int] = {}  # topic -> approx messages in queue

        self._subscription_map: dict[
            str, tuple[str, Callable]
        ] = {}  # sub_id -> (topic, handler)
        self._topic_handlers: dict[
            str, list[Callable]
        ] = {}  # topic -> list of handlers
        self._active_threads: set[str] = set()

    def on_publish(self, message: Message) -> None:
        with self._lock:
            topic = message.topic

            # Initialize counters if needed
            if topic not in self._topic_counters:
                self._topic_counters[topic] = {
                    "published": 0,
                    "processed": 0,
                    "failed": 0,
                }

            # Update counters
            self._topic_counters[topic]["published"] += 1

            # Update Queue Size: Message is waiting in queue
            self._queue_sizes[topic] = self._queue_sizes.get(topic, 0) + 1

    def on_message_dispatched(self, message: Message, topic: str) -> None:
        with self._lock:
            # Update Queue Size: Message removed from queue to be handled
            if topic in self._queue_sizes:
                self._queue_sizes[topic] -= 1

    def on_message_processed(self, message: Message, topic: str) -> None:
        with self._lock:
            # This event fires per HANDLER execution
            if topic in self._topic_counters:
                self._topic_counters[topic]["processed"] += 1

    def on_message_failed(self, message: Message, topic: str, error: Exception) -> None:
        with self._lock:
            # This event fires per HANDLER execution
            if topic in self._topic_counters:
                self._topic_counters[topic]["failed"] += 1

    def on_subscribe(self, topic: str, handler: Callable, subscription_id: str) -> None:
        with self._lock:
            self._subscription_map[subscription_id] = (topic, handler)

            if topic not in self._topic_handlers:
                self._topic_handlers[topic] = []
            self._topic_handlers[topic].append(handler)

    def on_unsubscribe(self, topic: str, subscription_id: str) -> None:
        with self._lock:
            # Only remove if we actually tracked this subscription
            # (handles case where listener was attached after subscription existed)
            if subscription_id not in self._subscription_map:
                return

            _, handler = self._subscription_map.pop(subscription_id)

            # Remove handler from topic list
            if topic in self._topic_handlers:
                try:
                    self._topic_handlers[topic].remove(handler)
                    if not self._topic_handlers[topic]:
                        del self._topic_handlers[topic]
                except ValueError:
                    pass

    def on_topic_created(self, topic: str) -> None:
        with self._lock:
            # Ensure topic exists in counters
            if topic not in self._topic_counters:
                self._topic_counters[topic] = {
                    "published": 0,
                    "processed": 0,
                    "failed": 0,
                }
            if topic not in self._queue_sizes:
                self._queue_sizes[topic] = 0

    def on_dispatch_thread_started(self, topic: str) -> None:
        with self._lock:
            self._active_threads.add(topic)

    def get_stats(self) -> BrokerStats:
        """
        Generate current statistics snapshot.
        """
        with self._lock:
            topics_stats = {}
            all_topics = set(self._topic_counters.keys())

            for topic in all_topics:
                # Calculate subscription count
                subscription_count = sum(
                    1 for t, _ in self._subscription_map.values() if t == topic
                )

                # Get handler info
                handlers = self._topic_handlers.get(topic, [])
                handler_names = self._extract_handler_names(handlers)

                # Get message counters
                counters = self._topic_counters.get(topic, {})

                topics_stats[topic] = TopicStats(
                    topic_name=topic,
                    handler_count=len(handlers),
                    subscription_count=subscription_count,
                    queue_size=self._queue_sizes.get(topic, 0),
                    messages_published=counters.get("published", 0),
                    messages_processed=counters.get("processed", 0),
                    messages_failed=counters.get("failed", 0),
                    handler_names=handler_names,
                    is_active=topic in self._active_threads,
                )

            uptime = time.perf_counter() - self._start_time

            # Calculate totals
            total_subscriptions = len(self._subscription_map)
            total_handlers = sum(
                len(handlers) for handlers in self._topic_handlers.values()
            )

            return BrokerStats(
                total_topics=len(topics_stats),
                total_subscriptions=total_subscriptions,
                total_handlers=total_handlers,
                topics=topics_stats,
                uptime_seconds=uptime,
            )

    @staticmethod
    def _extract_handler_names(handlers: list[Callable]) -> list[str]:
        """Extract human-readable names from handlers"""
        names = []
        for handler in handlers:
            if hasattr(handler, "__name__"):
                names.append(handler.__name__)
            elif hasattr(handler, "__class__"):
                names.append(f"{handler.__class__.__name__}.__call__")
            else:
                names.append(str(type(handler).__name__))
        return names

    @staticmethod
    def pprint(stats: BrokerStats, detailed: bool = True):
        """Pretty print"""
        print("\n" + "=" * 60)
        print("BROKER STATISTICS")
        print("=" * 60)
        print(f"Uptime: {stats.uptime_seconds:.2f}s")
        print(f"Total Topics: {stats.total_topics}")
        print(f"Total Subscriptions: {stats.total_subscriptions}")
        print(f"Total Handlers: {stats.total_handlers}")

        if detailed and stats.topics:
            print("\n" + "-" * 60)
            print("TOPIC DETAILS")
            print("-" * 60)

            for topic_name, topic_stats in sorted(stats.topics.items()):
                print(f"\nTopic: {topic_name}")
                print(f"  Status: {'Active' if topic_stats.is_active else 'Inactive'}")
                print(f"  Handlers: {topic_stats.handler_count}")
                print(f"  Subscriptions: {topic_stats.subscription_count}")
                print(f"  Queue Size (Waiting): {topic_stats.queue_size}")
                print(f"  Messages Published: {topic_stats.messages_published}")
                print(
                    f"  Handler Executions (Success): {topic_stats.messages_processed}"
                )
                print(f"  Handler Executions (Failed): {topic_stats.messages_failed}")

                if topic_stats.handler_names:
                    print("  Handler Names:")
                    for handler_name in topic_stats.handler_names:
                        print(f"    - {handler_name}")

        print("=" * 60 + "\n")

    def print_stats(self, detailed: bool = True) -> None:
        """
        Print formatted statistics to stdout.
        """
        stats = self.get_stats()
        self.pprint(stats, detailed)
