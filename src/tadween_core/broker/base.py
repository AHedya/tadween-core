import copy
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Annotated, Literal, TypeAlias


@dataclass(slots=True)
class Message:
    topic: Annotated[str, "Topic to subscribe to"]
    payload: dict | None = None
    metadata: dict | None = None
    id: str | None = None

    def __post_init__(self):
        self.id = self.id if self.id else str(uuid.uuid4())

        if self.payload is None:
            self.payload = {}
        if self.metadata is None:
            self.metadata = {}

    def fork(
        self,
        topic: str | None = None,
        payload: dict | None = None,
        metadata: dict | None = None,
    ) -> "Message":
        """
        Create a deep copy of the message with a new UUID.
        """
        return Message(
            topic=topic or self.topic,
            payload=payload or copy.deepcopy(self.payload),
            metadata=metadata or copy.deepcopy(self.metadata),
            id=str(uuid.uuid4()),
        )


@dataclass(slots=True)
class TopicStats:
    """Statistics for a specific topic"""

    topic_name: str
    handler_count: int
    subscription_count: int
    queue_size: int
    messages_published: int = 0
    messages_processed: int = 0  # Counts successful handler executions
    messages_failed: int = 0  # Counts failed handler executions
    handler_names: list[str] = field(default_factory=list)
    is_active: bool = True

    def to_dict(self) -> dict:
        """Convert topic stats to a dictionary"""
        return {
            "topic_name": self.topic_name,
            "handler_count": self.handler_count,
            "subscription_count": self.subscription_count,
            "queue_size": self.queue_size,
            "messages_published": self.messages_published,
            "messages_processed": self.messages_processed,
            "messages_failed": self.messages_failed,
            "handler_names": self.handler_names,
            "is_active": self.is_active,
        }


@dataclass(slots=True)
class BrokerStats:
    """Statistics about the broker's current state"""

    total_topics: int
    total_subscriptions: int
    total_handlers: int
    topics: dict[str, TopicStats] = field(default_factory=dict)
    uptime_seconds: float = 0.0

    def to_dict(self) -> dict:
        """Convert stats to a dictionary for easy serialization"""
        return {
            "total_topics": self.total_topics,
            "total_subscriptions": self.total_subscriptions,
            "total_handlers": self.total_handlers,
            "uptime_seconds": round(self.uptime_seconds, 3),
            "topics": {topic: stats.to_dict() for topic, stats in self.topics.items()},
        }


# observer pattern
BrokerEvents: TypeAlias = Literal[
    "on_publish",
    "on_message_dispatched",
    "on_message_processed",
    "on_message_failed",
    "on_subscribe",
    "on_unsubscribe",
    "on_topic_created",
    "on_dispatch_thread_started",
    "on_dispatch_thread_stopped",
]


class BaseMessageBroker(ABC):
    """Abstract message broker interface"""

    @abstractmethod
    def publish(self, message: Message) -> None:
        """Publish a message to a topic"""
        pass

    @abstractmethod
    def subscribe(
        self,
        topic: str,
        handler: Callable[[Message], None],
        auto_ack: bool = True,
    ) -> str:
        """
        Subscribe to a topic with a consumer group.
        Returns subscription ID.
        """
        pass

    @abstractmethod
    def unsubscribe(self, subscription_id: str) -> None:
        """Unsubscribe from a topic"""
        pass

    @abstractmethod
    def ack(self, message_id: str) -> None:
        """Acknowledge message processing"""
        pass

    @abstractmethod
    def nack(self, message_id: str, requeue_message: Message | None = None) -> None:
        """Negative acknowledgment"""
        pass

    @abstractmethod
    def close(self, timeout: float | None = None) -> None:
        """Close broker connection"""
        pass


class BrokerListener(ABC):
    """
    Observer interface for broker lifecycle events.
    Implement this to track stats, logging, monitoring, etc.
    """

    @abstractmethod
    def on_publish(self, message: Message) -> None:
        """Called when a message is published"""
        pass

    @abstractmethod
    def on_message_dispatched(self, message: Message, topic: str) -> None:
        """Called when a message is retrieved from queue for processing"""
        pass

    @abstractmethod
    def on_message_processed(self, message: Message, topic: str) -> None:
        """Called when a message is successfully processed by all handlers"""
        pass

    @abstractmethod
    def on_message_failed(self, message: Message, topic: str, error: Exception) -> None:
        """Called when at least one handler failed"""
        pass

    @abstractmethod
    def on_subscribe(self, topic: str, handler: Callable, subscription_id: str) -> None:
        """Called when a new subscription is added"""
        pass

    @abstractmethod
    def on_unsubscribe(self, topic: str, subscription_id: str) -> None:
        """Called when a subscription is removed"""
        pass

    @abstractmethod
    def on_topic_created(self, topic: str) -> None:
        """Called when a new topic is created"""
        pass

    @abstractmethod
    def on_dispatch_thread_started(self, topic: str) -> None:
        """Called when a dispatch thread starts for a topic"""
        pass
