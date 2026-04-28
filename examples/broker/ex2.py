import time

from tadween_core import set_logger
from tadween_core.broker import InMemoryBroker, Message

set_logger()

broker = InMemoryBroker()


# handlers are wrapped with brackets: []
# topics are wrapped with: <>
#
# submit message
#       ↓
# <topic.init>─┬─> [A1] prints metadata tag. (auto-ack)
#              └─> [A2] chain, publish to topic.A2-B1 (auto-ack)
#                   ↓
#              <topic.A2-B1>
#                   └─> [B1] Does some work, then ack (manual-ack)


def A1(msg: Message):
    tag = msg.metadata.get("tag", "N/A")
    print(f"[A1] Got {tag}")


def A2(msg: Message):
    tag = msg.metadata.get("tag", "N/A")

    print(f"[A2] Got tag: `{tag}`. Publishing to [topic.A2-B1]")

    new_msg = Message(
        topic="topic.A2-B1",
        metadata=msg.metadata,
    )
    broker.publish(new_msg)


# (message_id, tag)
B1_tags = []


def B1(msg: Message):
    tag = msg.metadata.get("tag", "N/A")
    print(f"[B1] Got the tag: `{tag}`. Acknowledging")
    # This sleep runs in a worker thread, not the dispatcher thread.
    time.sleep(1)
    broker.ack(msg.id)


broker.subscribe("topic.init", A1, auto_ack=True)
broker.subscribe("topic.init", A2, auto_ack=True)

broker.subscribe("topic.A2-B1", B1, auto_ack=False)

for i in range(5):
    msg = Message(topic="topic.init", metadata={"tag": str(i)})
    broker.publish(msg)

# Closes the broker. It will wait for pending tasks up to 3 seconds.
broker.close(3)

# If close() is called without a timeout, it will wait indefinitely for all pending tasks to be acknowledged.
# broker.close()
