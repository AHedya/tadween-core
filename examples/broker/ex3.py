from tadween_core import set_logger
from tadween_core.broker import InMemoryBroker, Message, StatsCollector

set_logger()

broker = InMemoryBroker()
stats_listener = StatsCollector()
broker.add_listener(stats_listener)


# handlers are wrapped with brackets: []
# topics are wrapped with: <>
#
# submit message
#       ↓
# <topic.init>─┬─> [A1] do something. (auto-ack)
#              └─> [A2] publish to two topics. (auto-ack)
#      ┌────────────┴──────┐
#      ↓                   ↓ one topic to many handlers.
# <topic.A2-B3>       <topic.A2-B1,2> ─┬─> [B1] do something, fail, nack. (manual-ack)
#      └─> [B3] (auto-ack)             └─> [B2] do something. (auto-ack)
#           ↓
#       <topic.B3-C1>
#           └─> [C1] do something, fail, nack and requeue. (manual-ack)


def A1(msg: Message):
    pass


def A2(msg: Message):
    tag = msg.metadata.get("tag", "N/A")

    print(f"[A2] Got tag `{tag}`. Publishing to:  <topic.A2-B1,2>, <topic.A2-B3> ")

    broker.publish(msg.fork(topic="topic.A2-B1,2"))
    broker.publish(msg.fork(topic="topic.A2-B3"))


def B1(msg: Message):
    tag = msg.metadata.get("tag", "N/A")

    if "retried" not in tag:
        print(f"[B1] Failed `{tag}`. Retrying...")
        metadata = {"tag": f"retried-{tag}"}
        # Note, republishing here will publish to this handler topic.
        # In other words, the topic of this handler is shared between two handler, so the
        # requeue message will be handled by both the handlers: [B1, B2].
        # Use one-to-one topic-to-handler relationship _if_ you need managed requeueing.
        broker.nack(
            msg.id,
            requeue_message=msg.fork(metadata=metadata),
        )
    else:
        print(f"[B1] We happy `{tag}`")
        broker.ack(msg.id)


def B2(msg: Message):
    tag = msg.metadata.get("tag", "N/A")
    print(f"[B2] Got: `{tag}")


def B3(msg: Message):
    broker.publish(msg.fork(topic="topic.B3-C1"))


def C1(msg: Message):
    tag = msg.metadata.get("tag", "N/A")

    if "retried" not in tag:
        # Same logic as [B1]. But retrying here publishes to one-to-one
        # relationship, so only C1 is affected.
        print("[C1] Failed. Retrying")
        metadata = {"tag": f"retried-{tag}"}
        broker.nack(
            msg.id,
            requeue_message=msg.fork(metadata=metadata),
        )
    else:
        print("[C1] We happy")
        broker.ack(msg.id)


broker.subscribe("topic.init", A1, auto_ack=True)
broker.subscribe("topic.init", A2, auto_ack=True)

broker.subscribe("topic.A2-B1,2", B1, auto_ack=False)
broker.subscribe("topic.A2-B1,2", B2, auto_ack=True)

broker.subscribe("topic.A2-B3", B3, auto_ack=True)

broker.subscribe("topic.B3-C1", C1, auto_ack=False)

for i in range(3):
    msg = Message(topic="topic.init", metadata={"tag": str(i)})
    broker.publish(msg)

broker.close(5)

# stats_listener.print_stats(True)
