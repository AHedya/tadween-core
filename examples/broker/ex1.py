from tadween_core.broker import InMemoryBroker, Message

broker = InMemoryBroker()


# Subscribe to a topic
def my_handler(msg: Message):
    print(f"Received message: {msg.payload}")


broker.subscribe("audio.downloaded", my_handler)

# Publish a message
msg = Message(topic="audio.downloaded", payload={"local_path": "/tmp/audio.mp3"})
broker.publish(msg)

# Graceful shutdown
broker.close(timeout=5.0)
