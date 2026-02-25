import time

from tadween_core.broker import InMemoryBroker, Message


def main():
    # 1. Initialize the broker
    broker = InMemoryBroker()

    # 2. Define a simple handler for a topic
    def on_audio_downloaded(msg: Message):
        print(f"[Consumer] Received message on topic '{msg.topic}':")
        print(f"           Payload: {msg.payload}")

    # 3. Subscribe to the topic
    broker.subscribe("audio.downloaded", on_audio_downloaded)

    # 4. Publish a message
    print("[Producer] Publishing a message to 'audio.downloaded'...")
    msg = Message(
        topic="audio.downloaded",
        payload={
            "url": "https://example.com/audio.mp3",
            "local_path": "/tmp/audio.mp3",
        },
    )
    broker.publish(msg)

    # 5. Wait a bit for the message to be processed
    time.sleep(0.5)

    # 6. Graceful shutdown
    print("[System] Closing broker...")
    broker.close()


if __name__ == "__main__":
    main()
