import pika


class QueuePublisher:
    def __init__(
        self,
        url: str,
        queue_name: str,
    ):
        self.queue_name = queue_name
        self.connection_params = pika.URLParameters(url + "/" + queue_name)

    def publish(self, message: bytes):
        conn = pika.BlockingConnection(self.connection_params)
        channel = conn.channel()

        channel.queue_declare(queue=self.queue_name, durable=True)  # pyright: ignore[reportUnknownMemberType]
        channel.basic_publish(
            exchange="",
            routing_key=self.queue_name,
            body=message,
        )

        conn.close()
