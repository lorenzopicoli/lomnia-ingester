import pika


class QueuePublisher:
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        queue_name: str,
    ):
        self.queue_name = queue_name
        self.connection_params = pika.ConnectionParameters(
            host=host,
            port=port,
            credentials=pika.PlainCredentials(username, password),
        )

    def publish(self, message: bytes):
        conn = pika.BlockingConnection(self.connection_params)
        channel = conn.channel()

        channel.queue_declare(queue=self.queue_name, durable=True)
        channel.basic_publish(
            exchange="",
            routing_key=self.queue_name,
            body=message,
        )

        conn.close()
