import boto3
import pika
from dotenv import load_dotenv
from mypy_boto3_s3 import S3Client

load_dotenv()


def garage_buckets():
    s3: S3Client = boto3.client(
        "s3",
        endpoint_url="http://localhost:3900",
    )
    print(s3.list_buckets())


def send_message(message: str, queue_name: str = "test_queue"):
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(
            host="localhost",
            port=5672,
            credentials=pika.PlainCredentials("guest", "guest"),
        )
    )
    channel = conn.channel()
    channel.queue_declare(queue=queue_name, durable=False)

    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=message.encode(),
    )

    print(f"[sender] Sent: {message}")
    conn.close()


if __name__ == "__main__":
    garage_buckets()
    send_message("Hello world")
    pass
