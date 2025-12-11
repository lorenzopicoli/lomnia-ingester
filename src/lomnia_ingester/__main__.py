import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pika
import yaml
from dotenv import load_dotenv
from mypy_boto3_s3 import S3Client

from lomnia_ingester.models import FailedToRunPlugin, PluginsConfig
from lomnia_ingester.plugin_runner import run_plugin

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


def load_config():
    with open("plugins.yaml") as stream:
        try:
            config = yaml.safe_load(stream)
            return PluginsConfig(**config)
        except yaml.YAMLError as exc:
            print(exc)


if __name__ == "__main__":
    # garage_buckets()
    # send_message("Hello world")
    config = load_config()
    if config is None:
        raise FailedToRunPlugin("MISSING_PLUGINS")

    for plugin in config.plugins:
        tmp = Path(tempfile.mkdtemp())
        raw_dir = Path(tempfile.mkdtemp())
        canonical_dir = Path(tempfile.mkdtemp())
        work_dir = tmp / plugin.folder if plugin.folder is not None else tmp

        last_week = datetime.now(timezone.utc) - timedelta(days=7)
        try:
            run_plugin(plugin)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
            shutil.rmtree(raw_dir, ignore_errors=True)
            shutil.rmtree(canonical_dir, ignore_errors=True)
