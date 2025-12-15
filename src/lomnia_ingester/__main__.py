from pathlib import Path

from dotenv import load_dotenv

from lomnia_ingester.config import load_config
from lomnia_ingester.models import FailedToRunPlugin
from lomnia_ingester.plugin_output_publisher import PluginOutputPublisher
from lomnia_ingester.plugin_runner import run_plugin
from lomnia_ingester.queue.publisher import QueuePublisher
from lomnia_ingester.storage.s3_client import S3Storage

load_dotenv()


config = load_config()

storage = S3Storage(
    bucket=config.s3.s3_bucket_name,
    endpoint_url=config.s3.s3_url,
    region_name=config.s3.s3_region_name,
    access_key_id=config.s3.s3_access_key_id,
    secret_access_key=config.s3.s3_secret_access_key,
)

queuePublisher = QueuePublisher(
    host=config.queue.queue_host,
    port=config.queue.queue_port,
    username=config.queue.queue_username,
    password=config.queue.queue_password,
    queue_name=config.queue.queue_name,
)

publisher = PluginOutputPublisher(storage, queuePublisher)


def process_plugin_outputs(canonical_dir: Path):
    if not canonical_dir.exists():
        raise FailedToRunPlugin("CANONICAL_FOLDER_NOT_FOUND")

    for file in canonical_dir.iterdir():
        if file.is_file():
            publisher.upload_and_notify(file)


if __name__ == "__main__":
    for plugin in config.plugins.plugins:
        with run_plugin(plugin) as plugin_output:
            process_plugin_outputs(plugin_output.canonical)
