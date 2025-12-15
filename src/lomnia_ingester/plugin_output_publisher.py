import json
from pathlib import Path

from lomnia_ingester.queue.publisher import QueuePublisher
from lomnia_ingester.storage.s3_client import S3Storage


class PluginOutputPublisher:
    def __init__(
        self,
        storage: S3Storage,
        publisher: QueuePublisher,
    ):
        self.storage = storage
        self.publisher = publisher

    def upload_and_notify(self, file_path: Path) -> str:
        key = f"plugins/{file_path.name}"

        self.storage.upload_file(file_path, key)

        payload = {
            "bucket": self.storage.bucket,
            "key": key,
        }

        self.publisher.publish(json.dumps(payload).encode())

        return key
