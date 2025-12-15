import json
from datetime import datetime
from pathlib import Path

from pydantic.dataclasses import dataclass

from lomnia_ingester.models import FailedToRunPlugin
from lomnia_ingester.plugin_runner import PluginOutput
from lomnia_ingester.queue.publisher import QueuePublisher
from lomnia_ingester.storage.s3_client import S3Storage


@dataclass
class PluginFilesUploadResult:
    bucket: str
    key: str


class PluginOutputPublisher:
    def __init__(
        self,
        storage: S3Storage,
        publisher: QueuePublisher,
    ):
        self.storage = storage
        self.publisher = publisher

    def handle_output(self, output: PluginOutput):
        canonical_dir = output.canonical
        raw_dir = output.raw
        extracted_at = output.extracted_at

        if not canonical_dir.exists():
            raise FailedToRunPlugin("CANONICAL_FOLDER_NOT_FOUND")

        for file in raw_dir.iterdir():
            if file.is_file():
                self.upload(
                    folder=f"{output.id}/raw",
                    file_path=file,
                    extracted_at=extracted_at,
                )

        for file in canonical_dir.iterdir():
            if file.is_file():
                result = self.upload(
                    folder=f"{output.id}/canonical",
                    file_path=file,
                    extracted_at=extracted_at,
                )

                payload = {
                    "bucket": result.bucket,
                    "key": result.key,
                }
                self.publisher.publish(json.dumps(payload).encode())

    def upload(
        self,
        folder: str,
        file_path: Path,
        extracted_at: datetime,
    ) -> PluginFilesUploadResult:
        date_path = extracted_at.strftime("%Y/%m/%d")
        key = f"plugins/{folder}/{date_path}/{file_path.name}"

        self.storage.upload_file(file_path, key)
        return PluginFilesUploadResult(
            bucket=self.storage.bucket,
            key=key,
        )
