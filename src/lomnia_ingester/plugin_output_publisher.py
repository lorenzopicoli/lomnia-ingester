import json
import logging
from datetime import datetime
from pathlib import Path

from pydantic.dataclasses import dataclass

from lomnia_ingester.models import FailedToRunPlugin, PluginOutput
from lomnia_ingester.queue.publisher import QueuePublisher
from lomnia_ingester.storage.s3_client import S3Storage

logger = logging.getLogger(__name__)


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

        logger.info(
            f"Handling plugin output | plugin_id={output.id} | "
            f"raw_dir={raw_dir} | canonical_dir={canonical_dir} | "
            f"extracted_at={extracted_at.isoformat()}"
        )

        if not canonical_dir.exists():
            logger.error(f"Canonical directory not found | plugin_id={output.id} | canonical_dir={canonical_dir}")
            raise FailedToRunPlugin("CANONICAL_FOLDER_NOT_FOUND")

        # Upload raw files
        for file in raw_dir.iterdir():
            if not file.is_file():
                continue

            logger.debug(f"Uploading raw file | plugin_id={output.id} | file={file}")

            self.upload(
                folder=f"{output.id}/raw",
                file_path=file,
                extracted_at=extracted_at,
            )

        for file in canonical_dir.iterdir():
            if not file.is_file() or file.suffix == ".meta.json":
                continue

            logger.debug(f"Uploading canonical file | plugin_id={output.id} | file={file}")

            result = self.upload(
                folder=f"{output.id}/canonical",
                file_path=file,
                extracted_at=extracted_at,
            )

            payload = {
                "bucket": result.bucket,
                "key": result.key,
            }

            logger.info(
                f"Publishing canonical file event | plugin_id={output.id} | bucket={result.bucket} | key={result.key}"
            )

            self.publisher.publish(json.dumps(payload).encode())

        logger.info(f"Finished handling plugin output | plugin_id={output.id}")

    def upload(
        self,
        folder: str,
        file_path: Path,
        extracted_at: datetime,
    ) -> PluginFilesUploadResult:
        date_path = extracted_at.strftime("%Y/%m/%d")
        key = f"plugins/{folder}/{date_path}/{file_path.name}"

        logger.debug(f"Uploading file to storage | bucket={self.storage.bucket} | key={key} | local_path={file_path}")

        self.storage.upload_file(file_path, key)

        return PluginFilesUploadResult(
            bucket=self.storage.bucket,
            key=key,
        )
