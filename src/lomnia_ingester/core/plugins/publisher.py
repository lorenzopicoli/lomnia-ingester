import json
import logging
from datetime import datetime
from pathlib import Path

from pydantic.dataclasses import dataclass

from lomnia_ingester.adapters.queue.queue_publisher import QueuePublisher
from lomnia_ingester.adapters.storage.s3_client import S3Storage
from lomnia_ingester.models import PluginOutput

logger = logging.getLogger(__name__)


class FailedToUploadPluginOutput(Exception):
    pass


@dataclass
class PluginFilesUploadResult:
    bucket: str
    key: str
    local_path: Path


class PluginOutputPublisher:
    def __init__(
        self,
        storage: S3Storage,
        publisher: QueuePublisher,
    ):
        self.storage = storage
        self.publisher = publisher

    def upload_output(self, output: PluginOutput):
        canonical_dir = output.canonical
        raw_dir = output.raw
        extracted_at = output.extracted_at

        uploaded: list[PluginFilesUploadResult] = []
        logger.info(
            f"Handling plugin output | plugin_id={output.id} | "
            f"raw_dir={raw_dir} | canonical_dir={canonical_dir} | "
            f"extracted_at={extracted_at.isoformat()}"
        )

        if not canonical_dir.exists():
            logger.error(
                f"Canonical directory not found |\
                    plugin_id={output.id} |\
                    canonical_dir={canonical_dir}"
            )
            raise FailedToUploadPluginOutput("CANONICAL_FOLDER_NOT_FOUND")

        # Upload raw files
        for file in raw_dir.iterdir():
            if not file.is_file():
                continue

            logger.debug(f"Uploading raw file | plugin_id={output.id} | file={file}")

            self._upload(
                folder=f"{output.id}/raw",
                file_path=file,
                extracted_at=extracted_at,
            )

        for file in canonical_dir.iterdir():
            if not file.is_file():
                continue

            logger.debug(
                f"Uploading canonical file | plugin_id={output.id} | file={file}"
            )

            result = self._upload(
                folder=f"{output.id}/canonical",
                file_path=file,
                extracted_at=extracted_at,
            )
            uploaded.append(result)

        logger.info(f"Finished uploading plugin output | plugin_id={output.id}")
        return uploaded

    def publish_file(self, uploaded: PluginFilesUploadResult):
        payload = {
            "bucket": uploaded.bucket,
            "key": uploaded.key,
        }

        logger.info(
            f"Publishing canonical file event |\
                    bucket={uploaded.bucket} |\
                    key={uploaded.key}"
        )

        if not uploaded.local_path.name.endswith(".meta.json"):
            self.publisher.publish(json.dumps(payload).encode())
            logger.info(f"Finished publishing plugin output | dump={json.dumps(payload)}")

    def _upload(
        self,
        folder: str,
        file_path: Path,
        extracted_at: datetime,
    ) -> PluginFilesUploadResult:
        date_path = extracted_at.strftime("%Y/%m/%d")
        key = f"plugins/{folder}/{date_path}/{file_path.name}"

        logger.debug(
            f"Uploading file to storage |\
                    bucket={self.storage.bucket} |\
                    key={key} |\
                    local_path={file_path}"
        )

        self.storage.upload_file(file_path, key)

        return PluginFilesUploadResult(
            bucket=self.storage.bucket, key=key, local_path=file_path
        )
