from pathlib import Path

from lomnia_ingester.adapters.queue.queue_publisher import QueuePublisher
from lomnia_ingester.adapters.storage.plugin_execution_repository import PluginExecutionRepository
from lomnia_ingester.adapters.storage.s3_client import S3Storage
from lomnia_ingester.models import Plugin


class PluginService:
    def __init__(self, storage: S3Storage, queue_publisher: QueuePublisher, execution_repo: PluginExecutionRepository):
        self.storage = storage
        self.queue_publisher = queue_publisher
        self.execution_repo = execution_repo

    def run_single(self, plugin: Plugin, in_dir: Path):
        print(f"To be implemented {plugin} {in_dir}")
