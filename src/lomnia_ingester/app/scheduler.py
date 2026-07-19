import logging

from lomnia_ingester.adapters.queue.queue_publisher import QueuePublisher
from lomnia_ingester.adapters.storage.plugin_execution_repository import (
    PluginExecutionRepository,
)
from lomnia_ingester.adapters.storage.publishes_repository import PublishesRepository
from lomnia_ingester.adapters.storage.s3_client import S3Storage
from lomnia_ingester.config import load_config
from lomnia_ingester.core.plugins.service import PluginService
from lomnia_ingester.logging import setup_logging

setup_logging(level="DEBUG")

logger = logging.getLogger(__name__)
logger.info("Application starting")

logger.info("Loading config")


def run_scheduler_process():
    config = load_config()
    storage = S3Storage(
        bucket=config.s3.s3_bucket_name,
        endpoint_url=config.s3.s3_url,
        region_name=config.s3.s3_region_name,
        access_key_id=config.s3.s3_access_key_id,
        secret_access_key=config.s3.s3_secret_access_key,
    )

    queue_publisher = QueuePublisher(
        url=config.queue.queue_url,
        queue_name=config.queue.queue_name,
    )

    repo = PluginExecutionRepository(config.store.store_path)
    publish_repo = PublishesRepository(config.store.store_path)

    service = PluginService(
        storage=storage,
        queue_publisher=queue_publisher,
        execution_repo=repo,
        plugins=config.plugins,
        publish_repo=publish_repo,
    )

    service.run_scheduler()
