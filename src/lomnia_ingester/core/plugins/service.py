import logging
from pathlib import Path

from lomnia_ingester.adapters.queue.queue_publisher import QueuePublisher
from lomnia_ingester.adapters.storage.plugin_execution_repository import (
    PluginExecutionRepository,
)
from lomnia_ingester.adapters.storage.publishes_repository import PublishesRepository
from lomnia_ingester.adapters.storage.s3_client import S3Storage
from lomnia_ingester.core.plugins.publisher import PluginOutputPublisher
from lomnia_ingester.core.plugins.runner import PluginRunner
from lomnia_ingester.core.plugins.scheduler import PluginScheduler
from lomnia_ingester.models import Plugin

logger = logging.getLogger(__name__)


class PluginService:
    publisher: PluginOutputPublisher

    def __init__(
        self,
        storage: S3Storage,
        queue_publisher: QueuePublisher,
        execution_repo: PluginExecutionRepository,
        publish_repo: PublishesRepository,
        plugins: list[Plugin],
    ):
        self.storage = storage
        self.plugins = plugins
        self.queue_publisher = queue_publisher
        self.execution_repo = execution_repo
        self.publisher = PluginOutputPublisher(storage=storage, publisher=queue_publisher)
        self.publish_repo = publish_repo

    def run_single(self, plugin: Plugin, in_dir: Path | None = None):
        """
        Run a single plugin. Accepts an optional in_dir which is passed as parameter
        to the plugin. Useful if the directory for the plugin's input is in the server
        (eg. user uploaded a file and now we'd like to run this through the pipeline)
        """
        logger.info(f"Running single plugin {plugin} | {in_dir}")
        runner = PluginRunner(plugin=plugin, execution_repo=self.execution_repo)
        with runner.run_plugin(in_dir) as output:
            logger.info("Uploading plugin results")
            uploaded = self.publisher.upload_output(output)
            for upload in uploaded:
                try:
                    self.publisher.publish_file(upload)
                    self.publish_repo.on_succesfull_publish(
                        plugin_id=plugin.id, bucket=upload.bucket, key=upload.key
                    )
                except Exception:
                    self.publish_repo.on_fail_publish(
                        plugin_id=plugin.id, bucket=upload.bucket, key=upload.key
                    )
                    logger.exception(f"Failed to publish {upload.bucket} - {upload.key}")

            logger.info(f"Saving next extraction start date | {output.next_start}")
            self.execution_repo.on_succesfull_run(
                plugin_name=output.id,
                next_start_date=output.next_start,
                started_at=output.started_at,
            )

    def run_scheduler(self):
        logger.info("Starting scheduler")
        scheduler = PluginScheduler(plugins=self.plugins, runner=self.run_single)
        scheduler.schedule()
        scheduler.run_forever()
