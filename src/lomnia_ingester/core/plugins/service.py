from pathlib import Path

from lomnia_ingester.adapters.queue.queue_publisher import QueuePublisher
from lomnia_ingester.adapters.storage.plugin_execution_repository import (
    PluginExecutionRepository,
)
from lomnia_ingester.adapters.storage.s3_client import S3Storage
from lomnia_ingester.core.plugins.publisher import PluginOutputPublisher
from lomnia_ingester.core.plugins.runner import PluginRunner
from lomnia_ingester.core.plugins.scheduler import PluginScheduler
from lomnia_ingester.models import Plugin


class PluginService:
    publisher: PluginOutputPublisher

    def __init__(
        self,
        storage: S3Storage,
        queue_publisher: QueuePublisher,
        execution_repo: PluginExecutionRepository,
        plugins: list[Plugin],
    ):
        self.storage = storage
        self.plugins = plugins
        self.queue_publisher = queue_publisher
        self.execution_repo = execution_repo
        self.publisher = PluginOutputPublisher(storage=storage, publisher=queue_publisher)

    def run_single(self, plugin: Plugin, in_dir: Path | None = None):
        """
        Run a single plugin. Accepts an optional in_dir which is passed as parameter
        to the plugin. Useful if the directory for the plugin's input is in the server
        (eg. user uploaded a file and now we'd like to run this through the pipeline)
        """
        runner = PluginRunner(plugin=plugin, execution_repo=self.execution_repo)
        with runner.run_plugin(in_dir) as output:
            self.publisher.handle_output(output)

    def run_scheduler(self):
        scheduler = PluginScheduler(plugins=self.plugins, runner=self.run_single)
        scheduler.run_forever()
