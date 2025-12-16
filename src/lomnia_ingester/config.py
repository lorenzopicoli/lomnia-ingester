import logging

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from pydantic.dataclasses import dataclass
from pydantic_settings import BaseSettings

from lomnia_ingester.models import FailedToRunPlugin, Plugin
from lomnia_ingester.plugin_output_publisher import PluginOutputPublisher
from lomnia_ingester.queue.publisher import QueuePublisher
from lomnia_ingester.storage.s3_client import S3Storage

load_dotenv()
logger = logging.getLogger(__name__)


class PluginsConfig(BaseModel):
    plugins: list[Plugin]


class S3Config(BaseSettings):
    s3_bucket_name: str = Field(default=...)
    s3_url: str = Field(default=...)
    s3_region_name: str = Field(default=...)
    s3_access_key_id: str = Field(default=...)
    s3_secret_access_key: str = Field(default=...)


class QueueConfig(BaseSettings):
    queue_host: str = Field(default=...)
    queue_port: int = Field(default=...)
    queue_username: str = Field(default=...)
    queue_password: str = Field(default=...)
    queue_name: str = Field(default=...)


@dataclass
class Configs:
    s3: S3Config
    queue: QueueConfig
    plugins: PluginsConfig


def load_plugins_config():
    with open("plugins.yaml") as stream:
        try:
            config = yaml.safe_load(stream)
            plugins = PluginsConfig(**config)
            if plugins is None:
                raise FailedToRunPlugin("MISSING_PLUGINS_CONFIG")
            else:
                return plugins
        except yaml.YAMLError as exc:
            print(exc)
    raise FailedToRunPlugin("MISSING_PLUGINS_CONFIG")


def load_config() -> Configs:
    try:
        s3_config = S3Config()

        queue_config = QueueConfig()
        plugins_config = load_plugins_config()
    except Exception as exc:
        raise FailedToRunPlugin(str(exc))  # noqa: B904

    return Configs(
        s3=s3_config,
        queue=queue_config,
        plugins=plugins_config,
    )


config = load_config()
logger.info("Loading S3 config")
storage = S3Storage(
    bucket=config.s3.s3_bucket_name,
    endpoint_url=config.s3.s3_url,
    region_name=config.s3.s3_region_name,
    access_key_id=config.s3.s3_access_key_id,
    secret_access_key=config.s3.s3_secret_access_key,
)

logger.info("Loading queue config")
queuePublisher = QueuePublisher(
    host=config.queue.queue_host,
    port=config.queue.queue_port,
    username=config.queue.queue_username,
    password=config.queue.queue_password,
    queue_name=config.queue.queue_name,
)

publisher = PluginOutputPublisher(storage, queuePublisher)
