import logging
from pathlib import Path

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel
from pydantic.dataclasses import dataclass
from pydantic_settings import BaseSettings

from lomnia_ingester.models import Plugin

logger = logging.getLogger(__name__)


class FailedToLoadConfig(ValueError):
    pass


# The yaml specification
class PluginsConfig(BaseModel):
    plugins: list[Plugin]


class S3Config(BaseSettings):
    s3_bucket_name: str
    s3_url: str
    s3_region_name: str
    s3_access_key_id: str
    s3_secret_access_key: str


class QueueConfig(BaseSettings):
    queue_url: str
    queue_name: str


class StoreConfig(BaseSettings):
    store_path: Path


@dataclass
class Configs:
    s3: S3Config
    queue: QueueConfig
    plugins: list[Plugin]
    store: StoreConfig


def load_plugins_config():
    logger.debug("Loading plugin config yaml file")
    with open("plugins.yaml") as stream:
        try:
            return PluginsConfig.model_validate(yaml.safe_load(stream))
        except yaml.YAMLError:
            logger.exception("Failed to load plugins YAML")
    raise FailedToLoadConfig("INVALID_YAML")


def load_config() -> Configs:
    load_dotenv()
    configs = Configs(
        s3=S3Config.model_validate({}),
        queue=QueueConfig.model_validate({}),
        store=StoreConfig.model_validate({}),
        plugins=load_plugins_config().plugins,
    )
    logger.info("Loaded configs succesfully")
    return configs
