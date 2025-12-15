import yaml
from pydantic import BaseModel, Field
from pydantic.dataclasses import dataclass
from pydantic_settings import BaseSettings

from lomnia_ingester.models import FailedToRunPlugin, Plugin


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
