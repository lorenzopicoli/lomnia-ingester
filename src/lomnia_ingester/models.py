from typing import Optional

from pydantic import BaseModel, Field, HttpUrl


class Plugin(BaseModel):
    repo: HttpUrl = Field(default=..., description="Git repository containing the plugin")
    folder: Optional[str] = Field(description="Folder inside the repo where the plugin lives")
    env: Optional[dict[str, str]] = Field(
        description="Environment variables to pass to the plugin",
    )


class PluginsConfig(BaseModel):
    plugins: list[Plugin]


class FailedToRunPlugin(ValueError):
    def __init__(self, value):
        super().__init__(value)


class FailedToLoadPlugin(ValueError):
    def __init__(self, value):
        super().__init__(value)
