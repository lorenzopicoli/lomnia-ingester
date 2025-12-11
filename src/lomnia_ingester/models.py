from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, HttpUrl


class Plugin(BaseModel):
    repo: Optional[HttpUrl] = Field(
        default=None, description="Git repository containing the plugin (optional if using local path)"
    )

    path: Optional[Path] = Field(description="Local path to repository containing the plugin")
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
