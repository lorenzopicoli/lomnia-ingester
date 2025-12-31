from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, HttpUrl
from pydantic.dataclasses import dataclass


class PluginSchedule(BaseModel):
    interval_minutes: Optional[int] = Field(None, description="Run plugin every N minutes")
    interval_hours: Optional[int] = Field(None, description="Run plugin every N hours")
    interval_days: Optional[int] = Field(None, description="Run plugin every N days")
    interval_months: Optional[int] = Field(None, description="Run plugin every N months")


class Plugin(BaseModel):
    repo: Optional[HttpUrl] = Field(
        default=None, description="Git repository containing the plugin (optional if using local path)"
    )
    path: Optional[Path] = Field(default=None, description="Local path to repository containing the plugin")
    folder: Optional[str] = Field(description="Folder inside the repo where the plugin lives")
    env: Optional[dict[str, str]] = Field(
        description="Environment variables to pass to the plugin",
    )
    id: str = Field(
        description="String that uniquely identifies this plugin",
    )
    schedule: PluginSchedule = Field(..., description="Scheduling information for the plugin")
    run_on_startup: bool = Field(default=False, description="Should the plugin run as soon as the program start")


@dataclass
class PluginOutput:
    raw: Path
    canonical: Path
    extracted_at: datetime
    id: str


class FailedToRunPlugin(ValueError):
    def __init__(self, value):
        super().__init__(value)


class FailedToLoadPlugin(ValueError):
    def __init__(self, value):
        super().__init__(value)
