from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, HttpUrl
from pydantic.dataclasses import dataclass


class PluginSchedule(BaseModel):
    interval_minutes: Optional[int] = Field(None)
    interval_hours: Optional[int] = Field(None)
    interval_days: Optional[int] = Field(None)
    interval_months: Optional[int] = Field(None)


class Plugin(BaseModel):
    repo: Optional[HttpUrl] = Field(
        default=None,
        description="Git repository containing the plugin (optional if using local path)",
    )
    path: Optional[Path] = Field(
        default=None, description="Local path to repository containing the plugin"
    )
    folder: Optional[str] = Field(
        description="Folder inside the repo where the plugin lives"
    )
    env: Optional[dict[str, str | bool]] = Field(
        default=None,
        description="Environment variables to pass to the plugin",
    )
    id: str = Field(
        description="String that uniquely identifies this plugin",
    )
    schedule: Optional[PluginSchedule] = Field(
        None, description="Scheduling information for the plugin"
    )
    run_on_startup: bool = Field(
        default=False, description="Should the plugin run as soon as the program start"
    )
    initial_date: datetime = Field(
        description="The start date passed to plugins on the first run"
    )


@dataclass
class PluginOutput:
    raw: Path
    canonical: Path
    extracted_at: datetime
    id: str
    next_start: datetime
    started_at: datetime
