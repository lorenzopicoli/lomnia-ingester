import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, TypedDict


class PluginLastRun(TypedDict):
    last_successful_run: str | None
    next_start_date: str | None


class PluginExecutionState(TypedDict):
    plugins: dict[str, PluginLastRun]


class PluginExecutionRepository:
    path: Path
    # { plugins: { <name>: { last_successful_run: ..., next_start_date: ... } } }
    _state: PluginExecutionState

    def __init__(self, path: Path):
        self.path = path
        if path.exists():
            with path.open("r") as f:
                self._state = json.load(f)
        else:
            self._state = {"plugins": {}}

        # In case the file didn't have plugins
        self._state.setdefault("plugins", {})

    def _save(self) -> None:
        tmp_path = self.path.with_suffix(".tmp")

        with tmp_path.open("w") as f:
            json.dump(self._state, f, indent=2, sort_keys=True)

        tmp_path.replace(self.path)

    def _plugin(self, plugin_name: str) -> PluginLastRun:
        plugins = self._state["plugins"]

        plugin = plugins.get(plugin_name)
        if plugin is None:
            plugin = PluginLastRun(last_successful_run=None, next_start_date=None)
            plugins[plugin_name] = plugin

        return self._state["plugins"][plugin_name]

    def _parse_dt(self, value: Optional[str]) -> Optional[datetime]:
        if value is None:
            return None
        return datetime.fromisoformat(value).astimezone(timezone.utc)

    def _format_dt(self, value: Optional[datetime]) -> Optional[str]:
        if value is None:
            return None
        return value.astimezone(timezone.utc).isoformat()

    def get_next_start_date(self, plugin_name: str) -> Optional[datetime]:
        plugin = self._plugin(plugin_name)
        return self._parse_dt(plugin.get("next_start_date"))

    def on_succesfull_run(
        self,
        *,
        plugin_name: str,
        next_start_date: datetime,
    ) -> None:
        plugin = self._plugin(plugin_name)

        plugin["next_start_date"] = self._format_dt(next_start_date)
        plugin["last_successful_run"] = datetime.now(timezone.utc).isoformat()

        self._save()

    def all_plugins(self) -> dict[str, PluginLastRun]:
        return dict(self._state["plugins"])
