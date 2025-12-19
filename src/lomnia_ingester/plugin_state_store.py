import json
from datetime import datetime
from pathlib import Path
from typing import Optional


class PluginStateStore:
    def __init__(self, path: Path):
        self.path = path
        if path.exists():
            with path.open("r") as f:
                self._state = json.load(f)
        else:
            self._state = {"plugins": {}}

        self._state.setdefault("plugins", {})

    def _save(self) -> None:
        tmp_path = self.path.with_suffix(".tmp")

        with tmp_path.open("w") as f:
            json.dump(self._state, f, indent=2, sort_keys=True)

        tmp_path.replace(self.path)

    def _plugin(self, plugin_name: str) -> dict:
        return self._state["plugins"].setdefault(plugin_name, {})

    def _parse_dt(self, value: Optional[str]) -> Optional[datetime]:
        if value is None:
            return None
        return datetime.fromisoformat(value)

    def _format_dt(self, value: Optional[datetime]) -> Optional[str]:
        if value is None:
            return None
        return value.isoformat()

    def get_next_start_date(self, plugin_name: str) -> Optional[datetime]:
        plugin = self._plugin(plugin_name)
        return self._parse_dt(plugin.get("next_start_date"))

    def set_next_start_date(
        self,
        plugin_name: str,
        next_start_date: datetime,
        *,
        last_successful_run: Optional[datetime] = None,
    ) -> None:
        plugin = self._plugin(plugin_name)

        plugin["next_start_date"] = self._format_dt(next_start_date)

        if last_successful_run is not None:
            plugin["last_successful_run"] = self._format_dt(last_successful_run)

        self._save()

    def clear_plugin(self, plugin_name: str) -> None:
        if plugin_name in self._state["plugins"]:
            del self._state["plugins"][plugin_name]
            self._save()

    def all_plugins(self) -> dict:
        return dict(self._state["plugins"])
