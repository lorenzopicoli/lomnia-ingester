import logging
import time
from collections.abc import Iterable
from typing import Callable

import schedule

from lomnia_ingester.models import Plugin

logger = logging.getLogger(__name__)

PluginRunner = Callable[[Plugin], None]


class PluginScheduler:
    def __init__(self, plugins: Iterable[Plugin], runner: PluginRunner):
        self._plugins = plugins
        self._runner = runner

    def schedule(self) -> None:
        run_on_startup: list[Plugin] = []

        for plugin in self._plugins:
            if plugin.schedule is None:
                continue

            if plugin.run_on_startup:
                run_on_startup.append(plugin)

            self._schedule_plugin(plugin)

        for plugin in run_on_startup:
            logger.debug(f"Running plugin {plugin.id} immediately (run_on_startup=true)")
            self._runner(plugin)

    def _schedule_plugin(self, plugin: Plugin) -> None:
        schedule_config = plugin.schedule

        if schedule_config is None:
            return

        if schedule_config.interval_minutes:
            logger.info(
                f"Scheduling plugin {plugin.id} every {schedule_config.interval_minutes} minutes"
            )
            schedule.every(schedule_config.interval_minutes).minutes.do(  # pyright: ignore[reportUnknownMemberType]
                self._runner, plugin
            )

        if schedule_config.interval_hours:
            logger.info(
                f"Scheduling plugin {plugin.id} every {schedule_config.interval_hours} hours"
            )
            schedule.every(schedule_config.interval_hours).hours.do(self._runner, plugin)

        if schedule_config.interval_days:
            logger.info(
                f"Scheduling plugin {plugin.id} every {schedule_config.interval_days} days"
            )
            schedule.every(schedule_config.interval_days).days.do(self._runner, plugin)  # pyright: ignore[reportUnknownMemberType]

        if schedule_config.interval_months:
            days = schedule_config.interval_months * 30
            logger.info(
                f"Scheduling plugin {plugin.id} every {days} days (monthly approximation)"
            )
            schedule.every(days).days.do(self._runner, plugin)  # pyright: ignore[reportUnknownMemberType]

    def run_forever(self, tick_seconds: int = 1) -> None:
        logger.info("Starting scheduler loop")
        while True:
            schedule.run_pending()
            time.sleep(tick_seconds)
