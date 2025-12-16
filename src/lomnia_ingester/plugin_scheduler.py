import logging
import time

import schedule

from lomnia_ingester.config import config, publisher
from lomnia_ingester.models import Plugin
from lomnia_ingester.plugin_runner import run_plugin

logger = logging.getLogger(__name__)


def run_and_publish(plugin: Plugin):
    with run_plugin(plugin) as plugin_output:
        publisher.handle_output(plugin_output)


def schedule_plugins():
    run_on_startup = []
    for plugin in config.plugins.plugins:
        if plugin.run_on_startup:
            run_on_startup.append(plugin)
        if plugin.schedule.interval_minutes:
            logger.info(f"Scheduling plugin {plugin.id} every {plugin.schedule.interval_minutes} minutes")
            schedule.every(plugin.schedule.interval_minutes).minutes.do(run_and_publish, plugin)
        if plugin.schedule.interval_hours:
            logger.info(f"Scheduling plugin {plugin.id} every {plugin.schedule.interval_hours} hours")
            schedule.every(plugin.schedule.interval_hours).hours.do(run_and_publish, plugin)
        if plugin.schedule.interval_days:
            logger.info(f"Scheduling plugin {plugin.id} every {plugin.schedule.interval_days} days")
            schedule.every(plugin.schedule.interval_days).days.do(run_and_publish, plugin)
        if plugin.schedule.interval_months:
            logger.info(f"Scheduling plugin {plugin.id} every {plugin.schedule.interval_months * 30} days")
            schedule.every(plugin.schedule.interval_months * 30).days.do(run_and_publish, plugin)
    for plugin in run_on_startup:
        logger.debug(f"Running plugin {plugin.id} immediately since it has run_on_startup set to true")
        run_and_publish(plugin)


def schedule_and_wait():
    logger.info("Scheduling plugins")
    schedule_plugins()
    while True:
        schedule.run_pending()
        time.sleep(1)
