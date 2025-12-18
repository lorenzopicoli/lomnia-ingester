import logging

from lomnia_ingester.logging import setup_logging
from lomnia_ingester.plugin_scheduler import schedule_and_wait

setup_logging(level="DEBUG")

logger = logging.getLogger(__name__)
logger.info("Application starting")

logger.info("Loading config")


if __name__ == "__main__":
    schedule_and_wait()
