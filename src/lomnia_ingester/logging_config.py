import logging
import logging.config

from rich.logging import RichHandler


def setup_logging(level="INFO"):
    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {
            "console": {
                "()": RichHandler,
                "level": level,
                "rich_tracebacks": True,
                "show_time": True,
                "show_level": True,
                "show_path": True,
            },
        },
        "root": {
            "handlers": ["console"],
            "level": "WARNING",
        },
        "loggers": {
            "lomnia_ingester": {
                "level": level,
                "handlers": ["console"],
                "propagate": False,
            }
        },
    })
