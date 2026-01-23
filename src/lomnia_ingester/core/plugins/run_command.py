import logging
import subprocess
import sys
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# Ensure that env values are string
def _normalize_env(env: dict[str, Any] | None) -> dict[str, str]:
    return {} if not env else {k: str(v) for k, v in env.items()}


def run_command(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, Any] | None = None,
    description: str,
):
    logger.info(
        f"Running command | description={description} | cmd={cmd} | cwd={cwd if cwd else None}"
    )
    subprocess.check_call(  # noqa: S603
        cmd,
        env=_normalize_env(env),
        cwd=cwd,
        stdout=sys.stdout,
        stderr=subprocess.STDOUT,
    )
