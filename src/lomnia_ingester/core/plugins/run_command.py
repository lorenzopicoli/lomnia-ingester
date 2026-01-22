import logging
import subprocess
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

    try:
        result = subprocess.run(  # noqa: S603
            cmd,
            cwd=cwd,
            env=_normalize_env(env),
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        logger.exception(
            f"Command failed | cmd={cmd} | cwd={cwd if cwd else None} | "
            f"stdout={exc.stdout} | stderr={exc.stderr} | returncode={exc.returncode}"
        )
        raise

    if result.stdout:
        logger.debug(f"Command stdout | stdout={result.stdout}")
    if result.stderr:
        logger.debug(f"Command stderr | stderr={result.stderr}")

    return result
