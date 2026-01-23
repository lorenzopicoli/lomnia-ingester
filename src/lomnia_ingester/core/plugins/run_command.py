import logging
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable

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
    output_callback: Callable[[str], None] | None = None,
):
    logger.info(
        f"Running command | description={description} | cmd={cmd} | cwd={cwd if cwd else None}"
    )

    # Force unbuffered output for Python subprocesses
    normalized_env = _normalize_env(env)
    normalized_env["PYTHONUNBUFFERED"] = "1"

    process = subprocess.Popen(  # noqa: S603
        cmd,
        env=normalized_env,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    if process.stdout:
        for line in iter(process.stdout.readline, b""):
            decoded_line = line.decode("utf-8", errors="replace").rstrip("\n\r")

            if output_callback:
                output_callback(decoded_line)

            sys.stdout.write(decoded_line + "\n")
            sys.stdout.flush()
    else:
        logger.error("Failed to get process stdout")

    returncode = process.wait()

    if returncode != 0:
        raise subprocess.CalledProcessError(returncode, cmd)
