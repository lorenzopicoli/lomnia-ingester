import json
import logging
import shutil
import subprocess
import tempfile
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from lomnia_ingester.config import store
from lomnia_ingester.models import FailedToRunPlugin, Plugin, PluginOutput

logger = logging.getLogger(__name__)


def run_command(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    env: dict | None = None,
    description: str,
):
    logger.info(f"Running command | description={description} | cmd={cmd} | cwd={cwd if cwd else None}")

    try:
        result = subprocess.run(  # noqa: S603
            cmd,
            cwd=cwd,
            env=env,
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


def run_extract(work_dir: Path, plugin: Plugin, out_dir: Path, start_date: datetime):
    uv = shutil.which("uv")
    if uv is None:
        logger.error("uv executable not found")
        raise FailedToRunPlugin("MISSING_EXECUTABLE_UV")

    logger.info(
        f"Starting extract | plugin_id={plugin.id} | work_dir={work_dir} | out_dir={out_dir} | start_date={start_date.isoformat()}"
    )

    run_command(
        [uv, "sync"],
        cwd=work_dir,
        description="uv sync",
    )

    run_command(
        [
            uv,
            "run",
            "extract",
            "--start_date",
            str(start_date.timestamp()),
            "--out_dir",
            str(out_dir),
        ],
        cwd=work_dir,
        env=plugin.env,
        description="plugin extract",
    )

    logger.info(f"Extract completed | plugin_id={plugin.id}")


def run_transform(work_dir: Path, plugin: Plugin, in_dir: Path, out_dir: Path):
    uv = shutil.which("uv")
    if uv is None:
        logger.error("uv executable not found")
        raise FailedToRunPlugin("MISSING_EXECUTABLE_UV")

    logger.info(
        f"Starting transform | plugin_id={plugin.id} | work_dir={work_dir} | in_dir={in_dir} | out_dir={out_dir}"
    )

    run_command(
        [uv, "sync"],
        cwd=work_dir,
        description="uv sync",
    )

    run_command(
        [
            uv,
            "run",
            "transform",
            "--in_dir",
            str(in_dir),
            "--out_dir",
            str(out_dir),
        ],
        cwd=work_dir,
        env=plugin.env,
        description="plugin transform",
    )

    logger.info(f"Transform completed | plugin_id={plugin.id}")


def clone_plugin(repo_url: str, out_dir: str):
    git = shutil.which("git")
    if git is None:
        logger.error("git executable not found")
        raise FailedToRunPlugin("MISSING_EXECUTABLES")

    logger.info(f"Cloning plugin repository | repo_url={repo_url} | out_dir={out_dir}")

    run_command(
        [git, "clone", repo_url, out_dir],
        description="git clone",
    )


def copy_plugin(path: str, out_dir: str):
    src = Path(path)
    dst = Path(out_dir)

    logger.info(f"Copying plugin from local path | src={src} | dst={dst}")

    if not src.exists():
        logger.error(f"Plugin path does not exist | src={src}")
        raise FailedToRunPlugin("PATH_DOES_NOT_EXIST")

    if dst.exists():
        logger.debug(f"Destination exists, removing | dst={dst}")
        shutil.rmtree(dst)

    shutil.copytree(src, dst)


def get_latest_extract_start(out_dir: Path) -> Optional[datetime]:
    latest: Optional[datetime] = None

    for meta_path in out_dir.rglob("*.meta.json"):
        try:
            with meta_path.open("r", encoding="utf-8") as f:
                data = json.load(f)

            extract_start = data.get("extract_start")
            if not extract_start:
                continue

            extract_start_dt = datetime.fromisoformat(extract_start)

            if latest is None or extract_start_dt > latest:
                latest = extract_start_dt

        except Exception as exc:
            logger.warning(
                "Failed to read extract_start from meta file",
                extra={"path": str(meta_path), "error": str(exc)},
            )

    return latest


@contextmanager
def run_plugin(plugin: Plugin):
    tmp = Path(tempfile.mkdtemp())
    raw_dir = Path(tempfile.mkdtemp())
    canonical_dir = Path(tempfile.mkdtemp())
    work_dir = tmp / plugin.folder if plugin.folder is not None else tmp

    last_week = datetime.now(timezone.utc) - timedelta(days=1)
    extracted_at = datetime.now(timezone.utc)

    start_date = store.get_next_start_date(plugin_name=plugin.id) or last_week
    logger.info(f"Loading next extraction start date | {start_date}")

    logger.info(
        f"Starting plugin run | plugin_id={plugin.id} | tmp={tmp} | raw_dir={raw_dir} | canonical_dir={canonical_dir}"
    )

    try:
        if plugin.repo:
            clone_plugin(str(plugin.repo), str(tmp))
        elif plugin.path:
            copy_plugin(str(plugin.path), out_dir=str(tmp))
        else:
            logger.error(f"Plugin has no repo or path | plugin_id={plugin.id}")
            raise FailedToRunPlugin("MISSING_REPO_OR_PATH")  # noqa: TRY301

        run_extract(
            work_dir,
            plugin=plugin,
            out_dir=raw_dir,
            start_date=start_date,
        )

        latest_extract_date = get_latest_extract_start(raw_dir)

        run_transform(
            work_dir,
            plugin=plugin,
            in_dir=raw_dir,
            out_dir=canonical_dir,
        )

        yield PluginOutput(
            raw=raw_dir,
            canonical=canonical_dir,
            extracted_at=extracted_at,
            id=plugin.id,
        )

        if latest_extract_date is not None:
            logger.info(f"Saving next extraction start date | {latest_extract_date}")
            store.set_next_start_date(
                plugin_name=plugin.id,
                next_start_date=latest_extract_date,
                last_successful_run=datetime.now(timezone.utc),
            )

        logger.info(f"Plugin run completed | plugin_id={plugin.id}")

    except Exception:
        logger.exception(f"Plugin run failed | plugin_id={plugin.id}")
        raise

    finally:
        logger.debug(f"Cleaning up temporary directories | tmp={tmp}")
        shutil.rmtree(tmp, ignore_errors=True)
        shutil.rmtree(raw_dir, ignore_errors=True)
        shutil.rmtree(canonical_dir, ignore_errors=True)
