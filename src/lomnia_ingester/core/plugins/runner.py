import json
import logging
import shutil
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from lomnia_ingester.adapters.storage.plugin_execution_repository import (
    PluginExecutionRepository,
)
from lomnia_ingester.core.plugins.run_command import run_command
from lomnia_ingester.models import Plugin, PluginOutput

logger = logging.getLogger(__name__)


class FailedToRunPlugin(Exception):
    pass


class PluginRunner:
    def __init__(self, plugin: Plugin, execution_repo: PluginExecutionRepository):
        self.plugin = plugin
        self.execution_repo = execution_repo

    def _run_extract(
        self,
        work_dir: Path,
        in_dir: Path | None,
        out_dir: Path,
        start_date: datetime,
    ):
        uv = shutil.which("uv")
        if uv is None:
            logger.error("uv executable not found")
            raise FailedToRunPlugin("MISSING_EXECUTABLE_UV")

        logger.info(
            f"Starting extract | plugin_id={self.plugin.id} | work_dir={work_dir} | out_dir={out_dir} | start_date={start_date.isoformat()}"
        )

        run_command(
            [uv, "sync"],
            cwd=work_dir,
            description="uv sync",
        )

        extract_command = [
            uv,
            "run",
            "extract",
            "--out_dir",
            str(out_dir),
        ]

        if start_date:
            extract_command = [
                *extract_command,
                "--start_date",
                str(start_date.timestamp()),
            ]

        if in_dir:
            extract_command = [*extract_command, "--in_dir", str(in_dir)]

        run_command(
            extract_command,
            cwd=work_dir,
            env=self.plugin.env,
            description="plugin extract",
        )

        logger.info(f"Extract completed | plugin_id={self.plugin.id}")

    def _run_transform(self, work_dir: Path, in_dir: Path, out_dir: Path):
        uv = shutil.which("uv")
        if uv is None:
            logger.error("uv executable not found")
            raise FailedToRunPlugin("MISSING_EXECUTABLE_UV")

        logger.info(
            f"Starting transform | plugin_id={self.plugin.id} | work_dir={work_dir} | in_dir={in_dir} | out_dir={out_dir}"
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
            env=self.plugin.env,
            description="plugin transform",
        )

        logger.info(f"Transform completed | plugin_id={self.plugin.id}")

    def _prepare_work_dir(self, work_dir: str):
        if self.plugin.repo:
            git = shutil.which("git")
            if git is None:
                logger.error("git executable not found")
                raise FailedToRunPlugin("MISSING_EXECUTABLES")

            logger.info(
                f"Cloning plugin repository | repo_url={self.plugin.repo} | out_dir={work_dir}"
            )

            run_command(
                [git, "clone", str(self.plugin.repo), work_dir],
                description="git clone",
            )
        elif self.plugin.path:
            src = self.plugin.path
            dst = Path(work_dir)

            logger.info(f"Copying plugin from local path | src={src} | dst={dst}")

            if not src.exists():
                logger.error(f"Plugin path does not exist | src={src}")
                raise FailedToRunPlugin("PATH_DOES_NOT_EXIST")

            if dst.exists():
                logger.debug(f"Destination exists, removing | dst={dst}")
                shutil.rmtree(dst)

            shutil.copytree(src, dst)
        else:
            logger.error(f"Plugin has no repo or path | plugin_id={self.plugin.id}")
            raise FailedToRunPlugin("MISSING_REPO_OR_PATH")

    def _get_latest_date_extracted(self, canonical_out_dir: Path) -> Optional[datetime]:
        latest: Optional[datetime] = None

        for meta_path in canonical_out_dir.rglob("*.meta.json"):
            try:
                with meta_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)

                data_window_end = data.get("window_end")
                if not data_window_end:
                    continue

                data_window_end_dt = datetime.fromisoformat(data_window_end)

                if latest is None or data_window_end_dt > latest:
                    latest = data_window_end_dt

            except Exception as exc:
                logger.warning(
                    "Failed to read data_window_end from meta file",
                    extra={"path": str(meta_path), "error": str(exc)},
                )

        return latest

    @contextmanager
    def run_plugin(self, in_dir: Path | None):
        tmp = Path(tempfile.mkdtemp())
        raw_dir = Path(tempfile.mkdtemp())
        canonical_dir = Path(tempfile.mkdtemp())
        work_dir = tmp / self.plugin.folder if self.plugin.folder is not None else tmp

        extracted_at = datetime.now(timezone.utc)
        started_at = datetime.now(timezone.utc)
        start_date = self.execution_repo.get_next_start_date(plugin_id=self.plugin.id)
        logger.info(f"Loading next extraction start date | {start_date}")

        logger.info(
            f"Starting plugin run |\
                    plugin_id={self.plugin.id} |\
                    tmp={tmp} | raw_dir={raw_dir} |\
                    canonical_dir={canonical_dir}"
        )

        try:
            self._prepare_work_dir(str(tmp))

            self._run_extract(
                work_dir,
                out_dir=raw_dir,
                start_date=start_date or self.plugin.initial_date,
                in_dir=in_dir,
            )

            self._run_transform(
                work_dir,
                in_dir=raw_dir,
                out_dir=canonical_dir,
            )

            latest_extract_date = self._get_latest_date_extracted(canonical_dir)

            if latest_extract_date is None:
                raise

            yield PluginOutput(
                raw=raw_dir,
                canonical=canonical_dir,
                extracted_at=extracted_at,
                id=self.plugin.id,
                next_start=latest_extract_date,
                started_at=started_at,
            )

            logger.info(f"Plugin run completed | plugin_id={self.plugin.id}")

        except Exception:
            logger.exception(f"Plugin run failed | plugin_id={self.plugin.id}")
            raise

        finally:
            logger.debug(f"Cleaning up temporary directories | tmp={tmp}")
            shutil.rmtree(tmp, ignore_errors=True)
            shutil.rmtree(raw_dir, ignore_errors=True)
            shutil.rmtree(canonical_dir, ignore_errors=True)
