import shutil
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from lomnia_ingester.models import FailedToRunPlugin, Plugin


def run_extract(work_dir: Path, plugin: Plugin, out_dir: Path, start_date: datetime):
    uv = shutil.which("uv")

    if uv is None:
        raise FailedToRunPlugin("MISSING_EXECUTABLE_UV")

    result = subprocess.run([uv, "sync"], cwd=work_dir, check=True)  # noqa: S603
    result = subprocess.run(  # noqa: S603
        [uv, "run", "extract", "--start_date", str(start_date.timestamp()), "--out_dir", str(out_dir)],
        env=plugin.env,
        cwd=work_dir,
        check=True,
        capture_output=True,
        text=True,
    )
    print("OUTPUT:", result.stdout)
    print("STDERR:", result.stderr)
    return result.check_returncode()


def run_transform(work_dir: Path, plugin: Plugin, in_dir: Path, out_dir: Path):
    uv = shutil.which("uv")

    if uv is None:
        raise FailedToRunPlugin("MISSING_EXECUTABLE_UV")

    result = subprocess.run([uv, "sync"], cwd=work_dir, check=True)  # noqa: S603
    result.check_returncode()
    result = subprocess.run(  # noqa: S603
        [uv, "run", "transform", "--in_dir", str(in_dir), "--out_dir", str(out_dir)],
        env=plugin.env,
        cwd=work_dir,
        check=True,
        capture_output=True,
        text=True,
    )
    print("OUTPUT:", result.stdout)
    print("STDERR:", result.stderr)

    return result.check_returncode()


def clone_plugin(repo_url: str, out_dir: str):
    git = shutil.which("git")

    if git is None:
        raise FailedToRunPlugin("MISSING_EXECUTABLES")
    result = subprocess.run([git, "clone", repo_url, out_dir], check=True)  # noqa: S603

    return result.check_returncode()


def run_plugin(plugin: Plugin):
    tmp = Path(tempfile.mkdtemp())
    raw_dir = Path(tempfile.mkdtemp())
    canonical_dir = Path(tempfile.mkdtemp())
    work_dir = tmp / plugin.folder if plugin.folder is not None else tmp

    last_week = datetime.now(timezone.utc) - timedelta(days=7)
    try:
        clone_plugin(str(plugin.repo), str(tmp))
        run_extract(work_dir, plugin=plugin, out_dir=raw_dir, start_date=last_week)
        run_transform(work_dir, plugin=plugin, in_dir=raw_dir, out_dir=canonical_dir)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        shutil.rmtree(raw_dir, ignore_errors=True)
        shutil.rmtree(canonical_dir, ignore_errors=True)
