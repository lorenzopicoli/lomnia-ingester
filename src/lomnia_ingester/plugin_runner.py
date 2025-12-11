import shutil
import subprocess
import tempfile
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pydantic.dataclasses import dataclass

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


def copy_plugin(path: str, out_dir: str):
    src = Path(path)
    dst = Path(out_dir)

    if not src.exists():
        raise FailedToRunPlugin("PATH_DOES_NOT_EXIST")

    if dst.exists():
        shutil.rmtree(dst)

    shutil.copytree(src, dst)


@dataclass
class PluginOutput:
    raw: Path
    canonical: Path


@contextmanager
def run_plugin(plugin: Plugin):
    tmp = Path(tempfile.mkdtemp())
    raw_dir = Path(tempfile.mkdtemp())
    canonical_dir = Path(tempfile.mkdtemp())
    work_dir = tmp / plugin.folder if plugin.folder is not None else tmp

    last_week = datetime.now(timezone.utc) - timedelta(days=1)
    try:
        if plugin.repo:
            clone_plugin(str(plugin.repo), str(tmp))
        elif plugin.path:
            copy_plugin(str(plugin.path), out_dir=str(tmp))
        else:
            raise FailedToRunPlugin("MISSING_REPO_OR_PATH")
        print("Extracting...")
        run_extract(work_dir, plugin=plugin, out_dir=raw_dir, start_date=last_week)
        print("Transforming...")
        run_transform(work_dir, plugin=plugin, in_dir=raw_dir, out_dir=canonical_dir)
        print("TMP FOLDER", tmp)
        print("RAW FOLDER", raw_dir)
        print("CANONICAL FOLDER", canonical_dir)
        yield PluginOutput(raw=raw_dir, canonical=canonical_dir)
    finally:
        print("FINALLY")
        shutil.rmtree(tmp, ignore_errors=True)
        shutil.rmtree(raw_dir, ignore_errors=True)
        shutil.rmtree(canonical_dir, ignore_errors=True)
