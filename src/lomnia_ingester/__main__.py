import argparse
import subprocess
import sys
from pathlib import Path

from lomnia_ingester.app import scheduler

ROOT = Path(__file__).resolve().parent


def main() -> None:
    parser = argparse.ArgumentParser("lomnia-ingester")
    parser.add_argument(
        "mode",
        choices=["scheduler", "dashboard"],
        help="Which system to run",
    )

    args = parser.parse_args()

    if args.mode == "scheduler":
        scheduler.run_scheduler_process()

    elif args.mode == "dashboard":
        subprocess.run(  # noqa: S603
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                str(ROOT / "app" / "dashboard.py"),
            ],
            check=True,
        )


if __name__ == "__main__":
    main()
