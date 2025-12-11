import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import boto3
import pika
from dotenv import load_dotenv
from mypy_boto3_s3 import S3Client

load_dotenv()


def garage_buckets():
    s3: S3Client = boto3.client(
        "s3",
        endpoint_url="http://localhost:3900",
    )
    print(s3.list_buckets())


def send_message(message: str, queue_name: str = "test_queue"):
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(
            host="localhost",
            port=5672,
            credentials=pika.PlainCredentials("guest", "guest"),
        )
    )
    channel = conn.channel()
    channel.queue_declare(queue=queue_name, durable=False)

    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=message.encode(),
    )

    print(f"[sender] Sent: {message}")
    conn.close()


def run_plugin(repo_url: str, folder: str):
    tmp = Path(tempfile.mkdtemp())
    plugin_dir = tmp / "plugin"
    work_dir = plugin_dir / folder
    try:
        subprocess.run(["git", "clone", repo_url, str(plugin_dir)], check=True)  # noqa: S603, S607

        subprocess.run(["uv", "sync"], cwd=work_dir, check=True)  # noqa: S607
        env = {
            **os.environ,  # keep existing env vars
            "OWNTRACKS_USER": "owntracks",
            "OWNTRACKS_DEVICE": "shiba",
        }
        result = subprocess.run(
            ["uv", "run", "extract", "--start_date", "1765058675", "--out_dir", ""],  # noqa: S607
            env=env,
            cwd=work_dir,
            check=True,
            capture_output=True,
            text=True,
        )
        print("OUTPUT:", result.stdout)
        print("STDERR:", result.stderr)
    except subprocess.CalledProcessError as e:
        print("----- SUBPROCESS ERROR -----")
        print("COMMAND:", e.cmd)
        print("RETURN CODE:", e.returncode)
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)
        raise
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    # garage_buckets()
    # send_message("Hello world")
    run_plugin("https://github.com/lorenzopicoli/lomnia-plugins.git", folder="owntracks-recorder")
    pass
