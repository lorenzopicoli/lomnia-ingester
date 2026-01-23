import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path

import streamlit as st

from lomnia_ingester.adapters.queue.queue_publisher import QueuePublisher
from lomnia_ingester.adapters.storage.plugin_execution_repository import (
    PluginExecutionRepository,
)
from lomnia_ingester.adapters.storage.s3_client import S3Storage
from lomnia_ingester.config import Configs, load_config
from lomnia_ingester.core.plugins.service import PluginService
from lomnia_ingester.logging import setup_logging
from lomnia_ingester.models import Plugin

setup_logging(level="DEBUG")

logger = logging.getLogger(__name__)
logger.info("Application starting")


@dataclass
class Application:
    config: Configs
    service: PluginService


def bootstrap():
    setup_logging("DEBUG")
    config = load_config()
    storage = S3Storage(
        bucket=config.s3.s3_bucket_name,
        endpoint_url=config.s3.s3_url,
        region_name=config.s3.s3_region_name,
        access_key_id=config.s3.s3_access_key_id,
        secret_access_key=config.s3.s3_secret_access_key,
    )

    queue_publisher = QueuePublisher(
        host=config.queue.queue_host,
        port=config.queue.queue_port,
        username=config.queue.queue_username,
        password=config.queue.queue_password,
        queue_name=config.queue.queue_name,
    )

    repo = PluginExecutionRepository(config.store.store_path)

    service = PluginService(
        storage=storage,
        queue_publisher=queue_publisher,
        execution_repo=repo,
        plugins=config.plugins,
    )
    app = Application(config=config, service=service)
    logger.info("Application bootstrapped succesfully")
    return app


application = bootstrap()

st.set_page_config(page_title="Lomnia Plugin Runner", layout="centered")
st.title("Lomnia Plugin Runner", anchor=False)

uploaded_files = st.file_uploader(
    label="Upload the file(s) that should be ingested by the plugin",
    type=None,
    accept_multiple_files=True,
    max_upload_size=2000,
)

plugin_labels: dict[str, Plugin] = {p.id: p for p in application.config.plugins}

selected_plugin_name = st.selectbox(
    "Select a plugin",
    options=plugin_labels.keys(),
)

selected_plugin = plugin_labels[selected_plugin_name]

st.caption(selected_plugin.repo or selected_plugin.folder)

run_clicked = st.button("Send", type="primary")


if run_clicked:
    if not uploaded_files:
        st.error("Please upload a file.")
        st.stop()

    # Save uploaded file to a temp location
    with tempfile.TemporaryDirectory() as tmpdir:
        for uploaded_file in uploaded_files:
            file_path = Path(tmpdir) / uploaded_file.name
            file_path.write_bytes(uploaded_file.getbuffer())

        st.success("File uploaded")

        application.service.run_single(selected_plugin, in_dir=Path(tmpdir))
