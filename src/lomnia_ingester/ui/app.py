import tempfile
from pathlib import Path

import streamlit as st

from lomnia_ingester.config import config, publisher
from lomnia_ingester.logging import setup_logging
from lomnia_ingester.models import Plugin
from lomnia_ingester.plugin_runner import run_plugin

setup_logging("DEBUG")


@st.cache_data
def load_plugins():
    return config.plugins.plugins


plugins = load_plugins()


st.set_page_config(page_title="Lomnia Plugin Runner", layout="centered")
st.title("Lomnia Plugin Runner", anchor=False)

uploaded_files = st.file_uploader(
    label="Upload the file(s) that should be ingested by the plugin",
    type=None,
    accept_multiple_files=True,
    max_upload_size=2000,
)

plugin_labels: dict[str, Plugin] = {p.id: p for p in plugins}

selected_plugin_name = st.selectbox(
    "Select a plugin",
    options=plugin_labels.keys(),
)

selected_plugin = plugin_labels[selected_plugin_name]

st.caption(selected_plugin.repo or selected_plugin.folder)

run_clicked = st.button("Send", type="primary")


def run_and_publish(plugin: Plugin, in_dir: Path):
    with run_plugin(plugin, in_dir=in_dir) as plugin_output:
        publisher.handle_output(plugin_output)


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

        run_and_publish(selected_plugin, Path(tmpdir))
