import tempfile
from pathlib import Path

import streamlit as st

from lomnia_ingester.config import config, publisher
from lomnia_ingester.models import Plugin
from lomnia_ingester.plugin_runner import run_plugin


@st.cache_data
def load_plugins():
    return config.plugins.plugins


plugins = load_plugins()


st.set_page_config(page_title="Plugin Runner", layout="centered")
st.title("Plugin Runner")

uploaded_file = st.file_uploader(
    "Drop a file",
    type=None,
)

plugin_labels: dict[str, Plugin] = {p.id: p for p in plugins}

selected_plugin_name = st.selectbox(
    "Select a plugin",
    options=plugin_labels.keys(),
)

selected_plugin = plugin_labels[selected_plugin_name]

st.caption(selected_plugin.repo or selected_plugin.folder)

run_clicked = st.button("Send", type="primary")


def run_and_publish(plugin: Plugin):
    with run_plugin(plugin) as plugin_output:
        publisher.handle_output(plugin_output)


if run_clicked:
    if not uploaded_file:
        st.error("Please upload a file.")
        st.stop()

    # Save uploaded file to a temp location
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = Path(tmpdir) / uploaded_file.name
        file_path.write_bytes(uploaded_file.getbuffer())

        st.success("File uploaded")

        run_and_publish(selected_plugin)

        # Call your existing logic here
        # Example:
        #
        # from plugin_runner import run_plugin
        # result = run_plugin(
        #     plugin_id=selected_plugin["id"],
        #     file_path=file_path,
        # )

        st.info(f"Would run plugin `{selected_plugin.id}` with file `{uploaded_file.name}`")
