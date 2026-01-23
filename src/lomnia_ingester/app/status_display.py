from enum import Enum
from typing import Optional

import streamlit as st


class PluginExecutionStatus(Enum):
    IDLE = "idle"
    CLONING = "cloning"
    SYNCING = "syncing"
    EXTRACTING = "extracting"
    TRANSFORMING = "transforming"
    COMPLETED = "completed"
    FAILED = "failed"


class PluginStatusDisplay:
    """
    Manages real-time status display for plugin execution in Streamlit.
    """

    def __init__(self, plugin_id: str):
        self.plugin_id = plugin_id
        self.status = PluginExecutionStatus.IDLE

        # Create display containers
        self.status_container = st.empty()
        self.progress_container = st.empty()
        self.output_container = st.empty()

        # Initialize display
        self._update_display()

    def set_status(self, status: PluginExecutionStatus, message: Optional[str] = None):
        """Update the execution status and refresh display."""
        self.status = status
        self._update_display(message)

    def append_output(self, line: str):
        """Append a line to the output display."""
        # This will be implemented when we integrate with the dashboard
        pass

    def _update_display(self, message: Optional[str] = None):
        """Update the status display based on current state."""
        status_messages = {
            PluginExecutionStatus.IDLE: "Ready to run",
            PluginExecutionStatus.CLONING: "Cloning plugin repository...",
            PluginExecutionStatus.SYNCING: "Syncing dependencies...",
            PluginExecutionStatus.EXTRACTING: "Extracting data...",
            PluginExecutionStatus.TRANSFORMING: "Transforming data...",
            PluginExecutionStatus.COMPLETED: "✅ Plugin execution completed successfully",
            PluginExecutionStatus.FAILED: "❌ Plugin execution failed",
        }

        status_colors = {
            PluginExecutionStatus.IDLE: "normal",
            PluginExecutionStatus.CLONING: "blue",
            PluginExecutionStatus.SYNCING: "blue",
            PluginExecutionStatus.EXTRACTING: "orange",
            PluginExecutionStatus.TRANSFORMING: "orange",
            PluginExecutionStatus.COMPLETED: "green",
            PluginExecutionStatus.FAILED: "red",
        }

        # Update status text
        status_text = message or status_messages.get(self.status, "Unknown status")
        color = status_colors.get(self.status, "normal")

        if color == "green":
            self.status_container.success(status_text)
        elif color == "red":
            self.status_container.error(status_text)
        elif color == "orange":
            self.status_container.warning(status_text)
        elif color == "blue":
            self.status_container.info(status_text)
        else:
            self.status_container.write(status_text)

        # Update progress bar
        progress_values = {
            PluginExecutionStatus.IDLE: 0.0,
            PluginExecutionStatus.CLONING: 0.1,
            PluginExecutionStatus.SYNCING: 0.2,
            PluginExecutionStatus.EXTRACTING: 0.6,
            PluginExecutionStatus.TRANSFORMING: 0.9,
            PluginExecutionStatus.COMPLETED: 1.0,
            PluginExecutionStatus.FAILED: 0.0,
        }

        progress_value = progress_values.get(self.status, 0.0)
        if self.status in [PluginExecutionStatus.COMPLETED, PluginExecutionStatus.FAILED]:
            # Hide progress bar when completed/failed
            self.progress_container.empty()
        else:
            self.progress_container.progress(
                progress_value, text=f"Progress: {int(progress_value * 100)}%"
            )

    def clear(self):
        """Clear all display containers."""
        self.status_container.empty()
        self.progress_container.empty()
        self.output_container.empty()


class PluginOutputDisplay:
    """
    Manages real-time output display for plugin execution in Streamlit.
    """

    def __init__(self, plugin_id: str):
        self.plugin_id = plugin_id
        self.output_lines = []
        self.max_lines = 1000  # Limit output to prevent memory issues

        # Create output display container
        with st.expander(f"📋 Plugin Output - {plugin_id}", expanded=True):
            self.output_container = st.empty()
            self._update_output_display()

    def append_line(self, line: str):
        """Append a new line to the output display."""
        self.output_lines.append(line)

        # Limit the number of lines to prevent memory issues
        if len(self.output_lines) > self.max_lines:
            self.output_lines = self.output_lines[-self.max_lines :]

        self._update_output_display()

    def clear(self):
        """Clear the output display."""
        self.output_lines = []
        self._update_output_display()

    def _update_output_display(self):
        """Update the output display with current lines."""
        if not self.output_lines:
            self.output_container.code("No output yet...")
        else:
            output_text = "".join(self.output_lines)
            self.output_container.code(output_text, language="text")
