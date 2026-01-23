import sys
from typing import Callable, Optional


class OutputTee:
    """
    Manages dual output streaming to both system stdout and optional callback.
    Maintains existing stdout behavior while enabling real-time display.
    """

    def __init__(self, output_callback: Optional[Callable[[str], None]] = None):
        self.output_callback = output_callback

    def write_line(self, line: str) -> None:
        """
        Write a line to both system stdout and callback.

        Args:
            line: The line to write (without newline character)
        """
        # Always write to system stdout to maintain existing behavior
        print(line, end="", file=sys.stdout, flush=True)

        # Optionally send to callback for real-time display
        if self.output_callback:
            self.output_callback(line + "\n")

    def write(self, text: str) -> None:
        """
        Write text to both system stdout and callback.

        Args:
            text: The text to write
        """
        # Always write to system stdout to maintain existing behavior
        sys.stdout.write(text)
        sys.stdout.flush()

        # Optionally send to callback for real-time display
        if self.output_callback:
            self.output_callback(text)
