# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
# A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
# Eidgenössische Technische Hochschule Zürich (ETHZ).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Communicator class for printing click output."""

import sys

import click
from tqdm import tqdm

from renku.core.utils.communication import CommunicationCallback


class StandardOutput(CommunicationCallback):
    """Communication listener that outputs to stdout/stderr."""

    def __init__(self):
        super().__init__()
        self._progress_bars = {}
        self._progress_types = ["download"]

    def echo(self, msg, end="\n"):
        """Write a message."""
        with CommunicationCallback.lock:
            print(msg, end=end)

    def info(self, msg):
        """Write an info message."""
        with CommunicationCallback.lock:
            print(msg)

    def warn(self, msg):
        """Write a warning message."""
        with CommunicationCallback.lock:
            print(msg)

    def error(self, msg):
        """Write an error message."""
        with CommunicationCallback.lock:
            print(msg, file=sys.stderr)

    def confirm(self, msg, abort=False, warning=False):
        """Get confirmation for an action."""
        return False

    def start_progress(self, name, total, **kwargs):
        """Start a new tqdm progressbar."""
        if name in self._progress_bars:
            raise ValueError("Name {} is already a registered progressbar.".format(name))

        if "type" not in kwargs:
            kwargs["type"] = "download"

        if kwargs["type"] not in self._progress_types:
            self._progress_bars[name] = None
        elif kwargs["type"] == "download":
            self._progress_bars[name] = tqdm(
                total=total,
                unit="iB",
                unit_scale=True,
                desc=name,
                leave=False,
                bar_format="{desc:.32}: {percentage:3.0f}%|{bar}{r_bar}",
            )

    def update_progress(self, name, amount):
        """Update a progressbar."""
        if name not in self._progress_bars or not self._progress_bars[name]:
            return

        self._progress_bars[name].update(amount)

    def finalize_progress(self, name):
        """End a progressbar."""
        if name not in self._progress_bars or not self._progress_bars[name]:
            return

        self._progress_bars[name].close()
        del self._progress_bars[name]


class ClickCallback(StandardOutput):
    """CommunicationCallback implementation for ``click`` messages."""

    INFO = click.style("Info: ", bold=True, fg="blue")
    WARNING = click.style("Warning: ", bold=True, fg="yellow")
    ERROR = click.style("Error: ", bold=True, fg="red")

    def echo(self, msg, end="\n"):
        """Write a message."""
        new_line = True
        if end != "\n":
            msg = msg + end
            new_line = False
        click.echo(msg, nl=new_line)

    def info(self, msg):
        """Write an info message."""
        click.echo(self.INFO + msg)

    def warn(self, msg):
        """Write a warning message."""
        click.echo(self.WARNING + msg)

    def error(self, msg):
        """Write an error message."""
        click.echo(self.ERROR + msg)

    def has_prompt(self):
        """Return True if communicator provides a direct prompt to users."""
        return True

    def confirm(self, msg, abort=False, warning=False):
        """Get confirmation for an action using a prompt."""
        prefix = self.WARNING if warning else ""
        return click.confirm(prefix + msg, abort=abort)

    def prompt(self, msg, type=None, default=None):
        """Show a message prompt from the first callback that has a prompt."""
        return click.prompt(msg, type=type, default=default)
