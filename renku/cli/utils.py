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
r"""Communicator class for printing click output."""

from functools import wraps

import click

import renku.core.utils.communication as communication


class ClickCallback(communication.StandardOutput):
    """CommunicationCallback implementation for ``click`` messages."""

    INFO = click.style('Info: ', bold=True, fg='blue')
    WARNING = click.style('Warning: ', bold=True, fg='yellow')
    ERROR = click.style('Error: ', bold=True, fg='red')

    def echo(self, msg):
        """Write a message."""
        click.echo(msg)

    def info(self, msg):
        """Write an info message."""
        click.echo(self.INFO + msg)

    def warn(self, msg):
        """Write a warning message."""
        click.echo(self.WARNING + msg)

    def error(self, msg):
        """Write an error message."""
        click.echo(self.ERROR + msg)

    def confirm(self, msg, abort=False):
        """Get confirmation for an action using a prompt."""
        return click.confirm(msg, abort=abort)


def click_callback_communication(func):
    """Decorator to add click callback communication."""

    @wraps(func)
    def with_click_callback(*args, **kwargs):
        callback = ClickCallback()
        communication.subscribe(callback)

        try:
            return func(*args, **kwargs)
        finally:
            communication.unsubscribe(callback)

    return with_click_callback
