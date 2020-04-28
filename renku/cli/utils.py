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
from tqdm import tqdm

import renku.core.utils.communication as communication


class ClickCallback(communication.CommunicationCallback):
    """CommunicationCallback implementation for ``click`` messages."""

    INFO = click.style('Info: ', bold=True, fg='blue')
    WARNING = click.style('Warning: ', bold=True, fg='yellow')
    ERROR = click.style('Error: ', bold=True, fg='red')

    progressbars = {}
    progress_types = ['download']

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

    def start_progress(self, name, total, **kwargs):
        """Start a new tqdm progressbar."""
        if name in self.progressbars:
            raise ValueError(
                'Name {} is already a registered progressbar.'.format(name)
            )

        if 'type' not in kwargs:
            kwargs['type'] = 'download'

        if kwargs['type'] not in self.progress_types:
            self.progressbars[name] = None
        elif kwargs['type'] == 'download':
            self.progressbars[name] = tqdm(
                total=total,
                unit='iB',
                unit_scale=True,
                desc=name,
                leave=False,
                bar_format='{desc:.32}: {percentage:3.0f}%|{bar}{r_bar}'
            )

    def update_progress(self, name, amount):
        """Update a progressbar."""
        if name not in self.progressbars or not self.progressbars[name]:
            return

        self.progressbars[name].update(amount)

    def finalize_progress(self, name):
        """End a progressbar."""
        if name not in self.progressbars or not self.progressbars[name]:
            return

        self.progressbars[name].close()
        del self.progressbars[name]


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
