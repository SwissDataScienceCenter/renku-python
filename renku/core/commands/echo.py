# -*- coding: utf-8 -*-
#
# Copyright 2018-2020 - Swiss Data Science Center (SDSC)
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
"""Custom console echo."""

import functools
import os

import click
from git.remote import RemoteProgress

INFO = click.style('Info: ', bold=True, fg='blue')
WARNING = click.style('Warning: ', bold=True, fg='yellow')
ERROR = click.style('Error: ', bold=True, fg='red')


def echo_via_pager(*args, **kwargs):
    """Display pager only if it does not fit in one terminal screen.

    NOTE: The feature is available only on ``less``-based pager.
    """
    try:
        restore = 'LESS' not in os.environ
        os.environ.setdefault('LESS', '-iXFR')
        click.echo_via_pager(*args, **kwargs)
    finally:
        if restore:
            os.environ.pop('LESS', None)


progressbar = functools.partial(
    click.progressbar,
    fill_char=click.style(u' ', bg='green'),
    show_pos=True,
    item_show_func=lambda x: x,
)


class GitProgress(RemoteProgress):
    """Progress printing for GitPython."""

    def __init__(self):
        """Initialize a Git progress printer."""
        super().__init__()
        self._previous_line_length = 0

    def update(self, op_code, cur_count, max_count=None, message=''):
        """Callback for printing Git operation status."""
        self._clear_line()
        print(self._cur_line, end='\r')
        self._previous_line_length = len(self._cur_line)
        if (op_code & RemoteProgress.END) != 0:
            print()

    def _clear_line(self):
        print(self._previous_line_length * ' ', end='\r')
