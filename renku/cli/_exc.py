# -*- coding: utf-8 -*-
#
# Copyright 2019 - Swiss Data Science Center (SDSC)
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
"""The exception handlers."""

import os
import platform
import re
import sys
import textwrap
import traceback
from urllib.parse import urlencode

import click


class IssueFromTraceback(click.Group):
    """Create an issue with formatted exception."""

    REPO_URL = 'https://github.com/SwissDataScienceCenter/renku-python'

    ISSUE_SUFFIX = '/issues/new'

    def main(self, *args, **kwargs):
        """Catch all exceptions."""
        try:
            return super().main(*args, **kwargs)
        except Exception:
            if not (sys.stdin.isatty() and sys.stdout.isatty()):
                raise

            value = click.prompt(
                click.style(
                    'Ahhhhhhhh! You have found a bug. 🐞\n\n',
                    fg='red',
                    bold=True,
                ) + click.style(
                    '1. Open an issue by typing "open";\n',
                    fg='green',
                ) + click.style(
                    '2. Print human-readable information by typing '
                    '"print";\n',
                    fg='yellow',
                ) + click.style(
                    '3. See the full traceback without submitting details '
                    '(default: "ignore").\n\n',
                    fg='red',
                ) + 'Please select an action by typing its name',
                type=click.Choice([
                    'open',
                    'print',
                    'ignore',
                ], ),
                default='ignore',
            )
            getattr(self, '_process_' + value)()

    def _format_issue_title(self):
        """Return formatted title."""
        return textwrap.shorten(
            'cli: renku ' + ' '.join(sys.argv[1:]),
            width=50,
        )

    def _format_issue_body(self, limit=-5):
        """Return formatted body."""
        from renku import __version__

        re_paths = r'(' + r'|'.join([path or os.getcwd()
                                     for path in sys.path]) + r')'
        tb = re.sub(re_paths, '[...]', traceback.format_exc(limit=limit))

        return (
            '## Describe the bug\nA clear and concise description.\n\n'
            '## Details\n'
            '*Please verify and redact the details.*\n\n'
            '**Renku version:** ' + __version__ + '\n'
            '**OS:** ' + platform.system() + ' (' + platform.version() + ')\n'
            '**Python:** ' + platform.python_version() + '\n\n'
            '### Traceback\n\n```\n' + tb + '```\n\n'
            '## Additional context\nAdd any other context about the problem.'
        )

    def _format_issue_url(self):
        """Format full issue URL."""
        query = urlencode({
            'title': self._format_issue_title(),
            'body': self._format_issue_body(),
        })
        return self.REPO_URL + self.ISSUE_SUFFIX + '?' + query

    def _process_open(self):
        """Open link in a browser."""
        click.launch(self._format_issue_url())
        if not click.confirm('Did it work?', default=True):
            click.echo()
            self._process_print()
            click.secho(
                '\nOpen the line manually and copy the text above\n',
                fg='yellow'
            )
            click.secho(
                '  ' + self.REPO_URL + self.ISSUE_SUFFIX + '\n', bold=True
            )

    def _process_print(self):
        """Print link in a console."""
        click.echo(self._format_issue_body(limit=None))

    def _process_ignore(self):
        """Print original exception in a console."""
        raise
