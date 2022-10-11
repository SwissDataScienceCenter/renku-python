# -*- coding: utf-8 -*-
#
# Copyright 2020-2022 - Swiss Data Science Center (SDSC)
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
"""Renku is not bug-free and you can help us to find them.

GitHub
~~~~~~

You can quickly open an issue on GitHub with a traceback and minimal system
information when you hit an unhandled exception in the CLI.

.. code-block:: text

    Ahhhhhhhh! You have found a bug. 🐞

    1. Open an issue by typing "open";
    2. Print human-readable information by typing "print";
    3. See the full traceback without submitting details (default: "ignore").

    Please select an action by typing its name (open, print, ignore) [ignore]:

Sentry
~~~~~~

When using ``renku`` as a hosted service the Sentry integration can be enabled
to help developers iterate faster by showing them where bugs happen, how often,
and who is affected.

1. Install ``Sentry-SDK`` with ``python -m pip install sentry-sdk``;
2. Set environment variables ``SENTRY_DSN=true`` and
   ``SENTRY_DSN=https://<key>@sentry.<domain>/<project>``.
3. Set the environment variable ``SENTRY_SAMPLE_RATE=0.2``. This would track
   20% of all requests in Sentry performance monitoring. Set to 0 to disable.

.. warning:: User information might be sent to help resolving the problem.
   If you are not using your own Sentry instance you should inform users
   that you are sending possibly sensitive information to a 3rd-party service.
"""

import os
import platform
import re
import sys
import textwrap
import traceback
from urllib.parse import urlencode

import click

import renku.ui.cli.utils.color as color
from renku.command.util import ERROR
from renku.core import errors
from renku.ui.service.config import SENTRY_ENABLED, SENTRY_SAMPLERATE

_BUG = click.style("Ahhhhhhhh! You have found a bug. 🐞\n\n", fg=color.RED, bold=True)
HAS_SENTRY = SENTRY_ENABLED

if SENTRY_ENABLED:
    try:
        from importlib.metadata import PackageNotFoundError, distribution
    except ImportError:
        from importlib_metadata import PackageNotFoundError, distribution  # type: ignore

    try:
        distribution("sentry-sdk")
    except PackageNotFoundError:
        HAS_SENTRY = False


class RenkuExceptionsHandler(click.Group):
    """Handles all RenkuExceptions."""

    def main(self, *args, **kwargs):
        """Catch and print all Renku exceptions."""
        from renku.core.errors import MigrationRequired, ParameterError, ProjectNotSupported, RenkuException, UsageError

        try:
            return super().main(*args, **kwargs)
        except errors.LockError:
            click.echo(
                click.style("Unable to acquire lock.\n", fg=color.RED)
                + "Hint: Please wait for another renku process to finish and then try again."
            )
        except RenkuException as e:
            click.echo(ERROR + str(e), err=True)
            if e.__cause__ is not None:
                click.echo(f"\n{traceback.format_exc()}")
            exit_code = 1
            if isinstance(e, (ParameterError, UsageError)):
                exit_code = 2
            elif isinstance(e, MigrationRequired):
                exit_code = 3
            elif isinstance(e, ProjectNotSupported):
                exit_code = 4
            sys.exit(exit_code)


class IssueFromTraceback(RenkuExceptionsHandler):
    """Create an issue with formatted exception."""

    REPO_URL = "https://github.com/SwissDataScienceCenter/renku-python"

    ISSUE_SUFFIX = "/issues/new"

    def __init__(self, *args, **kwargs):
        """Initialize a Sentry client."""
        super().__init__(*args, **kwargs)

        if HAS_SENTRY:
            import sentry_sdk

            sentry_sdk.init(
                dsn=os.getenv("SENTRY_DSN"), environment=os.getenv("SENTRY_ENV"), traces_sample_rate=SENTRY_SAMPLERATE
            )

    def main(self, *args, **kwargs):
        """Catch all exceptions."""
        try:
            return super().main(*args, **kwargs)
        except Exception:
            if HAS_SENTRY:
                self._handle_sentry()

            if not (sys.stdin.isatty() and sys.stdout.isatty()):
                raise

            self._handle_github()

    def _handle_sentry(self):
        """Handle exceptions using Sentry."""
        from sentry_sdk import capture_exception, configure_scope
        from sentry_sdk.utils import capture_internal_exceptions

        with configure_scope() as scope:
            with capture_internal_exceptions():
                from renku.core.util.git import get_git_repository

                user = get_git_repository().get_user()

                scope.user = {"name": user.name, "email": user.email}

            event_id = capture_exception()
            click.echo(_BUG + "Recorded in Sentry with ID: {0}\n".format(event_id), err=True)
            raise

    def _handle_github(self):
        """Handle exception and submit it as GitHub issue."""
        value = click.prompt(
            _BUG
            + click.style('1. Open an issue by typing "open";\n', fg=color.GREEN)
            + click.style("2. Print human-readable information by typing " '"print";\n', fg=color.YELLOW)
            + click.style(
                "3. See the full traceback without submitting details " '(default: "ignore").\n\n', fg=color.RED
            )
            + "Please select an action by typing its name",
            type=click.Choice(["open", "print", "ignore"]),
            default="ignore",
        )
        getattr(self, "_process_" + value)()

    def _format_issue_title(self):
        """Return formatted title."""
        return textwrap.shorten("cli: renku " + " ".join(sys.argv[1:]), width=50)

    def _format_issue_body(self, limit=-5):
        """Return formatted body."""
        from renku import __version__

        re_paths = r"(" + r"|".join([path or os.getcwd() for path in sys.path]) + r")"
        tb = re.sub(re_paths, "[...]", traceback.format_exc(limit=limit))

        return (
            "## Describe the bug\nA clear and concise description.\n\n"
            "## Details\n"
            "*Please verify and redact the details.*\n\n"
            "**Renku version:** " + __version__ + "\n"
            "**OS:** " + platform.system() + " (" + platform.version() + ")\n"
            "**Python:** " + platform.python_version() + "\n\n"
            "### Traceback\n\n```\n" + tb + "```\n\n"
            "## Additional context\nAdd any other context about the problem."
        )

    def _format_issue_url(self):
        """Format full issue URL."""
        query = urlencode({"title": self._format_issue_title(), "body": self._format_issue_body()})
        return self.REPO_URL + self.ISSUE_SUFFIX + "?" + query

    def _process_open(self):
        """Open link in a browser."""
        click.launch(self._format_issue_url())
        if not click.confirm("Did it work?", default=True):
            click.echo()
            self._process_print()
            click.secho("\nOpen the line manually and copy the text above\n", fg=color.YELLOW)
            click.secho("  " + self.REPO_URL + self.ISSUE_SUFFIX + "\n", bold=True)

    def _process_print(self):
        """Print link in a console."""
        click.echo(self._format_issue_body(limit=None))

    def _process_ignore(self):
        """Print original exception in a console."""
        raise
