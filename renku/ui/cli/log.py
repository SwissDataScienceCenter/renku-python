# -*- coding: utf-8 -*-
#
# Copyright 2018-2022 Swiss Data Science Center (SDSC)
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
"""Renku cli for history of renku commands.

Description
~~~~~~~~~~~

Get the history of renku commands.
At the moment, it shows workflow executions and dataset changes.

Commands and options
~~~~~~~~~~~~~~~~~~~~

.. rst-class:: cli-reference-commands

.. click:: renku.ui.cli.log:log
   :prog: renku log
   :nested: full

Examples
~~~~~~~~~~~

.. code-block:: console

    $ renku log
    Activity /activities/be60896d8d984a0bb585e53f7a3146dc
    Start Time: 2022-02-03T13:56:27+01:00
    End Time: 2022-02-03T13:56:28+01:00
    User: John Doe <John.Doe@example.com>
    Renku Version: renku 1.0.5
    Plan:
        Id: /plans/c00826a571a246e79b0a3d77712e6f3b
        Name: python-test-fc2ec
    Command: python test.py
    Inputs:
            input-1: test.py
    Parameters:
            text: hello

    Dataset testset
    Date: 2022-02-03T11:26:55+01:00
    Changes: created
    Title set to: testset
    Creators modified:
            + John Doe <John.Doe@example.com>

To show only dataset entries, use ``-d``, to show only workflows, use ``-w``.

You can select a format using the ``--format <format>`` argument.

.. cheatsheet::
   :group: Misc
   :command: $ renku log
   :description: Show a history of renku actions.
   :target: rp
"""

import click

import renku.ui.cli.utils.color as color
from renku.command.view_model.log import (
    LOG_COLUMNS,
    LOG_FORMATS,
    ActivityLogViewModel,
    DatasetLogViewModel,
    LogViewModel,
)
from renku.ui.cli.utils.terminal import show_text_with_pager, strip_ansi_codes


def _print_log(log_entry: LogViewModel) -> str:
    """Turn a log entry into a printable string."""
    if isinstance(log_entry, ActivityLogViewModel):
        return _print_activity_log(log_entry)
    elif isinstance(log_entry, DatasetLogViewModel):
        return _print_dataset_log(log_entry)

    raise NotImplementedError()


def _print_activity_log(log_entry: ActivityLogViewModel) -> str:
    """Turn an activity log entry into a printable string."""
    from renku.ui.cli.utils.terminal import style_header, style_key

    results = [
        style_header(f"Activity {log_entry.id}"),
        style_key("Start Time: ") + log_entry.details.start_time,
        style_key("End Time: ") + log_entry.details.end_time,
    ]

    if log_entry.details.user:
        results.append(style_key("User: ") + log_entry.details.user)
    if log_entry.details.renku_version:
        results.append(style_key("Renku Version: ") + log_entry.details.renku_version)

    results.append(style_key("Plan:"))
    results.append(style_key("\tId: ") + log_entry.plan.id)
    results.append(style_key("\tName: ") + log_entry.plan.name)

    results.append(style_key("Command: ") + log_entry.description)

    if log_entry.details.inputs:
        results.append(style_key("Inputs:\n\t") + "\n\t".join(f"{i[0]}: {i[1]}" for i in log_entry.details.inputs))
    if log_entry.details.outputs:
        results.append(style_key("Outputs:\n\t") + "\n\t".join(f"{o[0]}: {o[1]}" for o in log_entry.details.outputs))
    if log_entry.details.parameters:
        results.append(
            style_key("Parameters:\n\t") + "\n\t".join(f"{p[0]}: {p[1]}" for p in log_entry.details.parameters)
        )

    return "\n".join(results)


def _print_dataset_log(log_entry: DatasetLogViewModel) -> str:
    """Turn a dataset log entry into a printable string."""
    from renku.ui.cli.utils.terminal import style_header, style_key

    results = [style_header(f"Dataset {log_entry.id}"), style_key("Date: ") + log_entry.date.isoformat()]
    change = []
    if log_entry.details.created:
        change.append("created")
    if log_entry.details.imported:
        change.append("imported")
    if log_entry.details.deleted:
        change.append("deleted")
    if log_entry.details.migrated:
        change.append("migrated")
    if log_entry.details.modified:
        change.append("modified")

    if change:
        results.append(style_key("Changes: ") + ", ".join(change))

    if log_entry.details.source:
        results.append(style_key("Source: ") + log_entry.details.source)

    if log_entry.details.title_changed:
        results.append(style_key("Title set to: ") + log_entry.details.title_changed)

    if log_entry.details.description_changed:
        results.append(style_key("Description set to: ") + log_entry.details.description_changed)

    if log_entry.details.files_added or log_entry.details.files_removed:
        added = ""
        removed = ""

        if log_entry.details.files_added:
            added = click.style("\n\t".join(f"+ {f}" for f in log_entry.details.files_added), fg=color.GREEN)

        if log_entry.details.files_removed:
            removed = ("\n\t" if added else "") + click.style(
                "\n\t".join(f"- {f}" for f in log_entry.details.files_removed), fg=color.RED
            )

        results.append(style_key("Files modified:\n\t") + added + removed)

    if log_entry.details.creators_added or log_entry.details.creators_removed:
        added = ""
        removed = ""

        if log_entry.details.creators_added:
            added = click.style("\n\t".join(f"+ {c}" for c in log_entry.details.creators_added), fg=color.GREEN)

        if log_entry.details.creators_removed:
            removed = ("\n\t" if added else "") + click.style(
                "\n\t".join(f"- {c}" for c in log_entry.details.creators_removed), fg=color.RED
            )

        results.append(style_key("Creators modified:\n\t") + added + removed)

    if log_entry.details.keywords_added or log_entry.details.keywords_removed:
        added = ""
        removed = ""

        if log_entry.details.keywords_added:
            added = click.style("\n\t".join(f"+ {k}" for k in log_entry.details.keywords_added), fg=color.GREEN)

        if log_entry.details.keywords_removed:
            removed = ("\n\t" if added else "") + click.style(
                "\n\t".join(f"- {k}" for k in log_entry.details.keywords_removed), fg=color.RED
            )

        results.append(style_key("Keywords modified:\n\t") + added + removed)

    if log_entry.details.images_changed_to:
        results.append(
            style_key("Images set to:\n\t")
            + click.style("\n\t".join(f"{i}" for i in log_entry.details.images_changed_to), fg=color.GREEN)
        )

    return "\n".join(results)


@click.command()
@click.option(
    "-c",
    "--columns",
    type=click.STRING,
    default="date,type,description",
    metavar="<columns>",
    help="Comma-separated list of column to display (for 'tabular' format): {}.".format(", ".join(LOG_COLUMNS.keys())),
    show_default=True,
)
@click.option(
    "--format",
    type=click.Choice(list(LOG_FORMATS.keys())),
    default="detailed",
    help="Choose an output format (default: detailed).",
)
@click.option("-w", "--workflows", is_flag=True, default=False, help="Show only workflow executions.")
@click.option("-d", "--datasets", is_flag=True, default=False, help="Show only dataset modifications.")
@click.option("--no-pager", is_flag=True, help="Don't use pager (less) for output.")
@click.option("-c", "--no-color", is_flag=True, help="Do not colorize output.")
def log(columns, format, workflows, datasets, no_pager, no_color):
    """Show a history of renku workflow and dataset commands."""
    from renku.command.log import log_command

    result = log_command().with_database().build().execute(workflows_only=workflows, datasets_only=datasets).output
    if format == "detailed":
        entries = sorted(result, key=lambda e: e.date, reverse=True)
        text = "\n\n".join([_print_log(e) for e in entries])

        if no_color:
            text = strip_ansi_codes(text)
    else:
        text = LOG_FORMATS[format](result, columns)  # type: ignore

    if no_pager:
        click.echo(text)
    else:
        show_text_with_pager(text)
