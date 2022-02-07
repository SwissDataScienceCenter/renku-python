# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 Swiss Data Science Center (SDSC)
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

You can use ``renku log`` to get a history of renku commands.
At the moment, it only shows workflow executions.

.. code-block:: console

    $ renku log
    DATE                 TYPE  DESCRIPTION
    -------------------  ----  -------------
    2021-09-21 15:46:02  Run   cp A C
    2021-09-21 10:52:51  Run   cp A B

.. cheatsheet::
   :group: Misc
   :command: $ renku log
   :description: Show a history of renku actions.
   :extended:
"""

import click

from renku.core.commands.view_model.log import (
    LOG_COLUMNS,
    LOG_FORMATS,
    ActivityLogViewModel,
    DatasetLogViewModel,
    LogViewModel,
)


def _print_log(log_entry: LogViewModel) -> str:
    """Turn a log entry into a printable string."""
    if isinstance(log_entry, ActivityLogViewModel):
        return _print_activity_log(log_entry)
    elif isinstance(log_entry, DatasetLogViewModel):
        return _print_dataset_log(log_entry)


def _print_activity_log(log_entry: ActivityLogViewModel) -> str:
    """Turn an activity log entry into a printable string."""
    results = [click.style(f"Activity {log_entry.id}", fg="yellow", bold=True)]
    results.append(click.style("Start Time: ", bold=True, fg="magenta") + log_entry.details.start_time)
    results.append(click.style("End Time: ", bold=True, fg="magenta") + log_entry.details.end_time)

    if log_entry.details.user:
        results.append(click.style("User: ", bold=True, fg="magenta") + log_entry.details.user)
    if log_entry.details.renku_version:
        results.append(click.style("Renku Version: ", bold=True, fg="magenta") + log_entry.details.renku_version)

    results.append(click.style("Command: ", bold=True, fg="magenta") + log_entry.description)

    if log_entry.details.inputs:
        results.append(
            click.style("Inputs: \n\t", bold=True, fg="magenta")
            + "\n\t".join(f"{i[0]}: {i[1]}" for i in log_entry.details.inputs)
        )
    if log_entry.details.outputs:
        results.append(
            click.style("Outputs: \n\t", bold=True, fg="magenta")
            + "\n\t".join(f"{o[0]}: {o[1]}" for o in log_entry.details.outputs)
        )
    if log_entry.details.parameters:
        results.append(
            click.style("Parameters: \n\t", bold=True, fg="magenta")
            + "\n\t".join(f"{p[0]}: {p[1]}" for p in log_entry.details.parameters)
        )

    return "\n".join(results)


def _print_dataset_log(log_entry: DatasetLogViewModel) -> str:
    """Turn a dataset log entry into a printable string."""
    results = [click.style(f"Dataset {log_entry.id}", fg="yellow", bold=True)]
    results.append(click.style("Date: ", bold=True, fg="magenta") + log_entry.date.isoformat())
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
        results.append(click.style("Changes: ", bold=True, fg="magenta") + ", ".join(change))

    if log_entry.details.source:
        results.append(click.style("Source: ", bold=True, fg="magenta") + log_entry.details.source)

    if log_entry.details.title_changed:
        results.append(click.style("Title set to: ", bold=True, fg="magenta") + log_entry.details.title_changed)

    if log_entry.details.description_changed:
        results.append(
            click.style("Description set to: ", bold=True, fg="magenta") + log_entry.details.description_changed
        )

    if log_entry.details.files_added or log_entry.details.files_removed:
        added = ""
        removed = ""

        if log_entry.details.files_added:
            added = click.style("\n\t".join(f"+ {f}" for f in log_entry.details.files_added), fg="green")

        if log_entry.details.files_removed:
            removed = ("\n\t" if added else "") + click.style(
                "\n\t".join(f"- {f}" for f in log_entry.details.files_removed), fg="red"
            )

        results.append(click.style("Files modified: \n\t", bold=True, fg="magenta") + added + removed)

    if log_entry.details.creators_added or log_entry.details.creators_removed:
        added = ""
        removed = ""

        if log_entry.details.creators_added:
            added = click.style("\n\t".join(f"+ {c}" for c in log_entry.details.creators_added), fg="green")

        if log_entry.details.creators_removed:
            removed = ("\n\t" if added else "") + click.style(
                "\n\t".join(f"- {c}" for c in log_entry.details.creators_removed), fg="red"
            )

        results.append(click.style("Creators modified: \n\t", bold=True, fg="magenta") + added + removed)

    if log_entry.details.keywords_added or log_entry.details.keywords_removed:
        added = ""
        removed = ""

        if log_entry.details.keywords_added:
            added = click.style("\n\t".join(f"+ {k}" for k in log_entry.details.keywords_added), fg="green")

        if log_entry.details.keywords_removed:
            removed = ("\n\t" if added else "") + click.style(
                "\n\t".join(f"- {k}" for k in log_entry.details.keywords_removed), fg="red"
            )

        results.append(click.style("Keywords modified: \n\t", bold=True, fg="magenta") + added + removed)

    if log_entry.details.images_changed_to:
        results.append(
            click.style("Images set to: \n\t", bold=True, fg="magenta")
            + click.style("\n\t".join(f"{i}" for i in log_entry.details.images_changed_to), fg="green")
        )

    return "\n".join(results)


@click.command()
@click.option(
    "-c",
    "--columns",
    type=click.STRING,
    default="date,type,description",
    metavar="<columns>",
    help="Comma-separated list of column to display: {}.".format(", ".join(LOG_COLUMNS.keys())),
    show_default=True,
)
@click.option("--format", type=click.Choice(LOG_FORMATS), default="detailed", help="Choose an output format.")
@click.option("-w", "--workflows", is_flag=True, default=False, help="Show only workflow executions.")
@click.option("-d", "--datasets", is_flag=True, default=False, help="Show only dataset modifications.")
def log(columns, format, workflows, datasets):
    """Show a history of renku workflow and dataset commands."""
    from renku.core.commands.log import log_command

    result = log_command().with_database().build().execute(workflows_only=workflows, datasets_only=datasets).output
    if format == "detailed":
        entries = sorted(result, key=lambda e: e.date, reverse=True)
        texts = [_print_log(e) for e in entries]
        click.echo("\n\n".join(texts))
    else:
        click.echo(LOG_FORMATS[format](result, columns))
