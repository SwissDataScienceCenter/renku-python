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

from renku.core.commands.view_model.log import LOG_COLUMNS, LOG_FORMATS


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
@click.option("--format", type=click.Choice(LOG_FORMATS), default="tabular", help="Choose an output format.")
@click.option("-w", "--workflows", is_flag=True, default=False, help="Show only workflow executions.")
def log(columns, format, workflows):
    """Log in to the platform."""
    from renku.core.commands.log import log_command

    result = log_command().with_database().build().execute(workflows_only=workflows).output
    click.echo(LOG_FORMATS[format](result, columns))
