# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
r"""Manage project templates.

Renku projects are initialized using a project template. Renku has a set of
built-in templates that you can use in your projects. These templates can be
listed by using:

.. code-block:: console

    $ renku template ls

    INDEX  ID              PARAMETERS
    -----  --------------  ------------
        1  python-minimal
        2  R-minimal
        3  julia-minimal

You can use other sources of templates that reside inside a git repository:

.. code-block:: console

    $ renku template ls --template-source https://github.com/SwissDataScienceCenter/renku-project-template

    INDEX  ID              PARAMETERS
    -----  --------------  ------------
        1  python-minimal
        2  R-minimal
        3  julia-minimal

``renku template show`` command can be used to see detailed information about a
single template.


Set a template
~~~~~~~~~~~~~~

You can change a project's template using ``renku template set`` command:

.. code-block:: console

    $ renku template set --template-id <template-id>

or use a template from a different source:

.. code-block:: console

    $ renku template set --template-id <template-id> --template-source <template-repo-url>

This command fails if the project already has a template. Use ``--force`` flag
to force-change the template.

.. note::

    Setting a template overwrites existing files in a project. Pass
    ``--interactive`` flag to get a prompt for selecting which files to keep or
    overwrite.


Update a project's template
~~~~~~~~~~~~~~~~~~~~~~~~~~~
A project's template can be update using:

.. code-block:: console

    $ renku template update

If an update is available, this commands updates all project's files that are
not modified locally by the project. Pass ``--interactive`` flag to select
which files to keep or overwrite.

.. note::

    You can set a fixed template version for a project by passing a
    ``--template-ref`` when setting it (or when initializing a project). This
    approach only works for templates from sources other than Renku because
    Renku templates are bound to the Renku version. Note that a reference
    should be a git tag or commit SHA. If you set a git branch as a reference
    than the template can still be updated.

.. note::

    Passing ``--force`` flag causes Renku to update the template anyways. It
    causes Renku to ignore the template reference and checks the HEAD of the
    template's Git repository for an update.

.. note::

    Renku always preserve project's Renku version that is set in the Dockerfile
    even if you overwrite the Dockerfile. The reason is that project's metadata
    is not updated when setting/updating a template and therefore the project
    won't work with a different Renku version. To update Renku version you need
    to use ``renku migrate`` command.

"""

import click

from renku.cli.init import parse_parameters


@click.group()
def template():
    """Template commands."""


@template.command("ls")
@click.option("-s", "--template-source", help="Provide the templates repository url or path.")
@click.option(
    "-r", "--template-ref", default=None, help="Specify the reference to checkout on the remote template repository."
)
@click.option("-v", "--verbose", is_flag=True, help="Show detailed description for templates and parameters")
def list_templates(template_source, template_ref, verbose):
    """Show available templates in Renku or in a template source."""
    from renku.cli.utils.callback import ClickCallback
    from renku.core.commands.template import list_templates_command

    list_templates_command().with_communicator(ClickCallback()).build().execute(
        template_source=template_source, template_ref=template_ref, verbose=verbose
    )


@template.command("show")
@click.option("-s", "--template-source", help="Provide the templates repository url or path.")
@click.option(
    "-r", "--template-ref", default=None, help="Specify the reference to checkout on the remote template repository."
)
@click.option("-t", "--template-id", help="Provide the id of the template to use.")
def show_template(template_source, template_ref, template_id):
    """Show detailed template information for a single template."""
    from renku.cli.utils.callback import ClickCallback
    from renku.core.commands.template import show_template_command

    show_template_command().with_communicator(ClickCallback()).build().execute(
        template_source=template_source, template_ref=template_ref, template_id=template_id
    )


@template.command("set")
@click.option("-s", "--template-source", help="Provide the templates repository url or path.")
@click.option(
    "-r", "--template-ref", default=None, help="Specify the reference to checkout on the remote template repository."
)
@click.option("-t", "--template-id", help="Provide the id of the template to use.")
@click.option(
    "-p",
    "--parameter",
    "parameters",
    multiple=True,
    type=click.STRING,
    callback=parse_parameters,
    help=(
        "Provide parameters value. Should be invoked once per parameter. "
        'Please specify the values as follow: --parameter "param1"="value"'
    ),
)
@click.option("-f", "--force", is_flag=True, help="Override existing template.")
@click.option("-i", "--interactive", is_flag=True, help="Ask when overwriting an existing file in the project.")
@click.pass_context
def set_template(ctx, template_source, template_ref, template_id, parameters, force, interactive):
    """Set a template for a project."""
    from renku.cli.utils.callback import ClickCallback
    from renku.core import errors
    from renku.core.commands.template import set_template_command
    from renku.core.utils.metadata import is_renku_project_with_repository

    if not is_renku_project_with_repository(ctx.obj):
        raise errors.ProjectNotFound("Cannot set template outside a Renku project")

    set_template_command().with_communicator(ClickCallback()).build().execute(
        template_source=template_source,
        template_ref=template_ref,
        template_id=template_id,
        parameters=parameters,
        force=force,
        interactive=interactive,
    )


@template.command("update")
@click.option("-f", "--force", is_flag=True, help="Force an update for fixed template versions.")
@click.option("-i", "--interactive", is_flag=True, help="Ask when overwriting an existing file in the project.")
@click.pass_context
def update_template(ctx, force, interactive):
    """Update a project's template."""
    from renku.cli.utils.callback import ClickCallback
    from renku.core import errors
    from renku.core.commands.template import update_template_command
    from renku.core.utils.metadata import is_renku_project_with_repository

    if not is_renku_project_with_repository(ctx.obj):
        raise errors.ProjectNotFound("Cannot update template outside a Renku project")

    result = (
        update_template_command()
        .with_communicator(ClickCallback())
        .build()
        .execute(force=force, interactive=interactive)
    )

    if result.output is False:
        click.secho("Template is up-to-date", fg="green")
