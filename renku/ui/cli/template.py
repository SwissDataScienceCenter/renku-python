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

Description
~~~~~~~~~~~

Renku projects are initialized using a project template. Renku has a set of
built-in templates that you can use in your projects. These templates can be
listed by using:

Commands and options
~~~~~~~~~~~~~~~~~~~~

.. rst-class:: cli-reference-commands

.. click:: renku.ui.cli.template:template
   :prog: renku template
   :nested: full

Set a template
~~~~~~~~~~~~~~

You can change a project's template using ``renku template set`` command:

.. code-block:: console

    $ renku template set <template-id>

or use a template from a different source:

.. code-block:: console

    $ renku template set <template-id> --source <template-repo-url>

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

Passing ``--dry-run`` flags shows the newest available template version and a
list of files that will be updated.

.. note::

    You can specify a template version for a project by passing a
    ``--reference`` when setting it (or when initializing a project). This
    approach only works for templates from sources other than Renku because
    Renku templates are bound to the Renku version. Note that although a
    reference can be a git tag, branch or commit SHA, it's recommended to use
    only git tags as a reference.

.. note::

    A template maintainer can disable updates for a template. In this case,
    ``renku update`` refuses to update the project. Passing ``--force`` flag
    causes Renku to update the template anyways.

.. note::

    Renku always preserve the project's Renku version that is set in the Dockerfile
    even if you overwrite the Dockerfile. The reason is that the project's metadata
    is not updated when setting/updating a template and therefore the project
    won't work with a different Renku version. To update Renku version you need
    to use ``renku migrate`` command.


Validating a template repository
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you are developing your own templates in a template repository, there are
some rules that templates have to follow. To assist in creating your own
templates, you can check that everything is ok with:

.. code-block:: console

    $ renku template validate

Running this inside a template repository (not in a Renku project) will
check that the manifest and individual templates are correct and follow
Renku template conventions, printing warnings or errors if something
needs to be changed.

.. cheatsheet::
   :group: Project Templates
   :command: $ renku template ls
   :description: List available Renku templates.
   :target: rp

.. cheatsheet::
   :group: Project Templates
   :command: $ renku template show <template>
   :description: Show detailed information for the given template.
   :target: rp

.. cheatsheet::
   :group: Project Templates
   :command: $ renku template update
   :description: Update the project's template if a newer version is available.
   :target: rp

.. cheatsheet::
   :group: Project Templates
   :command: $ renku template set <template>
   :description: Replace the project's template with the given template.
   :target: rp

.. cheatsheet::
   :group: Misc
   :command: $ renku template validate
   :description: Check a template repository for possible errors (useful when creating Renku templates).
   :target: rp

"""

import functools
import json
from typing import TYPE_CHECKING, Any, Dict, List

import click

import renku.ui.cli.utils.color as color
from renku.ui.cli.init import parse_parameters

if TYPE_CHECKING:
    from renku.command.view_model.template import TemplateChangeViewModel, TemplateViewModel


@click.group()
@click.pass_context
def template(ctx):
    """Template commands."""
    from renku.ui.cli import is_allowed_subcommand

    is_allowed_subcommand(ctx)


@template.command("ls")
@click.option("-s", "--source", help="Provide the templates repository url or path")
@click.option(
    "-r", "--reference", default=None, help="Specify the reference to checkout on the remote template repository"
)
@click.option("-v", "--verbose", is_flag=True, help="Show detailed description for templates and parameters")
def list_templates(source, reference, verbose):
    """Show available templates in Renku or in a template source."""
    from renku.command.template import list_templates_command
    from renku.ui.cli.utils.callback import ClickCallback

    result = (
        list_templates_command().with_communicator(ClickCallback()).build().execute(source=source, reference=reference)
    )

    _print_template_list(result.output, verbose=verbose)


@template.command("show")
@click.option("-s", "--source", help="Provide the templates repository url or path")
@click.option(
    "-r", "--reference", default=None, help="Specify the reference to checkout on the remote template repository"
)
@click.argument("template-id", required=False, default=None)
def show_template(source, reference, template_id):
    """Show detailed template information for a single template."""
    from renku.command.template import show_template_command
    from renku.ui.cli.utils.callback import ClickCallback

    result = (
        show_template_command()
        .with_communicator(ClickCallback())
        .build()
        .execute(source=source, reference=reference, id=template_id)
    )
    _print_template(result.output)


@template.command("set")
@click.option("-s", "--source", help="Provide the templates repository url or path")
@click.option(
    "-r", "--reference", default=None, help="Specify the reference to checkout on the remote template repository"
)
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
@click.option("-f", "--force", is_flag=True, help="Override existing template")
@click.option("-i", "--interactive", is_flag=True, help="Ask for overwriting files or parameter values")
@click.option("-n", "--dry-run", is_flag=True, help="Show what would have been changed")
@click.argument("template-id", required=False, default=None)
def set_template(source, reference, template_id, parameters, force, interactive, dry_run):
    """Set a template for a project."""
    from renku.command.template import set_template_command
    from renku.ui.cli.utils.callback import ClickCallback

    result = (
        set_template_command()
        .with_communicator(ClickCallback())
        .build()
        .execute(
            source=source,
            reference=reference,
            id=template_id,
            input_parameters=parameters,
            force=force,
            interactive=interactive,
            dry_run=dry_run,
        )
    )

    if dry_run:
        _print_template_change(result.output)


@template.command("update")
@click.option("-f", "--force", is_flag=True, help="Force an update for fixed template versions")
@click.option("-i", "--interactive", is_flag=True, help="Ask for overwriting files or parameter values")
@click.option("-n", "--dry-run", is_flag=True, help="Show what would have been updated")
def update_template(force, interactive, dry_run):
    """Update a project's template."""
    from renku.command.template import update_template_command
    from renku.ui.cli.utils.callback import ClickCallback

    result = (
        update_template_command()
        .with_communicator(ClickCallback())
        .build()
        .execute(force=force, interactive=interactive, dry_run=dry_run)
    )

    if not result.output:
        click.secho("Template is up-to-date", fg="green")
    elif dry_run:
        _print_template_change(result.output)


@template.command("validate")
@click.pass_context
@click.option("json_format", "--json", is_flag=True, help="Return result as JSON.")
@click.option(
    "-r",
    "--reference",
    type=click.STRING,
    help="Git revision/branch/tag to validate the template at.",
)
@click.option(
    "-s",
    "--source",
    type=click.STRING,
    help="Remote template repository to clone and check.",
)
def validate_template(ctx, json_format, reference, source):
    """Validate a template repository and check for common issues."""
    from renku.command.template import validate_templates_command

    result = validate_templates_command().build().execute(source=source, reference=reference).output

    if json_format:
        click.echo(json.dumps(result))
    else:
        if result["warnings"]:
            click.secho("Manifest Warnings:", fg="yellow")
            for warning in result["warnings"]:
                click.secho(f"\t{warning}", fg="yellow")
        if result["manifest"]:
            click.secho(f"Manifest Errors:\n\t{result['manifest']}", fg="red")
        if result["templates"]:
            click.secho("Template Errors:", fg="red")

            for template_id, messages in result["templates"].items():
                click.secho(f"\t{template_id}:", fg="red")

                for message in messages:
                    click.secho(f"\t\t{message}", fg="red")
        if result["valid"]:
            click.secho("OK", fg=color.GREEN)
    if not result["valid"]:
        ctx.exit(1)


def _print_template(template: "TemplateViewModel"):
    """Print detailed template info."""
    from renku.core.util.util import to_string

    print_name = functools.partial(click.style, bold=True, fg="magenta")
    print_value = functools.partial(click.style, bold=True)

    def print_template_parameters():
        for p in template.parameters:
            click.echo(f"  {print_value(p.name)}:")
            click.echo(print_name("    Description: ") + print_value(p.description))
            if p.type:
                click.echo(print_name("    Type: ") + print_value(p.type))
            if p.possible_values:
                click.echo(print_name("    Possible values: ") + print_value(p.possible_values))
            if p.default is not None:
                click.echo(print_name("    Default value: ") + print_value(p.default))

    click.echo(print_name("Id: ") + print_value(template.id))
    click.echo(print_name("Name: ") + print_value(template.name))
    click.echo(print_name("Source: ") + print_value(template.source))
    click.echo(print_name("Reference: ") + print_value(to_string(template.reference)))
    click.echo(print_name("Version: ") + print_value(template.version))
    click.echo(print_name("Description: ") + print_value(template.description))
    click.echo(print_name("Parameters:"))
    print_template_parameters()
    click.echo(print_name("Immutable files: ") + print_value(template.immutable_files))
    click.echo(print_name("Available versions: ") + print_value(template.versions))


def _print_template_list(templates: List["TemplateViewModel"], verbose: bool):
    """Print a list of templates."""
    from renku.core.util.tabulate import tabulate

    for index, template in enumerate(templates, start=1):
        setattr(template, "index", index)

    headers: Dict[str, Any] = {"index": None, "id": None}
    if verbose:
        headers["description"] = "Description"
        # "variables": "Parameters"

    table = tabulate(templates, headers=headers)
    click.echo(table)


def _print_template_change(changes: "TemplateChangeViewModel"):
    """Print detailed template info."""
    from renku.core.util.util import to_string

    print_name = functools.partial(click.style, bold=True, fg="magenta")
    print_value = functools.partial(click.style, bold=True)

    click.echo(print_name("Id: ") + print_value(changes.id))
    click.echo(print_name("Source: ") + print_value(changes.source))
    click.echo(print_name("New reference: ") + print_value(to_string(changes.reference)))
    click.echo(print_name("New version: ") + print_value(to_string(changes.version)))
    click.echo(print_name("File changes:"))
    click.echo("  " + "\n  ".join(changes.file_changes))
