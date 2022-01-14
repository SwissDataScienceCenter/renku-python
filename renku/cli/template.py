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

Set a template
~~~~~~~~~~~~~~

To set a template for older Renku project that don't have a template, you can
use:

.. code-block:: console

    $ renku template set <template-id>

or:

.. code-block:: console

    $ renku template set <template-id> --template-source <template-repo-url>

This command fails if the project already has a template. Use ``--force`` flag
to force-change the template.

TODO: Complete the documentation
"""

import click

from renku.cli.init import parse_parameters

_GITLAB_CI = ".gitlab-ci.yml"
_DOCKERFILE = "Dockerfile"
_REQUIREMENTS = "requirements.txt"
CI_TEMPLATES = [_GITLAB_CI, _DOCKERFILE, _REQUIREMENTS]

INVALID_DATA_DIRS = [".", ".renku", ".git"]
"""Paths that cannot be used as data directory name."""


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
@click.option("-n", "--template-index", help="Provide the index number of the template to use.", type=int)
def show_template(template_source, template_ref, template_id, template_index):
    """Show detailed template information for a single template."""
    from renku.cli.utils.callback import ClickCallback
    from renku.core.commands.template import show_template_command

    show_template_command().with_communicator(ClickCallback()).build().execute(
        template_source=template_source,
        template_ref=template_ref,
        template_id=template_id,
        template_index=template_index,
    )


@template.command("set")
@click.option("-s", "--template-source", help="Provide the templates repository url or path.")
@click.option(
    "-r", "--template-ref", default=None, help="Specify the reference to checkout on the remote template repository."
)
@click.option("-t", "--template-id", help="Provide the id of the template to use.")
@click.option("-n", "--template-index", help="Provide the index number of the template to use.", type=int)
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
def set_template(template_source, template_ref, template_id, template_index, parameters, force, interactive):
    """Set a template for a project."""
    from renku.cli.utils.callback import ClickCallback
    from renku.core.commands.template import set_template_command

    set_template_command().with_communicator(ClickCallback()).build().execute(
        template_source=template_source,
        template_ref=template_ref,
        template_id=template_id,
        template_index=template_index,
        parameters=parameters,
        force=force,
        interactive=interactive,
    )


@template.command("update")
@click.option("-f", "--force", is_flag=True, help="Force-update a project with a fixed template reference.")
@click.option("-i", "--interactive", is_flag=True, help="Ask when overwriting an existing file in the project.")
def update_template(force, interactive):
    """Set a template for a project."""
    from renku.cli.utils.callback import ClickCallback
    from renku.core.commands.template import update_template_command

    update_template_command().with_communicator(ClickCallback()).build().execute(force=force, interactive=interactive)
