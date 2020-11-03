# -*- coding: utf-8 -*-
#
# Copyright 2017, 2018 - Swiss Data Science Center (SDSC)
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
r"""Create an empty Renku project or reinitialize an existing one.

Start a Renku project
~~~~~~~~~~~~~~~~~~~~~

If you have an existing directory which you want to turn into a Renku project,
you can type:

.. code-block:: console

    $ cd ~/my_project
    $ renku init

or:

.. code-block:: console

    $ renku init ~/my_project

This creates a new subdirectory named ``.renku`` that contains all the
necessary files for managing the project configuration.

If provided directory does not exist, it will be created.

Use a different template
~~~~~~~~~~~~~~~~~~~~~~~~

Renku is installed together with a specific set of templates you can select
when you initialize a project. You can check them by typing:

.. code-block:: console

    $ renku init --list-templates

    INDEX ID     DESCRIPTION                     PARAMETERS
    ----- ------ ------------------------------- -----------------------------
    1     python The simplest Python-based [...] description: project des[...]
    2     R      R-based renku project with[...] description: project des[...]

If you know which template you are going to use, you can provide either the id
``--template-id`` or the template index number ``--template-index``.

You can use a newer version of the templates or even create your own one and
provide it to the ``init`` command by specifying the target template repository
source ``--template-source`` (both local path and remote url are supported) and
the reference ``--template-ref`` (branch, tag or commit).

You can take inspiration from the
`official Renku template repository
<https://github.com/SwissDataScienceCenter/renku-project-template>`_

.. code-block:: console

    $ renku init --template-ref master --template-source \
    https://github.com/SwissDataScienceCenter/renku-project-template

    Fetching template from
    https://github.com/SwissDataScienceCenter/renku-project-template@master
    ... OK

    INDEX ID             DESCRIPTION                PARAMETERS
    ----- -------------- -------------------------- ----------------------
    1     python-minimal Basic Python Project:[...] description: proj[...]
    2     R-minimal      Basic R Project: The [...] description: proj[...]

    Please choose a template by typing the index:

Provide parameters
~~~~~~~~~~~~~~~~~-

Some templates require parameters to properly initialize a new project. You
can check them by listing the templates ``--list-templates``.

To provide parameters, use the ``--parameter`` option and provide each
parameter using ``--parameter "param1"="value1"``.

.. code-block:: console

    $ renku init --template-id python-minimal --parameter \
    "description"="my new shiny project"

    Initializing new Renku repository... OK

If you don't provide the required parameters through the option
``-parameter``, you will be asked to provide them. Empty values are allowed
and passed to the template initialization function.

.. note:: Every project requires a ``name`` that can either be provided using
   ``--name`` or automatically taken from the target folder. This is
   also considered as a special parameter, therefore it's automatically added
   to the list of parameters forwarded to the ``init`` command.

Update an existing project
~~~~~~~~~~~~~~~~~~~~~~~~~~

There are situations when the required structure of a Renku project needs
to be recreated or you have an **existing** Git repository. You can solve
these situation by simply adding the ``--force`` option.

.. code-block:: console

    $ git init .
    $ echo "# Example\nThis is a README." > README.md
    $ git add README.md
    $ git commit -m 'Example readme file'
    # renku init would fail because there is a git repository
    $ renku init --force

You can also enable the external storage system for output files, if it
was not installed previously.

.. code-block:: console

    $ renku init --force --external-storage

"""

import configparser
import os
from collections import OrderedDict, namedtuple
from pathlib import Path
from tempfile import mkdtemp

import attr
import click
from git import Repo

from renku.core import errors
from renku.core.commands.client import pass_local_client
from renku.core.commands.echo import INFO
from renku.core.commands.git import set_git_home
from renku.core.commands.init import create_from_template, fetch_template
from renku.core.commands.options import option_external_storage_requested
from renku.core.models.tabulate import tabulate

_GITLAB_CI = ".gitlab-ci.yml"
_DOCKERFILE = "Dockerfile"
_REQUIREMENTS = "requirements.txt"
CI_TEMPLATES = [_GITLAB_CI, _DOCKERFILE, _REQUIREMENTS]

INVALID_DATA_DIRS = [".", ".renku", ".git"]
"""Paths that cannot be used as data directory name."""


def parse_parameters(ctx, param, value):
    """Parse parameters to dictionary."""
    parameters = {}
    for parameter in value:
        splitted = parameter.split("=", 1)
        if len(splitted) < 2 or len(splitted[0]) < 1:
            raise errors.ParameterError(
                'Parameter format must be --parameter "param1"="value". ', f'--parameter "{parameter}"'
            )
        parameters[splitted[0]] = splitted[1]
    return parameters


def validate_name(ctx, param, value):
    """Validate a project name."""
    if not value:
        value = os.path.basename(ctx.params["path"].rstrip(os.path.sep))
    return value


def store_directory(value):
    """Store directory as a new Git home."""
    Path(value).mkdir(parents=True, exist_ok=True)
    set_git_home(value)
    return value


def create_template_sentence(templates, describe=False, instructions=False):
    """Create templates choice sentence.

    :ref templates: list of templates coming from manifest file
    :ref instructions: add instructions
    """
    Template = namedtuple("Template", ["index", "id", "description", "variables"])

    def extract_description(template_elem):
        """Extract description from template manifest."""
        if describe:
            return template_elem["description"]
        return None

    def extract_variables(template_elem):
        """Extract variables from tempalte manifest."""
        if describe:
            return "\n".join(
                [f"{variable[0]}: {variable[1]}" for variable in template_elem.get("variables", {}).items()]
            )

        return ",".join(template_elem.get("variables", {}).keys())

    templates_friendly = [
        Template(
            index=index + 1,
            id=template_elem["folder"],
            description=extract_description(template_elem),
            variables=extract_variables(template_elem),
        )
        for index, template_elem in enumerate(templates)
    ]

    table_headers = OrderedDict((("index", "Index"), ("id", "Id"), ("variables", "Parameters"),))

    if describe:
        table_headers["description"] = "Description"

    text = tabulate(templates_friendly, headers=table_headers)

    if not instructions:
        return text
    return "{0}\nPlease choose a template by typing the index".format(text)


def is_path_empty(path):
    """Check if path contains files.

    :ref path: target path
    """
    gen = Path(path).glob("**/*")
    return not any(gen)


def resolve_data_directory(data_dir, path):
    """Check data directory is within the project path."""
    if not data_dir:
        return

    absolute_data_dir = (Path(path) / data_dir).resolve()

    try:
        data_dir = absolute_data_dir.relative_to(path)
    except ValueError:
        raise errors.ParameterError(f"Data directory {data_dir} is not within project {path}")

    if str(data_dir).rstrip(os.path.sep) in INVALID_DATA_DIRS:
        raise errors.ParameterError(f"Cannot use {data_dir} as data directory.")

    return data_dir


def check_git_user_config():
    """Check that git user information is configured."""
    dummy_git_folder = mkdtemp()
    repo = Repo.init(dummy_git_folder)
    git_config = repo.config_reader()
    try:
        git_config.get_value("user", "name", None)
        git_config.get_value("user", "email", None)
        return True
    except (configparser.NoOptionError, configparser.NoSectionError):
        return False


@click.command()
@click.argument(
    "path", default=".", type=click.Path(writable=True, file_okay=False, resolve_path=True),
)
@click.option(
    "-n", "--name", callback=validate_name, help="Provide a custom project name.",
)
@click.option(
    "--data-dir",
    default=None,
    type=click.Path(writable=True, file_okay=False),
    help="Data directory within the project",
)
@click.option("-t", "--template-id", help="Provide the id of the template to use.")
@click.option(
    "-i", "--template-index", help="Provide the index number of the template to use.", type=int,
)
@click.option("-s", "--template-source", help="Provide the templates repository url or path.")
@click.option(
    "-r", "--template-ref", default="master", help="Specify the reference to checkout on remote template repository.",
)
@click.option(
    "-p",
    "--parameter",
    "metadata",
    multiple=True,
    type=click.STRING,
    callback=parse_parameters,
    help=(
        "Provide parameters value. Should be invoked once per parameter. "
        'Please specify the values as follow: --parameter "param1"="value"'
    ),
)
@click.option("-l", "--list-templates", is_flag=True, help="List templates available in the template-source.")
@click.option("-d", "--describe", is_flag=True, help="Show description for templates and parameters")
@click.option("--force", is_flag=True, help="Override target path.")
@option_external_storage_requested
@pass_local_client
@click.pass_context
def init(
    ctx,
    client,
    external_storage_requested,
    path,
    name,
    template_id,
    template_index,
    template_source,
    template_ref,
    metadata,
    list_templates,
    force,
    describe,
    data_dir,
):
    """Initialize a project in PATH. Default is the current path."""
    # verify dirty path
    if not is_path_empty(path) and not force and not list_templates:
        existing_paths = [str(p.relative_to(path)) for p in Path(path).iterdir()]
        existing_paths.sort()
        raise errors.InvalidFileOperation(
            f'Folder "{str(path)}" is not empty and contains the following files/directories:'
            + "".join((f"\n\t{e}" for e in existing_paths))
            + "\nPlease add --force flag to transform it into a Renku repository."
        )

    data_dir = resolve_data_directory(data_dir, path)

    if not check_git_user_config():
        raise errors.ConfigurationError(
            "The user name and email are not configured. "
            'Please use the "git config" command to configure them.\n\n'
            '\tgit config --global --add user.name "John Doe"\n'
            "\tgit config --global --add user.email "
            '"john.doe@example.com"\n'
        )

    template_manifest, template_folder, template_source, template_version = fetch_template(
        template_source, template_ref, click.echo
    )

    # select specific template
    repeat = False
    template_data = None
    if template_id:
        if template_index:
            raise errors.ParameterError("Use either --template-id or --template-index, not both", '"--template-index"')
        template_filtered = [
            template_elem for template_elem in template_manifest if template_elem["folder"] == template_id
        ]
        if len(template_filtered) == 1:
            template_data = template_filtered[0]
        else:
            click.echo(f'The template with id "{template_id}" is not available.')
            repeat = True

    if template_index or template_index == 0:
        if template_index > 0 and template_index <= len(template_manifest):
            template_data = template_manifest[template_index - 1]
        else:
            click.echo(f"The template at index {template_index} is not available.")
            repeat = True

    if list_templates:
        if template_data:
            click.echo(create_template_sentence([template_data], describe=describe))
        else:
            click.echo(create_template_sentence(template_manifest, describe=describe))
        return

    if repeat or not (template_id or template_index):
        templates = [template_elem for template_elem in template_manifest]
        if len(templates) == 1:
            template_data = templates[0]
        else:
            template_index = click.prompt(
                text=create_template_sentence(templates, describe=describe, instructions=True),
                type=click.IntRange(1, len(templates)),
                show_default=False,
                show_choices=False,
            )
            template_data = templates[template_index - 1]

        template_id = template_data["folder"]

    # verify variables have been passed
    template_variables = template_data.get("variables", {})
    template_variables_keys = set(template_variables.keys())
    input_parameters_keys = set(metadata.keys())
    for key in template_variables_keys - input_parameters_keys:
        value = click.prompt(
            text=(f'The template requires a value for "{key}" ' f"({template_variables[key]})"),
            default="",
            show_default=False,
        )
        metadata[key] = value
    useless_variables = input_parameters_keys - template_variables_keys
    if len(useless_variables) > 0:
        click.echo(
            INFO + "These parameters are not used by the template and were "
            "ignored:\n\t{}".format("\n\t".join(useless_variables))
        )
        for key in useless_variables:
            del metadata[key]

    # set local path and storage
    store_directory(path)
    if not client.external_storage_requested:
        external_storage_requested = False
    ctx.obj = client = attr.evolve(
        client, path=path, data_dir=data_dir, external_storage_requested=external_storage_requested
    )
    if not is_path_empty(path):
        from git import GitCommandError

        try:
            commit = client.find_previous_commit("*")
            branch_name = "pre_renku_init_{0}".format(commit.hexsha[:7])
            with client.worktree(
                path=path,
                branch_name=branch_name,
                commit=commit,
                merge_args=["--no-ff", "-s", "recursive", "-X", "ours", "--allow-unrelated-histories"],
            ):
                click.echo("Saving current data in branch {0}".format(branch_name))
        except AttributeError:
            click.echo("Warning! Overwriting non-empty folder.")
        except GitCommandError as e:
            click.UsageError(e)

    # supply additional metadata
    metadata["__template_source__"] = template_source
    metadata["__template_ref__"] = template_ref
    metadata["__template_id__"] = template_id
    metadata["__namespace__"] = ""
    metadata["__sanitized_project_name__"] = ""
    metadata["__repository__"] = ""
    metadata["__project_slug__"] = ""

    # clone the repo
    template_path = template_folder / template_data["folder"]
    click.echo("Initializing new Renku repository... ", nl=False)
    with client.lock:
        try:
            create_from_template(
                template_path=template_path,
                client=client,
                name=name,
                metadata=metadata,
                template_version=template_version,
                immutable_template_files=template_data.get("immutable_template_files", []),
                automated_update=template_data.get("allow_template_update", False),
                force=force,
                data_dir=data_dir,
            )
        except FileExistsError as e:
            raise click.UsageError(e)

    # Install git hooks
    from .githooks import install

    ctx.invoke(install, force=force)
