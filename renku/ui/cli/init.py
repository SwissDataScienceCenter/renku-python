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
r"""Create an empty Renku project or reinitialize an existing one.

Description
~~~~~~~~~~~

Use existing renku templates, either official or from the community, to
create a new Renku project or Renku-ize an existing one.

Commands and options
~~~~~~~~~~~~~~~~~~~~

.. rst-class:: cli-reference-commands

.. click:: renku.ui.cli.init:init
   :prog: renku init
   :nested: full

Use a different template
~~~~~~~~~~~~~~~~~~~~~~~~

Renku is installed together with a specific set of templates you can select
when you initialize a project. You can check them by typing:

.. code-block:: console

    $ renku template ls

    INDEX ID     DESCRIPTION                     PARAMETERS
    ----- ------ ------------------------------- -----------------------------
    1     python The simplest Python-based [...] description: project des[...]
    2     R      R-based renku project with[...] description: project des[...]

If you know which template you are going to use, you can provide its id using
``--template-id``.

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
~~~~~~~~~~~~~~~~~~

Some templates require parameters to properly initialize a new project. You
can check them by listing the templates ``renku template ls --verbose``.

To provide parameters, use the ``--parameter`` option and provide each
parameter using ``--parameter "param1"="value1"``.

.. code-block:: console

    $ renku init --template-id python-minimal --parameter \
    "description"="my new shiny project"

    Initializing new Renku repository... OK

If you don't provide the required parameters through the option
``-parameter``, you will be asked to provide them. Empty values are allowed
and passed to the template initialization function.

.. note::

    Project's ``name`` is considered as a special parameter and it's
    automatically added to the list of parameters forwarded to the ``init``
    command.

Provide custom metadata
~~~~~~~~~~~~~~~~~~~~~~~

Custom metadata can be added to the projects knowledge graph by writing
it to a json file and passing that via the `--metadata` option.

.. code-block:: console

    $ echo '{"@id": "https://example.com/id1", \
        "@type": "https://schema.org/Organization", \
        "https://schema.org/legalName": "ETHZ"}' > metadata.json

    $ renku init --template-id python-minimal --parameter \
    "description"="my new shiny project" --metadata metadata.json

    Initializing new Renku repository... OK


Update an existing project
~~~~~~~~~~~~~~~~~~~~~~~~~~

There are situations when the required structure of a Renku project needs
to be recreated or you have an **existing** Git repository for folder that
you wish to turn into a Renku project. In these cases, Renku will warn you
if there are any files that need to be overwritten. ``README.md`` and
``README.rst`` will never be overwritten. ``.gitignore`` will be appended to
to prevent files accidentally getting committed. Files that are not present
in the template will be left untouched by the command.

.. code-block:: console

    $ echo "# Example\nThis is a README." > README.md
    $ echo "FROM python:3.7-alpine" > Dockerfile
    $ renku init

    INDEX  ID              PARAMETERS
    -------  --------------  ------------
        1  python-minimal  description
        2  R-minimal       description
        3  bioc-minimal    description
        4  julia-minimal   description
        5  minimal
    Please choose a template by typing the index: 1
    The template requires a value for "description": Test Project
    Initializing Git repository...
    Warning: The following files exist in the directory and will be overwritten:
            Dockerfile
    Proceed? [y/N]: y
    Initializing new Renku repository...
    Initializing file .dockerignore ...
    Initializing file .gitignore ...
    Initializing file .gitlab-ci.yml ...
    Initializing file .renku/renku.ini ...
    Initializing file .renkulfsignore ...
    Overwriting file Dockerfile ...
    Initializing file data/.gitkeep ...
    Initializing file environment.yml ...
    Initializing file notebooks/.gitkeep ...
    Initializing file requirements.txt ...
    Project initialized.
    OK

If you initialize in an existing git repository, Renku will create a backup
branch before overwriting any files and will print commands to revert the
changes done and to see what changes were made.

You can also enable the external storage system for output files, if it
was not installed previously.

.. code-block:: console

    $ renku init --external-storage

"""

import json
import os
from pathlib import Path

import click

from renku.command.options import option_external_storage_requested
from renku.core import errors
from renku.ui.cli.utils import color

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


@click.command()
@click.argument("path", default=".", type=click.Path(writable=True, file_okay=False, resolve_path=True))
@click.option("-n", "--name", default=None, help="Provide a custom project name.")
@click.option("--description", help="Provide a description for the project.")
@click.option("-k", "--keyword", default=None, multiple=True, type=click.STRING, help="List of keywords.")
@click.option(
    "--datadir",
    default=None,
    type=click.Path(writable=True, file_okay=False),
    help="Data directory within the project",
)
@click.option("-t", "--template-id", help="Provide the id of the template to use.")
@click.option("-s", "--template-source", help="Provide the templates repository url or path.")
@click.option(
    "-r", "--template-ref", default=None, help="Specify the reference to checkout on remote template repository."
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
@click.option(
    "-m",
    "--metadata",
    "metadata",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Custom metadata to be associated with the project.",
)
@click.option("-l", "--list-templates", is_flag=True, help="List templates available in the template-source.")
@click.option("-d", "--describe", is_flag=True, help="Show description for templates and parameters")
@click.option("--force", is_flag=True, help="Override target path.")
@click.option("--initial-branch", help="Initial git branch to create.")
@option_external_storage_requested
@click.pass_context
def init(
    ctx,
    external_storage_requested,
    path,
    name,
    description,
    keyword,
    template_id,
    template_source,
    template_ref,
    parameters,
    metadata,
    list_templates,
    force,
    describe,
    datadir,
    initial_branch,
):
    """Initialize a project in PATH. Default is the current path."""
    from renku.command.init import init_project_command
    from renku.core.util.git import check_global_git_user_is_configured
    from renku.ui.cli.utils.callback import ClickCallback

    if template_ref and not template_source:
        raise errors.ParameterError("Can't use '--template-ref' without specifying '--template-source'")
    if describe:
        raise errors.ParameterError("'-d/--describe' is deprecated: Use 'renku template show' instead.")
    if list_templates:
        raise errors.ParameterError("'-l/--list-templates' is deprecated: Use 'renku template ls' instead.")

    datadir = resolve_data_directory(datadir, path)

    check_global_git_user_is_configured()

    custom_metadata = None
    if metadata:
        custom_metadata = json.loads(Path(metadata).read_text())

    communicator = ClickCallback()
    init_project_command().with_communicator(communicator).build().execute(
        external_storage_requested=external_storage_requested,
        path=path,
        name=name,
        description=description,
        keywords=keyword,
        template_id=template_id,
        template_source=template_source,
        template_ref=template_ref,
        input_parameters=parameters,
        custom_metadata=custom_metadata,
        force=force,
        data_dir=datadir,
        initial_branch=initial_branch,
        install_mergetool=True,
    )

    click.secho("OK", fg=color.GREEN)
