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

If you don't provide the required parameters, the template will use an empty
strings instead.

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
import pkg_resources
from git import Repo

from renku.core import errors
from renku.core.commands.client import pass_local_client
from renku.core.commands.git import set_git_home
from renku.core.commands.init import create_from_template, fetch_template, \
    read_template_manifest
from renku.core.commands.options import option_use_external_storage
from renku.core.models.tabulate import tabulate

_GITLAB_CI = '.gitlab-ci.yml'
_DOCKERFILE = 'Dockerfile'
_REQUIREMENTS = 'requirements.txt'
CI_TEMPLATES = [_GITLAB_CI, _DOCKERFILE, _REQUIREMENTS]


def parse_parameters(ctx, param, value):
    """Parse parameters to dictionary."""
    parameters = {}
    for parameter in value:
        splitted = parameter.split('=', 1)
        if len(splitted) < 2 or len(splitted[0]) < 1:
            raise errors.ParameterError(
                'Parameter format must be --parameter "param1"="value". ',
                f'--parameter "{parameter}"'
            )
        parameters[splitted[0]] = splitted[1]
    return parameters


def validate_name(ctx, param, value):
    """Validate a project name."""
    if not value:
        value = os.path.basename(ctx.params['path'].rstrip(os.path.sep))
    return value


def store_directory(value):
    """Store directory as a new Git home."""
    Path(value).mkdir(parents=True, exist_ok=True)
    set_git_home(value)
    return value


def create_template_sentence(templates, instructions=False):
    """Create templates choice sentence.

    :ref templates: list of templates coming from manifest file
    :ref instructions: add instructions
    """
    Template = namedtuple(
        'Template', ['index', 'id', 'description', 'variables']
    )
    templates_friendly = [
        Template(
            index=index + 1,
            id=template_elem['folder'],
            description=(
                f'{template_elem["name"]}: {template_elem["description"]}'
            ),
            variables='\n'.join([
                f'{variable[0]}: {variable[1]}'
                for variable in template_elem.get('variables', {}).items()
            ])
        ) for index, template_elem in enumerate(templates)
    ]

    text = tabulate(
        templates_friendly,
        headers=OrderedDict((
            ('index', 'Index'),
            ('id', 'Id'),
            ('description', 'Description'),
            ('variables', 'Parameters'),
        ))
    )

    if not instructions:
        return text
    return '{0}\nPlease choose a template by typing the index'.format(text)


def is_path_empty(path):
    """Check if path contains files.

    :ref path: target path
    """
    gen = Path(path).glob('**/*')
    return not any(gen)


def check_git_user_config():
    """Check that git user information is configured."""
    dummy_git_folder = mkdtemp()
    repo = Repo.init(dummy_git_folder)
    git_config = repo.config_reader()
    try:
        git_config.get_value('user', 'name', None)
        git_config.get_value('user', 'email', None)
        return True
    except (configparser.NoOptionError, configparser.NoSectionError):
        return False


@click.command()
@click.argument(
    'path',
    default='.',
    type=click.Path(writable=True, file_okay=False, resolve_path=True),
)
@click.option(
    '--name',
    callback=validate_name,
    help='Provide a custom project name.',
)
@click.option(
    '-t', '--template-id', help='Provide the id of the template to use.'
)
@click.option(
    '-i',
    '--template-index',
    help='Provide the index number of the template to use.',
    type=int,
)
@click.option(
    '-s',
    '--template-source',
    help='Provide the templates repository url or path.'
)
@click.option(
    '-r',
    '--template-ref',
    default='master',
    help='Specify the reference to checkout on remote template repository.',
)
@click.option(
    '-p',
    '--parameter',
    multiple=True,
    type=click.STRING,
    callback=parse_parameters,
    help=(
        'Provide parameters value. Should be invoked once per parameter. '
        'Please specify the values as follow: --parameter "param1"="value"'
    )
)
@click.option(
    '-l',
    '--list-templates',
    is_flag=True,
    help='List templates available in the template-source.'
)
@click.option('--force', is_flag=True, help='Override target path.')
@option_use_external_storage
@pass_local_client
@click.pass_context
def init(
    ctx, client, use_external_storage, path, name, template_id, template_index,
    template_source, template_ref, parameter, list_templates, force
):
    """Initialize a project in PATH. Default is current path."""
    # verify dirty path
    if not is_path_empty(path) and not force and not list_templates:
        raise errors.InvalidFileOperation(
            'Folder "{0}" is not empty. Please add --force '
            'flag to transform it into a Renku repository.'.format(str(path))
        )

    if not check_git_user_config():
        raise errors.ConfigurationError(
            'The user name and email are not configured. '
            'Please use the "git config" command to configure them.\n\n'
            '\tgit config --global --add user.name "John Doe"\n'
            '\tgit config --global --add user.email '
            '"john.doe@example.com"\n'
        )

    # select template source
    if template_source:
        click.echo(
            'Fetching template from {0}@{1}... '.format(
                template_source, template_ref
            ),
            nl=False
        )
        template_folder = Path(mkdtemp())
        fetch_template(template_source, template_ref, template_folder)
        template_manifest = read_template_manifest(
            template_folder, checkout=True
        )
        click.secho('OK', fg='green')
    else:
        template_folder = Path(
            pkg_resources.resource_filename('renku', 'templates')
        )
        template_manifest = read_template_manifest(template_folder)

    # select specific template
    repeat = False
    template_data = None
    if template_id:
        if template_index:
            raise errors.ParameterError(
                'Use either --template-id or --template-index, not both',
                '"--template-index"'
            )
        template_filtered = [
            template_elem for template_elem in template_manifest
            if template_elem['folder'] == template_id
        ]
        if len(template_filtered) == 1:
            template_data = template_filtered[0]
        else:
            click.echo(
                f'The template with id "{template_id}" is not available.'
            )
            repeat = True

    if template_index or template_index == 0:
        if template_index > 0 and template_index <= len(template_manifest):
            template_data = template_manifest[template_index - 1]
        else:
            click.echo(
                f'The template at index {template_index} is not available.'
            )
            repeat = True

    if list_templates:
        if template_data:
            click.echo(create_template_sentence([template_data]))
        else:
            click.echo(create_template_sentence(template_manifest))
        return

    if repeat or not (template_id or template_index):
        templates = [template_elem for template_elem in template_manifest]
        if len(templates) == 1:
            template_data = templates[0]
        else:
            template_num = click.prompt(
                text=create_template_sentence(templates, True),
                type=click.IntRange(1, len(templates)),
                show_default=False,
                show_choices=False
            )
            template_data = templates[template_num - 1]

    # set local path and storage
    store_directory(path)
    if not client.use_external_storage:
        use_external_storage = False
    ctx.obj = client = attr.evolve(
        client, path=path, use_external_storage=use_external_storage
    )
    if not is_path_empty(path):
        from git import GitCommandError
        try:
            commit = client.find_previous_commit('*')
            branch_name = 'pre_renku_init_{0}'.format(commit.hexsha[:7])
            with client.worktree(
                path=path,
                branch_name=branch_name,
                commit=commit,
                merge_args=[
                    '--no-ff', '-s', 'recursive', '-X', 'ours',
                    '--allow-unrelated-histories'
                ]
            ):
                click.echo(
                    'Saving current data in branch {0}'.format(branch_name)
                )
        except AttributeError:
            click.echo('Warning! Overwriting non-empty folder.')
        except GitCommandError as e:
            click.UsageError(e)

    # clone the repo
    template_path = template_folder / template_data['folder']
    click.echo('Initializing new Renku repository... ', nl=False)
    with client.lock:
        try:
            create_from_template(template_path, client, name, parameter, force)
        except FileExistsError as e:
            raise click.UsageError(e)

    # Install git hooks
    from .githooks import install
    ctx.invoke(install, force=force)
