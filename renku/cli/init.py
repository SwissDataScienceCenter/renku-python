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

Starting a Renku project
~~~~~~~~~~~~~~~~~~~~~~~~

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

Updating an existing project
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

import ast
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


def parse_variables(ctx, param, value):
    """Parse template variables to dictionary."""
    try:
        variables = ast.literal_eval(value)
    except ValueError:
        raise_template_error(value)
    if type(variables) is not dict:
        raise_template_error(value)
    return variables


def raise_template_error(value):
    """Raise template error with short explanation."""
    error_info = [
        '{0}'.format(value), 'Tip: a dictionary is expected',
        (
            'Example: --template-variables '
            '\'{ "variable_1": "string", "variable_2": 2 }\''
        )
    ]
    raise errors.ParameterError(
        '\n'.join(error_info), '"--template-variables"'
    )


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
    Template = namedtuple('Template', ['index', 'name', 'description'])
    templates_friendly = [
        Template(
            index=index + 1,
            name=template_elem['name'],
            description=template_elem['description'],
        ) for index, template_elem in enumerate(templates)
    ]

    text = tabulate(
        templates_friendly,
        headers=OrderedDict((
            ('index', 'Number'),
            ('name', 'Name'),
            ('description', 'Description'),
        ))
    )

    if not instructions:
        return text
    return '{0}\nPlease choose a template by typing the number'.format(text)


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
@click.option('--template', help='Provide the name of the template to use.')
@click.option(
    '--template-source', help='Provide the templates repository url or path.'
)
@click.option(
    '--template-ref',
    default='master',
    help='Specify the reference to checkout on remote template repository.',
)
@click.option(
    '--template-variables',
    default='{}',
    callback=parse_variables,
    help=(
        'Provide custom values for template variables. It must be a python '
        'dictionary.\nExample: \'{ "variable_1": "string", "variable_2": 2 }\''
    )
)
@click.option('--description', help='Describe your project.')
@click.option(
    '--print-manifest', is_flag=True, help='Print templates manifest only.'
)
@click.option('--force', is_flag=True, help='Override target path.')
@option_use_external_storage
@pass_local_client
@click.pass_context
def init(
    ctx, client, use_external_storage, path, name, template, template_source,
    template_ref, template_variables, description, print_manifest, force
):
    """Initialize a project in PATH. Default is current path."""
    # verify dirty path
    if not is_path_empty(path) and not force and not print_manifest:
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
    if template:
        template_filtered = [
            template_elem for template_elem in template_manifest
            if template_elem['name'] == template
        ]
        if len(template_filtered) == 1:
            template_data = template_filtered[0]
        else:
            click.echo('The template "{0}" is not available.'.format(template))
            repeat = True

    if print_manifest:
        if template_data:
            click.echo(create_template_sentence([template_data]))
        else:
            click.echo(create_template_sentence(template_manifest))
        return

    if not template or repeat:
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
            create_from_template(
                template_path, client, name, description, template_variables,
                force
            )
        except FileExistsError as e:
            raise click.UsageError(e)

    # Install git hooks
    from .githooks import install
    ctx.invoke(install, force=force)
