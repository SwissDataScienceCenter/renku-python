# -*- coding: utf-8 -*-
#
# Copyright 2017-2019 - Swiss Data Science Center (SDSC)
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
r"""Work with datasets in the current repository.

Manipulating datasets
~~~~~~~~~~~~~~~~~~~~~

Creating an empty dataset inside a Renku project:

.. code-block:: console

    $ renku dataset create my-dataset

Adding data to the dataset:

.. code-block:: console

    $ renku dataset add my-dataset http://data-url

This will copy the contents of ``data-url`` to the dataset and add it
to the dataset metadata.

To add data from a git repository, you can specify it via https or git+ssh
URL schemes. For example,

.. code-block:: console

    $ renku dataset add my-dataset git+ssh://host.io/namespace/project.git

Sometimes you want to import just a specific path within the parent project.
In this case, use the ``--target`` flag:

.. code-block:: console

    $ renku dataset add my-dataset --target relative-path/datafile \
        git+ssh://host.io/namespace/project.git

To trim part of the path from the parent directory, use the ``--relative-to``
option. For example, the command above will result in a structure like

.. code-block:: console

    data/
      my-dataset/
        relative-path/
          datafile

Using instead

.. code-block:: console

    $ renku dataset add my-dataset \
        --target relative-path/datafile \
        --relative-to relative-path \
        git+ssh://host.io/namespace/project.git

will yield:

.. code-block:: console

    data/
      my-dataset/
        datafile
"""
from collections import OrderedDict

import click
from click import BadParameter

from renku._compat import Path
from renku.cli._checks.files_in_datasets import file_in_datasets
from renku.models._tabulate import tabulate
from renku.models.datasets import Author

from ._client import pass_local_client
from ._echo import WARNING, progressbar


@click.group(invoke_without_command=True)
@click.option('--datadir', default='data', type=click.Path(dir_okay=True))
@pass_local_client(clean=False, commit=False)
@click.pass_context
def dataset(ctx, client, datadir):
    """Handle datasets."""
    ctx.meta['renku.datasets.datadir'] = datadir

    if ctx.invoked_subcommand is not None:
        return

    output = tabulate(
        client.datasets.values(),
        headers=OrderedDict((('short_id', 'id'), ('name', None),
                             ('created', None), ('authors_csv', 'authors'))),
    )
    click.echo(output)


@dataset.command('ls-files')
@click.argument('name')
@pass_local_client(clean=False, commit=False)
def ls_files(client, name):
    """List files in dataset."""
    with client.with_dataset(name=name) as dataset:
        output = tabulate(
            sorted(dataset.files.values(), key=lambda file_: file_.added),
            headers=OrderedDict((
                ('added', None),
                ('authors_csv', 'authors'),
                ('filename', None),
            )),
        )
        click.echo(output)


@dataset.command()
@click.argument('name')
@pass_local_client(clean=True, commit=True)
def create(client, name):
    """Create an empty dataset in the current repo."""
    with client.with_dataset(name=name) as dataset:
        click.echo('Creating a dataset ... ', nl=False)
        author = Author.from_git(client.repo)
        if author not in dataset.authors:
            dataset.authors.append(author)

    click.secho('OK', fg='green')


@dataset.command()
@click.argument('name')
@click.option(
    '--force',
    default=False,
    is_flag=True,
    help='Delete dataset with existing files.',
)
@click.option('--verbose', '-v', is_flag=True)
@pass_local_client(clean=True, commit=True)
def delete(client, name, force, verbose):
    """Delete a dataset."""
    with client.with_dataset(name=name) as dataset:
        client.delete_dataset(dataset, force=force)

        if dataset.files:
            unlinked = delete_files_with_progress(
                client, dataset, include=('*', )
            )

        click.secho('OK', fg='green')
        if verbose:
            if len(unlinked) > 0:
                click.secho(
                    '\nDeleted {0} file{1}.'.format(
                        len(unlinked), '' if len(unlinked) == 1 else 's'
                    )
                )
            else:
                click.secho('No files to delete.')


@dataset.command()
@click.argument('name')
@click.argument('urls', nargs=-1)
@click.option('nocopy', '--copy/--no-copy', default=False, is_flag=True)
@click.option('--relative-to', default=None)
@click.option(
    '-t',
    '--target',
    default=None,
    multiple=True,
    help='Target path in the git repo.'
)
@click.option(
    '--force', is_flag=True, help='Allow adding otherwise ignored files.'
)
@pass_local_client(clean=True, commit=True)
def add(client, name, urls, nocopy, relative_to, target, force):
    """Add data to a dataset."""
    try:
        with client.with_dataset(name=name) as dataset:
            target = target if target else None
            with progressbar(urls, label='Adding data to dataset') as bar:
                for url in bar:
                    client.add_data_to_dataset(
                        dataset,
                        url,
                        nocopy=nocopy,
                        target=target,
                        relative_to=relative_to,
                        force=force,
                    )
    except FileNotFoundError:
        raise BadParameter('Could not process {0}'.format(url))


@dataset.command()
@click.argument('name')
@click.option('--include', '-I', default='*', multiple=True)
@click.option('--exclude', '-X', multiple=True)
@click.option('--verbose', '-v', is_flag=True)
@click.option('--yes', '-y', is_flag=True)
@pass_local_client(clean=True, commit=True)
def unlink(client, yes, verbose, exclude, include, name):
    """Removes a file from dataset."""
    with client.with_dataset(name=name) as dataset:
        prompt = None if yes else delete_confirm_prompt
        unlinked = delete_files_with_progress(
            client, dataset, include=include, exclude=exclude, prompt=prompt
        )

        click.secho('OK', fg='green')

        if verbose:
            click.secho(
                '\nDeleted {0} file{1}.'.format(
                    len(unlinked), '' if len(unlinked) == 1 else 's'
                )
            )
            for absolute_path, _ in unlinked:
                click.secho(absolute_path)


def get_datadir():
    """Fetch the current data directory."""
    ctx = click.get_current_context()
    return ctx.meta['renku.datasets.datadir']


def delete_confirm_prompt(files_to_unlink, dataset):
    """Shows confirm prompt and awaits for user input.

    :param files_to_unlink: List of files which we are deleting.
    :param dataset_name: Name of the dataset from which we are deleting files.
    """
    prompt_text = 'Deleting:\n'
    prompt_text += '\n'.join([absolute for absolute, file_ in files_to_unlink])
    prompt_text += '\nYou are about to delete {0} ' \
                   'file{1} from {2} dataset.\n\n' \
                   'Do you wish to proceed?' \
                   ''.format(len(files_to_unlink),
                             '' if len(files_to_unlink) == 1 else 's',
                             dataset.name)

    return click.confirm(WARNING + prompt_text)


def delete_files_with_progress(
    client, dataset, include=None, exclude=None, prompt=None
):
    """Delete files from dataset and tracks progress of deletion.

    :raises click.Abort: If user does not confirms deletion.
    :param client: LocalClient instance.
    :param dataset: Dataset from which we are deleting files.
    :param include: Remove files matching the include pattern.
    :param exclude: Keep files matching the exclude pattern.
    :param prompt: Delete confirmation prompt.
    """
    unlinked_files = client.unlink_files(
        dataset, include=include, exclude=exclude
    )

    if callable(prompt) and not prompt(unlinked_files, dataset):
        raise click.Abort

    remove_paths = []
    for absolute, file_ in unlinked_files:
        other_datasets = file_in_datasets(
            client, file_, exclude_dataset=dataset
        )
        exist_in_other = []
        for dataset_ in other_datasets:
            if delete_confirm_prompt([absolute], dataset_):
                dataset_.remove_file(file_.path)
            else:
                exist_in_other.append(dataset_)

        if len(exist_in_other) == 0:
            remove_paths.append(absolute)

    label = 'Removing files from dataset'
    with progressbar(remove_paths, label=label) as files:
        for file_ in files:
            Path(file_).unlink()

    return unlinked_files
