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

from fnmatch import fnmatch

import click
from click import BadParameter

from renku._compat import Path
from renku.api.datasets import check_same_paths

from ._client import pass_local_client
from ._echo import WARNING, progressbar
from ._format.dataset_files import FORMATS as DATASET_FILES_FORMATS
from ._format.datasets import FORMATS as DATASETS_FORMATS


@click.group(invoke_without_command=True)
@click.option('--datadir', default='data', type=click.Path(dir_okay=True))
@click.option(
    '--format',
    type=click.Choice(DATASETS_FORMATS),
    default='tabular',
    help='Choose an output format.'
)
@pass_local_client(clean=False, commit=False)
@click.pass_context
def dataset(ctx, client, datadir, format):
    """Handle datasets."""
    ctx.meta['renku.datasets.datadir'] = datadir

    if ctx.invoked_subcommand is not None:
        return

    DATASETS_FORMATS[format](client)


@dataset.command()
@click.argument('name')
@pass_local_client(clean=True, commit=True)
def create(client, name):
    """Create an empty dataset in the current repo."""
    from renku.models.datasets import Author

    with client.with_dataset(name=name) as dataset:
        click.echo('Creating a dataset ... ', nl=False)
        author = Author.from_git(client.repo)
        if author not in dataset.authors:
            dataset.authors.append(author)

    click.secho('OK', fg='green')


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


@dataset.command('ls-files')
@click.option(
    '--dataset', multiple=True, help='Filter files in specific dataset.'
)
@click.option(
    '--authors',
    help='Filter files which where authored by specific authors. '
    'Multiple authors are specified by comma.'
)
@click.option(
    '--include',
    default='*',
    multiple=True,
    help='Include files matching given pattern.'
)
@click.option(
    '--exclude', multiple=True, help='Exclude files matching given pattern.'
)
@click.option(
    '--format',
    type=click.Choice(DATASET_FILES_FORMATS),
    default='tabular',
    help='Choose an output format.'
)
@pass_local_client(clean=False, commit=False)
def ls_files(client, format, exclude, include, authors, dataset):
    """List files in dataset."""
    records = _filter(
        client.datasets,
        dataset_names=dataset,
        authors=authors,
        include=include,
        exclude=exclude
    )

    DATASET_FILES_FORMATS[format](client, records)


@dataset.command()
@click.argument('name')
@click.option(
    '--include',
    '-I',
    default='*',
    multiple=True,
    help='Include files matching given pattern.'
)
@click.option(
    '--exclude',
    '-X',
    multiple=True,
    help='Exclude files matching given pattern.'
)
@click.option('--verbose', '-v', is_flag=True, help='Display deleted files.')
@click.option(
    '--yes', '-y', is_flag=True, help='Confirm unlinking of all files.'
)
@click.option('--delete', is_flag=True, help='Remove files from filesystem.')
@pass_local_client(clean=True, commit=True)
def unlink(client, delete, yes, verbose, exclude, include, name):
    """Removes a file from dataset."""
    with client.with_dataset(name=name) as dataset:
        prompt = None if yes else delete_confirm_prompt
        unlinked, deleted = unlink_files(
            client,
            dataset,
            delete=delete,
            include=include,
            exclude=exclude,
            prompt=prompt
        )

        click.secho('OK', fg='green')

        if verbose:
            for path_, data_file in unlinked.items():
                click.secho(
                    'Unlinked {0} from "{1}" dataset.'.format(
                        path_,
                        data_file.dataset,
                    )
                )
            for path_, data_file in deleted.items():
                click.secho(
                    'Deleted {0} from "{1}" dataset.'.format(
                        path_,
                        data_file.dataset,
                    )
                )

            click.secho(
                'Unlinked {0} file{1}.'.format(
                    len(unlinked),
                    '' if len(unlinked) == 1 else 's',
                )
            )
            click.secho(
                'Deleted {0} file{1}.'.format(
                    len(deleted),
                    '' if len(deleted) == 1 else 's',
                )
            )


def _include_exclude(file_path, include, exclude=None):
    """Check if file matches one of include filters and not in exclude filter.

    :param file_path: Path to the file.
    :param include: Tuple containing patterns to which include from result.
    :param exclude: Tuple containing patterns to which exclude from result.
    """
    filename = Path(file_path).name

    for pattern in exclude:
        if fnmatch(filename, pattern):
            return False

    found = False
    for pattern in include:
        found = found or fnmatch(filename, pattern)

    return found


def _filter(
    datasets, dataset_names=None, authors=None, include=None, exclude=None
):
    """Filter dataset files by specified filters.

    :param dataset_names: Filter by specified dataset names.
    :param authors: Filter by authors.
    :param include: Include files matching file pattern.
    :param exclude: Exclude files matching file pattern.
    """
    if isinstance(authors, str):
        authors = set(authors.split(','))

    if isinstance(authors, list) or isinstance(authors, tuple):
        authors = set(authors)

    records = []
    for path_, dataset in datasets.items():
        if dataset.name in dataset_names or not dataset_names:
            for file_ in dataset.files.values():
                file_.dataset = dataset.name
                match = _include_exclude(file_.filename, include, exclude)

                if authors:
                    match = match and authors.issubset(
                        set([author.name for author in file_.authors])
                    )

                if match:
                    records.append(file_)

    return sorted(records, key=lambda file_: file_.added)


def delete_confirm_prompt(data_to_unlink, dataset):
    """Shows confirm prompt and awaits for user input.

    :param data_to_unlink: List of files which we are removing.
    :param dataset_name: Name of the dataset from which files are removed.
    :return: Boolean representing user confirmation.
    """
    prompt_text = 'You are about to remove following ' \
                  'file{0} from "{1}" dataset.' \
                  '\n'.format('' if len(data_to_unlink) == 1 else 's',
                              dataset.name)

    prompt_text += '\n'.join([
        absolute for absolute, file_ in data_to_unlink.items()
    ])

    prompt_text += '\nDo you wish to proceed?'
    return click.confirm(WARNING + prompt_text)


def same_files_prompt(path_, file_):
    """Shows confirm prompt and awaits for user input.

    :param path_: Absolute path of the file which is being removed.
    :param file_: DatasetFile instance which is being removed.
    :return: Boolean representing user confirmation.
    """
    prompt_text = 'File you are removing is also ' \
                  'part of "{1}" dataset.\n' \
                  'You are about to remove following ' \
                  'file from "{1}" dataset.\n' \
                  '{0}\n' \
                  'Do you wish to proceed?'.format(path_, file_.dataset)

    return click.confirm(WARNING + prompt_text)


def unlink_files(
    client, dataset, delete=False, include=None, exclude=None, prompt=None
):
    """Removes files from dataset.

    :raises click.Abort: If user does not confirms deletion.

    :param client: LocalClient instance.
    :param dataset: Dataset instance from which we are deleting files.
    :param delete: Flag indicating if files should be removed from filesystem.
    :param include: Remove files matching the include pattern.
    :param exclude: Keep files matching the exclude pattern.
    """
    data_to_unlink = client.data_to_unlink(
        dataset, include=include, exclude=exclude
    )

    if callable(prompt) and not prompt(data_to_unlink, dataset):
        raise click.Abort

    unlinked, removed = {}, {}
    same_paths = check_same_paths(client, data_to_unlink)
    for path_, file_ in same_paths.items():
        if not same_files_prompt(path_, file_):
            if dataset.unlink_file(file_.path):
                unlinked[path_] = data_to_unlink.pop(path_)

    remove_paths = [path_ for path_ in data_to_unlink.keys()]
    label = 'Removing files from dataset'
    with progressbar(remove_paths, label=label) as files:
        for path_ in files:
            data_file = data_to_unlink.get(path_, None)

            if data_file:
                if dataset.unlink_file(data_file.path):
                    unlinked[path_] = data_file
                if delete:
                    Path(path_).unlink()
                    removed[path_] = data_file

    return unlinked, removed


def get_datadir():
    """Fetch the current data directory."""
    ctx = click.get_current_context()
    return ctx.meta['renku.datasets.datadir']
