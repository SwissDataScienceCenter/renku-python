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

import click
from click import BadParameter

from renku import errors

from .._compat import Path
from ._client import pass_local_client
from ._echo import WARNING, progressbar
from ._format.dataset_files import FORMATS as DATASET_FILES_FORMATS
from ._format.datasets import FORMATS as DATASETS_FORMATS


@click.group(invoke_without_command=True)
@click.option('--revision', default=None)
@click.option('--datadir', default='data', type=click.Path(dir_okay=True))
@click.option(
    '--format',
    type=click.Choice(DATASETS_FORMATS),
    default='tabular',
    help='Choose an output format.'
)
@pass_local_client(clean=False, commit=False)
@click.pass_context
def dataset(ctx, client, revision, datadir, format):
    """Handle datasets."""
    ctx.meta['renku.datasets.datadir'] = datadir

    if ctx.invoked_subcommand is not None:
        return

    if revision is None:
        datasets = client.datasets.values()
    else:
        datasets = client.datasets_from_commit(client.repo.commit(revision))

    DATASETS_FORMATS[format](client, datasets)


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
@click.option('--link', is_flag=True, help='Creates a hard link.')
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
def add(client, name, urls, link, relative_to, target, force):
    """Add data to a dataset."""
    try:
        with client.with_dataset(name=name) as dataset:
            target = target if target else None
            with progressbar(urls, label='Adding data to dataset') as bar:
                for url in bar:
                    client.add_data_to_dataset(
                        dataset,
                        url,
                        link=link,
                        target=target,
                        relative_to=relative_to,
                        force=force,
                    )
    except FileNotFoundError:
        raise BadParameter('Could not process {0}'.format(url))


@dataset.command('ls-files')
@click.argument('names', nargs=-1)
@click.option(
    '--authors',
    help='Filter files which where authored by specific authors. '
    'Multiple authors are specified by comma.'
)
@click.option(
    '-I',
    '--include',
    default=None,
    multiple=True,
    help='Include files matching given pattern.'
)
@click.option(
    '-X',
    '--exclude',
    default=None,
    multiple=True,
    help='Exclude files matching given pattern.'
)
@click.option(
    '--format',
    type=click.Choice(DATASET_FILES_FORMATS),
    default='tabular',
    help='Choose an output format.'
)
@pass_local_client(clean=False, commit=False)
def ls_files(client, names, authors, include, exclude, format):
    """List files in dataset."""
    records = _filter(
        client, names=names, authors=authors, include=include, exclude=exclude
    )

    DATASET_FILES_FORMATS[format](client, records)


@dataset.command()
@click.argument('name')
@click.option(
    '-I',
    '--include',
    multiple=True,
    help='Include files matching given pattern.'
)
@click.option(
    '-X',
    '--exclude',
    multiple=True,
    help='Exclude files matching given pattern.'
)
@click.option('--verbose', '-v', is_flag=True, help='Display deleted files.')
@click.option(
    '-y', '--yes', is_flag=True, help='Confirm unlinking of all files.'
)
@click.option(
    '-D', '--delete', is_flag=True, help='Remove files from filesystem.'
)
@pass_local_client(clean=True, commit=True)
def unlink(client, delete, yes, verbose, exclude, include, name):
    """Remove matching files from a dataset."""
    dataset = client.load_dataset(name=name)
    prompt = None if yes else remove_confirm_prompt
    unlinked = unlink_files(
        dataset,
        to_unlink(dataset, include=include, exclude=exclude),
        prompt=prompt
    )
    client.store_dataset(dataset)

    if verbose:
        for path_, df_ in unlinked.items():
            click.secho(
                'Unlinked {0} from "{1}" dataset.'.format(
                    path_,
                    df_[0].name,
                )
            )

        click.secho(
            'Unlinked {0} file{1}.'.format(
                len(unlinked),
                '' if len(unlinked) == 1 else 's',
            )
        )

    if delete:
        deleted = []
        related_files = check_related_files(client, dataset, unlinked)
        for path_, file_ in related_files.items():
            ds_ = client.load_dataset(name=file_[0].name)

            try:
                related_unlinked = unlink_files(
                    ds_, {path_: file_}, prompt=remove_related_file_prompt
                )
                if related_unlinked:
                    deleted += delete_files(related_unlinked)
                    client.store_dataset(ds_)
            except click.Abort:
                unlinked.pop(path_)

        deleted += delete_files(unlinked)

        if verbose:
            for dataset_, df_ in deleted:
                click.secho(
                    'Deleted {0} from "{1}" dataset.'.format(
                        df_.full_path,
                        dataset_.name,
                    )
                )
                click.secho(
                    'Deleted {0} file{1}.'.format(
                        len(deleted),
                        '' if len(deleted) == 1 else 's',
                    )
                )

    click.secho('OK', fg='green')


def _include_exclude(file_path, include=None, exclude=None):
    """Check if file matches one of include filters and not in exclude filter.

    :param file_path: Path to the file.
    :param include: Tuple containing patterns to which include from result.
    :param exclude: Tuple containing patterns to which exclude from result.
    """
    if exclude is not None and exclude:
        for pattern in exclude:
            if file_path.match(pattern):
                return False

    if include is not None and include:
        for pattern in include:
            if file_path.match(pattern):
                return True
        return False

    return True


def _filter(client, names=None, authors=None, include=None, exclude=None):
    """Filter dataset files by specified filters.

    :param names: Filter by specified dataset names.
    :param authors: Filter by authors.
    :param include: Include files matching file pattern.
    :param exclude: Exclude files matching file pattern.
    """
    if isinstance(authors, str):
        authors = set(authors.split(','))

    if isinstance(authors, list) or isinstance(authors, tuple):
        authors = set(authors)

    records = []
    for path_, dataset in client.datasets.items():
        if not names or dataset.name in names:
            for file_ in dataset.files.values():
                file_.dataset = dataset.name

                path_ = file_.full_path.relative_to(client.path)
                match = _include_exclude(path_, include, exclude)

                if authors:
                    match = match and authors.issubset({
                        author.name
                        for author in file_.authors
                    })

                if match:
                    records.append(file_)

    return sorted(records, key=lambda file_: file_.added)


def get_datadir():
    """Fetch the current data directory."""
    ctx = click.get_current_context()
    return ctx.meta['renku.datasets.datadir']


def remove_confirm_prompt(data_to_unlink, dataset):
    """Shows confirm prompt and awaits for user input.

    :param data_to_unlink: List of files which we are removing.
    :param dataset: Dataset instance from which files are removed.
    :return: Boolean representing user confirmation.
    """
    prompt_text = (
        WARNING + 'You are about to remove following '
        'file{0} from "{1}" dataset.'
        '\n'.format('' if len(data_to_unlink) == 1 else 's', dataset.name) +
        '\n'.join([str(path_)
                   for path_ in data_to_unlink]) + '\nDo you wish to proceed?'
    )

    return click.confirm(prompt_text)


def remove_related_file_prompt(files, dataset):
    """Shows confirm prompt and awaits for user input.

    :param path_: Absolute path of the file which is being removed.
    :param dataset: Dataset instance from which file is being removed.
    :return: Boolean representing user confirmation.
    """
    prompt_text = (
        '\n' + WARNING + 'File you are removing is also '
        'part of "{0}" dataset.\n'
        'You are about to remove following '
        'file from "{0}" dataset.\n' +
        '\n'.join([str(path_)
                   for path_ in files]) + '\nDo you wish to proceed?'
    )

    return click.confirm(prompt_text.format(dataset.name))


def check_related_files(client, dataset, files):
    """Check which files are related.

    :param client: Client instance.
    :param dataset: Dataset instance.
    :param files: Files which are check for relationships.
    """
    related = {}
    index = client.file_dataset_index()
    for _, df_ in files.copy().values():
        for dataset_, file_ in index.get(df_.full_path, []):
            if dataset_.identifier == dataset.identifier:
                continue
            related[df_.full_path] = (dataset_, file_)

    return related


def to_unlink(dataset, include=None, exclude=None):
    """Determine which files to unlink from dataset based on filters.

    :raises`errors.ResourceNotFound`: If no matching files are found.
    :param dataset: Dataset from which we are removing files.
    :param include: Unlink files matching the include pattern.
    :param exclude: Keep files matching the exclude pattern.
    """
    data = {
        file_.full_path: (dataset, file_)
        for path_, file_ in dataset.files.items()
        if _include_exclude(Path(path_), include, exclude)
    }

    if not data:
        raise errors.ResourceNotFound(resource_type='file')

    return data


def unlink_files(dataset, files, prompt=None):
    """Unlinks files from dataset.

    :raises click.Abort: If user does not confirms deletion.

    :param dataset: Dataset instance from which files are deleted.
    :param files: Files to be unlinked.
    :param prompt: User confirmation prompt.
    """
    if callable(prompt) and not prompt(files, dataset):
        raise click.Abort

    for path_, data_file in files.items():
        dataset.unlink_file(data_file[1].path)

    return files


def delete_files(files):
    """Delete files from filesystem.

    :param files: Collection of files to be deleted.
    :return: Successfully deleted files.
    """
    deleted = []
    remove_paths = [str(path_) for path_ in files.keys()]
    label = 'Removing files from dataset'
    with progressbar(remove_paths, label=label) as pb_files:
        for path_ in pb_files:
            path_ = Path(path_)
            df_ = files.get(path_, None)
            if df_ and path_.exists():
                path_.unlink()
                deleted.append(df_)
    return deleted
