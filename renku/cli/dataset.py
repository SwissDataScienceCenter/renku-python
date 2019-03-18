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
@click.option(
    '-y', '--yes', is_flag=True, help='Confirm unlinking of all files.'
)
@pass_local_client(clean=True, commit=True)
def unlink(client, name, include, exclude, yes):
    """Remove matching files from a dataset."""
    dataset = client.load_dataset(name=name)
    records = _filter(
        client, names=[dataset.name], include=include, exclude=exclude
    )

    if not yes and records:
        prompt_text = (
            'You are about to remove '
            'following from "{0}" dataset.\n'.format(dataset.name) +
            '\n'.join([str(record.full_path)
                       for record in records]) + '\nDo you wish to continue?'
        )
        click.confirm(WARNING + prompt_text, abort=True)

    if records:
        for item in records:
            dataset.unlink_file(item.path)

        client.store_dataset(dataset)
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
