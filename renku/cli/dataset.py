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
    Creating a dataset ... OK

Listing all datasets:

.. code-block:: console

    $ renku dataset
    ID        NAME           CREATED              CREATORS
    --------  -------------  -------------------  ---------
    0ad1cb9a  some-dataset   2019-03-19 16:39:46  sam
    9436e36c  my-dataset     2019-02-28 16:48:09  sam

Deleting a dataset:

.. code-block:: console

    $ renku dataset rm some-dataset
    OK


Working with data
~~~~~~~~~~~~~~~~~


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

Listing all files in the project associated with a dataset.

.. code-block:: console

    $ renku dataset ls-files
    ADDED                CREATORS    DATASET        PATH
    -------------------  ---------  -------------  ---------------------------
    2019-02-28 16:48:09  sam        my-dataset     ...my-dataset/addme
    2019-02-28 16:49:02  sam        my-dataset     ...my-dataset/weather/file1
    2019-02-28 16:49:02  sam        my-dataset     ...my-dataset/weather/file2
    2019-02-28 16:49:02  sam        my-dataset     ...my-dataset/weather/file3

Sometimes you want to filter the files. For this we use ``--dataset``,
``--include`` and ``--exclude`` flags:

.. code-block:: console

    $ renku dataset ls-files --include "file*" --exclude "file3"
    ADDED                CREATORS    DATASET     PATH
    -------------------  ---------  ----------  ----------------------------
    2019-02-28 16:49:02  sam        my-dataset  .../my-dataset/weather/file1
    2019-02-28 16:49:02  sam        my-dataset  .../my-dataset/weather/file2

Unlink a file from a dataset:

.. code-block:: console

    $ renku dataset unlink my-dataset --include file1
    OK

Unlink all files within a directory from a dataset:

.. code-block:: console

    $ renku dataset unlink my-dataset --include "weather/*"
    OK

Unlink all files from a dataset:

.. code-block:: console

    $ renku dataset unlink my-dataset
    Warning: You are about to remove following from "my-dataset" dataset.
    .../my-dataset/weather/file1
    .../my-dataset/weather/file2
    .../my-dataset/weather/file3
    Do you wish to continue? [y/N]:

.. note:: The ``unlink`` command does not delete files,
    only the dataset record.
"""
import multiprocessing as mp
import os
import tempfile
from collections import OrderedDict
from multiprocessing import RLock, freeze_support
from urllib.parse import ParseResult

import click
import editor
import requests
import yaml
from click import BadParameter
from requests import HTTPError
from tqdm import tqdm

from renku.api._git import COMMIT_DIFF_STRATEGY
from renku.api.datasets import DATASET_METADATA_PATHS
from renku.cli._providers import ProviderFactory
from renku.models._tabulate import tabulate
from renku.models.datasets import Dataset

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
@pass_local_client(
    clean=False, commit=True, commit_only=DATASET_METADATA_PATHS
)
def create(client, name):
    """Create an empty dataset in the current repo."""
    from renku.models.datasets import Creator

    if write_dataset(client, name):

        with client.with_dataset(name=name) as dataset:
            click.echo('Creating a dataset ... ', nl=False)
            creator = Creator.from_git(client.repo)
            if creator not in dataset.creator:
                dataset.creator.append(creator)

        click.secho('OK', fg='green')


@dataset.command()
@click.argument('id')
@pass_local_client(
    clean=False, commit=True, commit_only=DATASET_METADATA_PATHS
)
def edit(client, id):
    """Edit dataset metadata."""
    dataset_ = client.load_dataset(id)

    if dataset_:
        metadata_edited = editor.edit(
            contents=bytes(
                yaml.safe_dump(dataset_.editable), encoding='utf-8'
            )
        )

        edited = yaml.safe_load(metadata_edited)
        updated_ = Dataset(client=client, **edited)

        dataset_.update_metadata(updated_)
        dataset_.to_yaml()


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
@pass_local_client(
    clean=False,
    commit=True,
    commit_only=COMMIT_DIFF_STRATEGY,
)
def add(client, name, urls, link, relative_to, target, force):
    """Add data to a dataset."""
    add_to_dataset(client, urls, name, link, force, relative_to, target)


def add_to_dataset(
    client,
    urls,
    name,
    link=False,
    force=False,
    relative_to=None,
    target=None,
    with_metadata=None
):
    """Adds data to dataset."""
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

            if with_metadata:

                for file_ in with_metadata.files:
                    for added_ in dataset.files:

                        if added_.path.endswith(file_.filename):
                            if isinstance(file_.url, ParseResult):
                                file_.url = file_.url.geturl()

                            file_.path = added_.path
                            file_.creator = with_metadata.creator

                dataset.update_metadata(with_metadata)

    except FileNotFoundError:
        raise BadParameter('Could not process {0}'.format(url))


@dataset.command('ls-files')
@click.argument('names', nargs=-1)
@click.option(
    '--creators',
    help='Filter files which where authored by specific creators. '
    'Multiple creators are specified by comma.'
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
def ls_files(client, names, creators, include, exclude, format):
    """List files in dataset."""
    records = _filter(
        client,
        names=names,
        creators=creators,
        include=include,
        exclude=exclude
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
@pass_local_client(
    clean=False,
    commit=True,
    commit_only=COMMIT_DIFF_STRATEGY,
)
def unlink(client, name, include, exclude, yes):
    """Remove matching files from a dataset."""
    dataset = client.load_dataset(name=name)

    if not dataset:
        raise BadParameter('Dataset does not exist.')

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

        dataset.to_yaml()
        click.secho('OK', fg='green')


@dataset.command('rm')
@click.argument('names', nargs=-1)
@pass_local_client(
    clean=False,
    commit=True,
    commit_only=COMMIT_DIFF_STRATEGY,
)
def remove(client, names):
    """Delete a dataset."""
    from renku.models.refs import LinkReference
    datasets = {name: client.dataset_path(name) for name in names}

    if not datasets:
        raise click.BadParameter(
            'use dataset name or identifier', param_hint='names'
        )

    unknown = [
        name
        for name, path in datasets.items() if not path or not path.exists()
    ]
    if unknown:
        raise click.BadParameter(
            'unknown datasets ' + ', '.join(unknown), param_hint='names'
        )

    datasets = set(datasets.values())

    with progressbar(
        datasets,
        label='Removing metadata files'.ljust(30),
        item_show_func=lambda item: str(item) if item else '',
    ) as bar:
        for dataset in bar:
            if dataset and dataset.exists():
                dataset.unlink()

    with progressbar(
        list(LinkReference.iter_items(client, common_path='datasets')),
        label='Removing aliases'.ljust(30),
        item_show_func=lambda item: item.name if item else '',
    ) as bar:
        for ref in bar:
            if ref.reference in datasets:
                ref.delete()

    click.secho('OK', fg='green')


@dataset.command('export')
@click.argument('id')
@click.argument('provider')
@click.option(
    '-p',
    '--publish',
    is_flag=True,
    help='Automatically publish exported dataset.'
)
@pass_local_client(
    clean=False,
    commit=True,
    commit_only=COMMIT_DIFF_STRATEGY,
)
def export_(client, id, provider, publish):
    """Export data to 3rd party provider."""
    config_key_secret = 'access_token'
    provider_id = provider

    dataset_ = client.load_dataset(id)
    if not dataset_:
        raise BadParameter('Dataset not found.')

    try:
        provider = ProviderFactory.from_id(provider_id)
    except KeyError:
        raise BadParameter('Unknown provider.')

    access_token = client.get_value(provider_id, config_key_secret)
    exporter = provider.get_exporter(dataset_, access_token=access_token)

    if access_token is None:
        text_prompt = 'Before exporting, you must configure an access token\n'
        text_prompt += 'Create one at: {0}\n'.format(
            exporter.access_token_url()
        )
        text_prompt += 'Access token'

        access_token = click.prompt(text_prompt, type=str)
        if access_token is None or len(access_token) == 0:
            raise BadParameter(
                'You must provide an access token for the target provider.'
            )

        client.set_value(provider_id, config_key_secret, access_token)
        exporter.set_access_token(access_token)

    try:
        destination = exporter.export(publish)
    except HTTPError as e:
        if 'unauthorized' in str(e):
            client.remove_value(provider_id, config_key_secret)

        raise BadParameter(e)

    click.secho('Exported to: {0}'.format(destination))
    click.secho('OK', fg='green')


@dataset.command('import')
@click.argument('uri')
@click.option('-n', '--name', help='Dataset name.')
@click.option(
    '-x',
    '--extract',
    is_flag=True,
    help='Extract files before importing to dataset.'
)
@pass_local_client(
    clean=False,
    commit=True,
    commit_only=COMMIT_DIFF_STRATEGY,
)
@click.pass_context
def import_(ctx, client, uri, name, extract):
    """Import data from a 3rd party provider.

    Supported providers: [Zenodo, ]
    """
    provider, err = ProviderFactory.from_uri(uri)
    if err and provider is None:
        raise BadParameter('Could not process {0}.\n{1}'.format(uri, err))

    try:

        record = provider.find_record(uri)
        dataset_ = record.as_dataset(client)
        files_ = dataset_.files

        click.echo(
            tabulate(
                files_,
                headers=OrderedDict((
                    ('checksum', None),
                    ('filename', 'name'),
                    ('size_in_mb', 'size (mb)'),
                    ('filetype', 'type'),
                ))
            )
        )

        text_prompt = 'Do you wish to download this version?'
        if record.is_last_version(uri) is False:
            text_prompt = WARNING + 'Newer version found at {}\n'.format(
                record.links.get('latest_html')
            ) + text_prompt

    except KeyError as e:
        raise BadParameter((
            'Could not process {0}.\n'
            'Unable to fetch metadata due to {1}'.format(uri, e)
        ))

    except LookupError:
        raise BadParameter(
            ('Could not process {0}.\n'
             'URI not found.'.format(uri))
        )

    if files_ and click.confirm(text_prompt):
        data_folder = tempfile.mkdtemp()

        pool_size = min(
            int(os.getenv('RENKU_POOL_SIZE',
                          mp.cpu_count() // 2)), 4
        )

        freeze_support()  # Windows support
        pool = mp.Pool(
            pool_size,
            # Windows support
            initializer=tqdm.set_lock,
            initargs=(RLock(), )
        )

        processing = [
            pool.apply_async(
                download_file, args=(
                    i,
                    extract,
                    data_folder,
                    file_,
                )
            ) for i, file_ in enumerate(files_)
        ]

        for p in processing:
            p.wait()
        pool.close()

        dataset_name = name or dataset_.display_name
        if write_dataset(client, dataset_name):
            add_to_dataset(
                client,
                urls=[str(p) for p in Path(data_folder).glob('*')],
                name=dataset_name,
                with_metadata=dataset_
            )

            click.secho('OK', fg='green')


def download_file(position, extract, data_folder, file, chunk_size=16384):
    """Download a file with progress tracking."""
    local_filename = Path(file.filename).name
    download_to = Path(data_folder) / Path(local_filename)

    def extract_dataset(data_folder_, filename, file_):
        """Extract downloaded dataset."""
        import patoolib
        filepath = data_folder_ / filename
        patoolib.extract_archive(filepath, outdir=data_folder_)
        filepath.unlink()

    def stream_to_file(request):
        """Stream bytes to file."""
        with open(download_to, 'wb') as f_:
            progressbar_ = tqdm(
                total=round(file.filesize * 1e-6, 1),
                position=position,
                desc=file.filename[:32],
                bar_format=(
                    '{percentage:3.0f}% '
                    '{n_fmt}MB/{total_fmt}MB| '
                    '{bar} | {desc}'
                ),
                leave=False,
            )
            bytes_downloaded = 0
            for chunk in request.iter_content(chunk_size=chunk_size):
                if chunk:  # remove keep-alive chunks
                    f_.write(chunk)
                    bytes_downloaded += chunk_size
                    progressbar_.n = float(
                        '{0:.1f}'.format(bytes_downloaded * 1e-6)
                    )
                    progressbar_.update(0)
            progressbar_.close()

        if extract:
            extract_dataset(data_folder, local_filename, file)

    with requests.get(file.url.geturl(), stream=True) as r:
        r.raise_for_status()
        stream_to_file(r)


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


def _filter(client, names=None, creators=None, include=None, exclude=None):
    """Filter dataset files by specified filters.

    :param names: Filter by specified dataset names.
    :param creators: Filter by creators.
    :param include: Include files matching file pattern.
    :param exclude: Exclude files matching file pattern.
    """
    if isinstance(creators, str):
        creators = set(creators.split(','))

    if isinstance(creators, list) or isinstance(creators, tuple):
        creators = set(creators)

    records = []
    for path_, dataset in client.datasets.items():
        if not names or dataset.name in names:
            for file_ in dataset.files:
                file_.dataset = dataset.name
                path_ = file_.full_path.relative_to(client.path)
                match = _include_exclude(path_, include, exclude)

                if creators:
                    match = match and creators.issubset({
                        creator.name
                        for creator in file_.creator
                    })

                if match:
                    records.append(file_)

    return sorted(records, key=lambda file_: file_.added)


def get_datadir():
    """Fetch the current data directory."""
    ctx = click.get_current_context()
    return ctx.meta['renku.datasets.datadir']


def write_dataset(client, name):
    """Check if existing dataset should be overwritten.

    :param client: `LocalClient` instance.
    :param name: Dataset name.
    :return: True is dataset exists and user confirmed overwriting.
    """
    if client.load_dataset(name=name):
        warn_ = WARNING + 'This dataset already exists.'
        click.echo(warn_)
        return click.confirm('Do you wish to overwrite it?')
    return True
