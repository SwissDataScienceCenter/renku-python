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
"""Repository datasets management."""

import multiprocessing as mp
import os
import re
import tempfile
from collections import OrderedDict
from contextlib import contextmanager
from multiprocessing import RLock, freeze_support
from pathlib import Path
from time import sleep
from urllib.parse import ParseResult

import click
import requests
import yaml
from click import BadParameter
from requests import HTTPError
from tqdm import tqdm

from renku.core.commands.checks.migration import check_dataset_resources, \
    dataset_pre_0_3
from renku.core.commands.format.dataset_tags import DATASET_TAGS_FORMATS
from renku.core.commands.providers import ProviderFactory
from renku.core.errors import DatasetNotFound, InvalidAccessToken, \
    MigrationRequired
from renku.core.management.datasets import DATASET_METADATA_PATHS
from renku.core.management.git import COMMIT_DIFF_STRATEGY
from renku.core.models.datasets import Creator, Dataset
from renku.core.models.refs import LinkReference
from renku.core.models.tabulate import tabulate

from .client import pass_local_client
from .echo import WARNING, progressbar
from .format.dataset_files import DATASET_FILES_FORMATS
from .format.datasets import DATASETS_FORMATS


@pass_local_client(clean=False, commit=False)
def dataset_parent(client, revision, datadir, format, ctx=None):
    """Handle datasets subcommands."""
    missing_dataset, missing_files = check_dataset_resources(client)
    old_datasets = [ds for ds in dataset_pre_0_3(client)]

    if missing_dataset or missing_files or old_datasets:
        raise MigrationRequired('datasets')

    if revision is None:
        datasets = client.datasets.values()
    else:
        datasets = client.datasets_from_commit(client.repo.commit(revision))

    return DATASETS_FORMATS[format](client, datasets)


@pass_local_client(
    clean=False, commit=True, commit_only=DATASET_METADATA_PATHS
)
def create_dataset(client, name, handle_duplicate_fn=None):
    """Create an empty dataset in the current repo.

    :raises: ``click.BadParameter``
    """
    if handle_duplicate_fn and handle_duplicate_fn(client, name):
        with client.with_dataset(name=name) as dataset:
            creator = Creator.from_git(client.repo)
            if creator not in dataset.creator:
                dataset.creator.append(creator)


@pass_local_client(
    clean=False, commit=True, commit_only=DATASET_METADATA_PATHS
)
def edit_dataset(client, dataset_id, transform_fn):
    """Edit dataset metadata."""
    dataset = client.load_dataset(dataset_id)

    if not dataset:
        raise DatasetNotFound()

    edited = yaml.safe_load(transform_fn(dataset))
    updated_ = Dataset(client=client, **edited)
    dataset.update_metadata(updated_)
    dataset.to_yaml()


@pass_local_client(
    clean=False,
    commit=True,
    commit_only=COMMIT_DIFF_STRATEGY,
)
def add_file(
    client,
    urls,
    name,
    link=False,
    force=False,
    relative_to=None,
    target=None,
    with_metadata=None
):
    """Add data file to a dataset."""
    add_to_dataset(
        client, urls, name, link, force, relative_to, target, with_metadata
    )


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
    """Add data to a dataset."""
    try:
        with client.with_dataset(name=name) as dataset:
            target = target if target else None
            with progressbar(urls, label='Adding data to dataset') as bar:
                client.add_data_to_dataset(
                    dataset,
                    bar,
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
                            file_._label = added_._label
                            file_.commit = added_.commit

                dataset.update_metadata(with_metadata)

    except FileNotFoundError:
        raise BadParameter('Could not process \n{0}'.format('\n'.join(urls)))


@pass_local_client(clean=False, commit=False)
def list_files(client, names, creators, include, exclude, format):
    """List files in dataset."""
    records = _filter(
        client,
        names=names,
        creators=creators,
        include=include,
        exclude=exclude
    )

    return DATASET_FILES_FORMATS[format](client, records)


@pass_local_client(
    clean=False,
    commit=True,
    commit_only=COMMIT_DIFF_STRATEGY,
)
@contextmanager
def file_unlink(client, name, include, exclude):
    """Remove matching files from a dataset."""
    dataset = client.load_dataset(name=name)

    if not dataset:
        raise BadParameter('Dataset does not exist.')

    records = _filter(
        client, names=[dataset.name], include=include, exclude=exclude
    )
    if not records:
        raise BadParameter('No records found.')

    yield records

    for item in records:
        dataset.unlink_file(item.path)

    dataset.to_yaml()


@pass_local_client(
    clean=False,
    commit=True,
    commit_only=COMMIT_DIFF_STRATEGY,
)
def dataset_remove(client, names, with_output=False):
    """Delete a dataset."""
    datasets = {name: client.dataset_path(name) for name in names}

    if not datasets:
        raise BadParameter(
            'use dataset name or identifier', param_hint='names'
        )

    unknown = [
        name
        for name, path in datasets.items() if not path or not path.exists()
    ]
    if unknown:
        raise BadParameter(
            'unknown datasets ' + ', '.join(unknown), param_hint='names'
        )

    datasets = set(datasets.values())
    references = list(LinkReference.iter_items(client, common_path='datasets'))

    if not with_output:
        for dataset in datasets:
            if dataset and dataset.exists():
                dataset.unlink()

        for ref in references:
            if ref.reference in datasets:
                ref.delete()

        return datasets, references

    with progressbar(
        datasets,
        label='Removing metadata files'.ljust(30),
        item_show_func=lambda item: str(item) if item else '',
    ) as bar:
        for dataset in bar:
            if dataset and dataset.exists():
                dataset.unlink()

    with progressbar(
        references,
        label='Removing aliases'.ljust(30),
        item_show_func=lambda item: item.name if item else '',
    ) as bar:
        for ref in bar:
            if ref.reference in datasets:
                ref.delete()


@pass_local_client(
    clean=True,
    commit=True,
    commit_only=COMMIT_DIFF_STRATEGY,
)  # TODO: Finish refactor (ticket #702)
def export_dataset(client, id, provider, publish, tag, with_prompt=False):
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

    selected_tag = None
    selected_commit = client.repo.head.commit

    if tag:
        selected_tag = next((t for t in dataset_.tags if t.name == tag), None)

        if not selected_tag:
            raise BadParameter('Tag {} not found'.format(tag))

        selected_commit = selected_tag.commit
    elif dataset_.tags and len(dataset_.tags) > 0:
        # Prompt user to select a tag to export
        tags = sorted(dataset_.tags, key=lambda t: t.created)

        text_prompt = 'Tag to export: \n\n<HEAD>\t[1]\n'

        text_prompt += '\n'.join(
            '{}\t[{}]'.format(t.name, i) for i, t in enumerate(tags, start=2)
        )

        text_prompt += '\n\nTag'

        if with_prompt:
            selection = click.prompt(
                text_prompt, type=click.IntRange(1,
                                                 len(tags) + 1), default=1
            )

            if selection > 1:
                selected_tag = tags[selection - 2]
                selected_commit = selected_tag.commit

    with client.with_commit(selected_commit):
        dataset_ = client.load_dataset(id)
        if not dataset_:
            raise BadParameter('Dataset not found.')

        access_token = client.get_value(provider_id, config_key_secret)
        exporter = provider.get_exporter(dataset_, access_token=access_token)

        if access_token is None:
            text_prompt = (
                'Before exporting, you must configure an access'
                ' token\n'
            )
            text_prompt += 'Create one at: {0}\n'.format(
                exporter.access_token_url()
            )
            text_prompt += 'Access token'

            access_token = click.prompt(text_prompt, type=str)
            if access_token is None or len(access_token) == 0:
                raise InvalidAccessToken()

            client.set_value(provider_id, config_key_secret, access_token)
            exporter.set_access_token(access_token)

        try:
            destination = exporter.export(publish, selected_tag)
        except HTTPError as e:
            if 'unauthorized' in str(e):
                client.remove_value(provider_id, config_key_secret)

            raise BadParameter(e)

    result = 'Exported to: {0}'.format(destination)
    return result


@pass_local_client(
    clean=False,
    commit=True,
    commit_only=COMMIT_DIFF_STRATEGY,
)
def import_dataset(
    client,
    uri,
    name,
    extract,
    with_prompt=False,
    force=False,
    handle_duplicate_fn=None
):
    """Import data from a 3rd party provider."""
    provider, err = ProviderFactory.from_uri(uri)
    if err and provider is None:
        raise BadParameter('Could not process {0}.\n{1}'.format(uri, err))

    try:
        record = provider.find_record(uri)
        dataset = record.as_dataset(client)
        files = dataset.files

        if with_prompt:
            click.echo(
                tabulate(
                    files,
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

            if not force:
                click.confirm(text_prompt, abort=True)

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

    if files:
        data_folder = tempfile.mkdtemp()

        pool_size = min(
            int(os.getenv('RENKU_POOL_SIZE',
                          mp.cpu_count() // 2)), 4
        )

        manager = mp.Manager()
        id_queue = manager.Queue()

        for i in range(pool_size):
            id_queue.put(i)

        def _init(lock, id_queue):
            """Set up tqdm lock and worker process index.

            See https://stackoverflow.com/a/42817946
            Fixes tqdm line position when |files| > terminal-height
            so only |workers| progressbars are shown at a time
            """
            global current_process_position
            current_process_position = id_queue.get()
            tqdm.set_lock(lock)

        freeze_support()  # Windows support
        pool = mp.Pool(
            pool_size,
            # Windows support
            initializer=_init,
            initargs=(RLock(), id_queue)
        )

        processing = [
            pool.apply_async(
                download_file, args=(
                    extract,
                    data_folder,
                    file_,
                )
            ) for file_ in files
        ]

        try:
            for p in processing:
                p.get()  # Will internally do the wait() as well.

        except HTTPError as e:
            raise BadParameter((
                'Could not process {0}.\n'
                'URI not found.'.format(e.request.url)
            ))
        pool.close()

        dataset_name = name or dataset.display_name
        if (
            force or (
                handle_duplicate_fn and
                handle_duplicate_fn(client, dataset_name)
            )
        ):
            add_to_dataset(
                client,
                urls=[str(p) for p in Path(data_folder).glob('*')],
                name=dataset_name,
                with_metadata=dataset
            )

            if dataset.version:
                tag_name = re.sub('[^a-zA-Z0-9.-_]', '_', dataset.version)
                tag_dataset(
                    client,
                    dataset_name,
                    tag_name,
                    'Tag {} created by renku import'.format(dataset.version),
                    force=True
                )


def download_file(extract, data_folder, file, chunk_size=16384):
    """Download a file with progress tracking."""
    global current_process_position

    local_filename = Path(file.filename).name
    download_to = Path(data_folder) / Path(local_filename)

    def extract_dataset(data_folder_, filename):
        """Extract downloaded dataset."""
        import patoolib
        filepath = Path(data_folder_) / Path(filename)
        patoolib.extract_archive(filepath, outdir=data_folder_)
        filepath.unlink()

    def stream_to_file(request):
        """Stream bytes to file."""
        with open(str(download_to), 'wb') as f_:
            scaling_factor = 1e-6
            unit = 'MB'

            # We round sizes to 0.1, files smaller than 1e5 would
            # get rounded to 0, so we display bytes instead
            if file.filesize < 1e5:
                scaling_factor = 1.0
                unit = 'B'

            total = round(file.filesize * scaling_factor, 1)
            progressbar_ = tqdm(
                total=total,
                position=current_process_position,
                desc=file.filename[:32],
                bar_format=(
                    '{{percentage:3.0f}}% '
                    '{{n_fmt}}{unit}/{{total_fmt}}{unit}| '
                    '{{bar}} | {{desc}}'.format(unit=unit)
                ),
                leave=False,
            )

            try:
                bytes_downloaded = 0
                for chunk in request.iter_content(chunk_size=chunk_size):
                    if chunk:  # remove keep-alive chunks
                        f_.write(chunk)
                        bytes_downloaded += chunk_size
                        progressbar_.n = min(
                            float(
                                '{0:.1f}'.format(
                                    bytes_downloaded * scaling_factor
                                )
                            ), total
                        )
                        progressbar_.update(0)
            finally:
                sleep(0.1)
                progressbar_.close()

        if extract:
            extract_dataset(data_folder, local_filename)

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


@pass_local_client(
    clean=False,
    commit=True,
    commit_only=COMMIT_DIFF_STRATEGY,
)
def tag_dataset_with_client(client, name, tag, description, force=False):
    """Creates a new tag for a dataset and injects a LocalClient."""
    tag_dataset(client, name, tag, description, force)


def tag_dataset(client, name, tag, description, force=False):
    """Creates a new tag for a dataset."""
    dataset_ = client.load_dataset(name)
    if not dataset_:
        raise BadParameter('Dataset not found.')

    try:
        dataset = client.add_dataset_tag(dataset_, tag, description, force)
    except ValueError as e:
        raise BadParameter(e)

    dataset.to_yaml()


@pass_local_client(
    clean=False,
    commit=True,
    commit_only=COMMIT_DIFF_STRATEGY,
)
def remove_dataset_tags(client, name, tags):
    """Removes tags from a dataset."""
    dataset = client.load_dataset(name)
    if not dataset:
        raise BadParameter('Dataset not found.')

    try:
        dataset = client.remove_dataset_tags(dataset, tags)
    except ValueError as e:
        raise BadParameter(e)

    dataset.to_yaml()


@pass_local_client(clean=False, commit=False)
def list_tags(client, name, format):
    """List all tags for a dataset."""
    dataset_ = client.load_dataset(name)

    if not dataset_:
        raise BadParameter('Dataset not found.')

    tags = sorted(dataset_.tags, key=lambda t: t.created)

    return DATASET_TAGS_FORMATS[format](client, tags)
