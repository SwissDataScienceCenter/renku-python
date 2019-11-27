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
from multiprocessing import freeze_support
from pathlib import Path
from urllib.parse import ParseResult

import click
import git
import requests
import yaml
from requests import HTTPError

from renku.core.commands.checks.migration import check_dataset_resources, \
    dataset_pre_0_3
from renku.core.commands.format.dataset_tags import DATASET_TAGS_FORMATS
from renku.core.commands.providers import ProviderFactory
from renku.core.compat import contextlib
from renku.core.errors import DatasetNotFound, InvalidAccessToken, \
    MigrationRequired, ParameterError, UsageError
from renku.core.management.datasets import DATASET_METADATA_PATHS
from renku.core.management.git import COMMIT_DIFF_STRATEGY
from renku.core.models.datasets import Dataset
from renku.core.models.provenance.agents import Person
from renku.core.models.refs import LinkReference
from renku.core.models.tabulate import tabulate
from renku.core.utils.doi import extract_doi

from .client import pass_local_client
from .echo import WARNING
from .format.dataset_files import DATASET_FILES_FORMATS
from .format.datasets import DATASETS_FORMATS


def default_download_file(extract, data_folder, file, chunk_size=16384):
    """Download a file."""
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
            for chunk in request.iter_content(chunk_size=chunk_size):
                if chunk:  # remove keep-alive chunks
                    f_.write(chunk)
        if extract:
            extract_dataset(data_folder, local_filename)

    with requests.get(file.url.geturl(), stream=True) as r:
        r.raise_for_status()
        stream_to_file(r)


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
def create_dataset(client, name):
    """Create an empty dataset in the current repo.

    :raises: ``renku.core.errors.ParameterError``
    """
    with client.with_dataset(name=name, create=True) as dataset:
        creator = Person.from_git(client.repo)
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
    commit_empty=False,
    raise_if_empty=True
)
def add_file(
    client,
    urls,
    name,
    link=False,
    force=False,
    create=False,
    sources=(),
    destination='',
    ref=None,
    with_metadata=None,
    urlscontext=contextlib.nullcontext
):
    """Add data file to a dataset."""
    add_to_dataset(
        client, urls, name, link, force, create, sources, destination, ref,
        with_metadata, urlscontext
    )


def add_to_dataset(
    client,
    urls,
    name,
    link=False,
    force=False,
    create=False,
    sources=(),
    destination='',
    ref=None,
    with_metadata=None,
    urlscontext=contextlib.nullcontext
):
    """Add data to a dataset."""
    if len(urls) == 0:
        raise UsageError('No URL is specified')
    if (sources or destination) and len(urls) > 1:
        raise UsageError(
            'Cannot add multiple URLs with --source or --destination'
        )

    # check for identifier before creating the dataset
    identifier = extract_doi(
        with_metadata.identifier
    ) if with_metadata else None
    try:
        with client.with_dataset(
            name=name, identifier=identifier, create=create
        ) as dataset:
            with urlscontext(urls) as bar:
                warning_message = client.add_data_to_dataset(
                    dataset,
                    bar,
                    link=link,
                    force=force,
                    sources=sources,
                    destination=destination,
                    ref=ref
                )

            if warning_message:
                click.echo(WARNING + warning_message)

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

    except DatasetNotFound:
        raise DatasetNotFound(
            'Dataset "{0}" does not exist.\n'
            'Use "renku dataset create {0}" to create the dataset or retry '
            '"renku dataset add {0}" command with "--create" option for '
            'automatic dataset creation.'.format(name)
        )
    except (FileNotFoundError, git.exc.NoSuchPathError) as e:
        raise ParameterError(
            'Could not find paths/URLs: \n{0}'.format('\n'.join(urls))
        ) from e


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
        raise ParameterError('Dataset does not exist.')

    records = _filter(
        client, names=[dataset.name], include=include, exclude=exclude
    )
    if not records:
        raise ParameterError('No records found.')

    yield records

    for item in records:
        dataset.unlink_file(item.path)

    dataset.to_yaml()


@pass_local_client(
    clean=False,
    commit=True,
    commit_only=COMMIT_DIFF_STRATEGY,
)
def dataset_remove(
    client,
    names,
    with_output=False,
    datasetscontext=contextlib.nullcontext,
    referencescontext=contextlib.nullcontext
):
    """Delete a dataset."""
    datasets = {name: client.dataset_path(name) for name in names}

    if not datasets:
        raise ParameterError(
            'use dataset name or identifier', param_hint='names'
        )

    unknown = [
        name
        for name, path in datasets.items() if not path or not path.exists()
    ]
    if unknown:
        raise ParameterError(
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

    datasets_c = datasetscontext(datasets)

    with datasets_c as bar:
        for dataset in bar:
            if dataset and dataset.exists():
                dataset.unlink()

    references_c = referencescontext(references)

    with references_c as bar:
        for ref in bar:
            if ref.reference in datasets:
                ref.delete()


@pass_local_client(
    clean=True,
    commit=True,
    commit_only=COMMIT_DIFF_STRATEGY,
)
def export_dataset(
    client,
    id,
    provider,
    publish,
    tag,
    handle_access_token_fn=None,
    handle_tag_selection_fn=None
):
    """Export data to 3rd party provider.

    :raises: ``ValueError``, ``HTTPError``, ``InvalidAccessToken``,
             ``DatasetNotFound``
    """
    # TODO: all these callbacks are ugly, improve in #737
    config_key_secret = 'access_token'
    provider_id = provider

    dataset_ = client.load_dataset(id)
    if not dataset_:
        raise DatasetNotFound()

    try:
        provider = ProviderFactory.from_id(provider_id)
    except KeyError:
        raise ValueError('Unknown provider.')

    selected_tag = None
    selected_commit = client.repo.head.commit

    if tag:
        selected_tag = next((t for t in dataset_.tags if t.name == tag), None)

        if not selected_tag:
            raise ValueError('Tag {} not found'.format(tag))

        selected_commit = selected_tag.commit
    elif dataset_.tags and len(dataset_.tags) > 0 and handle_tag_selection_fn:
        tag_result = handle_tag_selection_fn(dataset_.tags)

        if tag_result:
            selected_tag = tag_result
            selected_commit = tag_result.commit

    with client.with_commit(selected_commit):
        dataset_ = client.load_dataset(id)
        if not dataset_:
            raise DatasetNotFound()

        access_token = client.get_value(provider_id, config_key_secret)
        exporter = provider.get_exporter(dataset_, access_token=access_token)

        if access_token is None:

            if handle_access_token_fn:
                access_token = handle_access_token_fn(exporter)

            if access_token is None or len(access_token) == 0:
                raise InvalidAccessToken()

            client.set_value(
                provider_id, config_key_secret, access_token, global_only=True
            )
            exporter.set_access_token(access_token)

        try:
            destination = exporter.export(publish, selected_tag)
        except HTTPError as e:
            if 'unauthorized' in str(e):
                client.remove_value(
                    provider_id, config_key_secret, global_only=True
                )

            raise

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
    pool_init_fn=None,
    pool_init_args=None,
    download_file_fn=default_download_file
):
    """Import data from a 3rd party provider."""
    provider, err = ProviderFactory.from_uri(uri)
    if err and provider is None:
        raise ParameterError('Could not process {0}.\n{1}'.format(uri, err))

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

            click.confirm(text_prompt, abort=True)

    except KeyError as e:
        raise ParameterError((
            'Could not process {0}.\n'
            'Unable to fetch metadata due to {1}'.format(uri, e)
        ))

    except LookupError:
        raise ParameterError(
            ('Could not process {0}.\n'
             'URI not found.'.format(uri))
        )

    if files:
        data_folder = tempfile.mkdtemp()

        pool_size = min(
            int(os.getenv('RENKU_POOL_SIZE',
                          mp.cpu_count() // 2)), 4
        )

        freeze_support()  # Windows support

        pool = mp.Pool(
            pool_size,
            # Windows support
            initializer=pool_init_fn,
            initargs=pool_init_args
        )

        processing = [
            pool.apply_async(
                download_file_fn, args=(
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
            raise ParameterError((
                'Could not process {0}.\n'
                'URI not found.'.format(e.request.url)
            ))
        pool.close()

        dataset_name = name or dataset.display_name
        add_to_dataset(
            client,
            urls=[str(p) for p in Path(data_folder).glob('*')],
            name=dataset_name,
            with_metadata=dataset,
            create=True
        )

        if dataset.version:
            tag_name = re.sub('[^a-zA-Z0-9.-_]', '_', dataset.version)
            tag_dataset(
                client, dataset_name, tag_name,
                'Tag {} created by renku import'.format(dataset.version)
            )


@pass_local_client(
    clean=True,
    commit=True,
    commit_only=DATASET_METADATA_PATHS,
    commit_empty=False
)
def update_datasets(
    client,
    names,
    creators,
    include,
    exclude,
    ref,
    delete,
    progress_context=contextlib.nullcontext
):
    """Update files from a remote Git repo."""
    records = _filter(
        client,
        names=names,
        creators=creators,
        include=include,
        exclude=exclude
    )

    if not records:
        raise ParameterError('No files matched the criteria.')

    datasets = {}
    possible_updates = []
    unique_remotes = set()

    for file_ in records:
        if file_.based_on:
            dataset_name = file_.dataset
            dataset = datasets.get(dataset_name)

            if not dataset:
                dataset = client.load_dataset(name=dataset_name)
                datasets[dataset_name] = dataset

            file_.dataset = dataset
            possible_updates.append(file_)
            unique_remotes.add(file_.based_on['url'])

    if ref and len(unique_remotes) > 1:
        raise ParameterError(
            'Cannot use "--ref" with more than one Git repository.\n'
            'Limit list of files to be updated to one repository. See'
            '"renku dataset update -h" for more information.'
        )

    with progress_context(
        possible_updates, item_show_func=lambda x: x.path if x else None
    ) as progressbar:
        deleted_files = client.update_dataset_files(
            files=progressbar, ref=ref, delete=delete
        )

    if deleted_files and not delete:
        click.echo(
            'Some files are deleted from remote. To also delete them locally '
            'run update command with `--delete` flag.'
        )


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
        raise ParameterError('Dataset not found.')

    try:
        dataset = client.add_dataset_tag(dataset_, tag, description, force)
    except ValueError as e:
        raise ParameterError(e)

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
        raise ParameterError('Dataset not found.')

    try:
        dataset = client.remove_dataset_tags(dataset, tags)
    except ValueError as e:
        raise ParameterError(e)

    dataset.to_yaml()


@pass_local_client(clean=False, commit=False)
def list_tags(client, name, format):
    """List all tags for a dataset."""
    dataset_ = client.load_dataset(name)

    if not dataset_:
        raise ParameterError('Dataset not found.')

    tags = sorted(dataset_.tags, key=lambda t: t.created)

    return DATASET_TAGS_FORMATS[format](client, tags)
