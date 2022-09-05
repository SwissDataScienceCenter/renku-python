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
"""Dataset add business logic."""

import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Set, Union, cast
from urllib.parse import urlparse

from renku.core import errors
from renku.core.dataset.constant import renku_pointers_path
from renku.core.dataset.context import DatasetContext
from renku.core.dataset.datasets_provenance import DatasetsProvenance
from renku.core.dataset.pointer_file import create_external_file
from renku.core.dataset.providers.api import ImporterApi
from renku.core.dataset.providers.factory import ProviderFactory
from renku.core.dataset.providers.models import DatasetAddAction
from renku.core.util import communication, requests
from renku.core.util.dataset import check_url
from renku.core.util.dispatcher import get_client, get_database
from renku.core.util.git import get_git_user
from renku.core.util.os import delete_dataset_file, get_relative_path
from renku.domain_model.dataset import Dataset, DatasetFile

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import DatasetAddMetadata
    from renku.core.management.client import LocalClient


def add_to_dataset(
    dataset_name: str,
    urls: List[str],
    *,
    importer: Optional[ImporterApi] = None,
    force: bool = False,
    create: bool = False,
    overwrite: bool = False,
    sources: Optional[List[Union[str, Path]]] = None,
    destination: str = "",
    revision: Optional[str] = None,
    extract: bool = False,
    clear_files_before: bool = False,
    total_size: Optional[int] = None,
    datadir: Optional[Path] = None,
    storage: Optional[str] = None,
    **kwargs,
) -> Dataset:
    """Import the data into the data directory."""
    client = get_client()
    sources = sources or []

    _check_available_space(client, urls, total_size=total_size)

    if not create and storage:
        raise errors.ParameterError(
            "Using the '--storage' parameter is only required if the '--create' parameter is also used to "
            "create the dataset at the same time as when data is added to it"
        )
    if create and not storage and any([url.lower().startswith("s3://") for url in urls]):
        raise errors.ParameterError(
            "Creating a S3 dataset at the same time as adding data requires the '--storage' parameter to be set"
        )

    try:
        with DatasetContext(name=dataset_name, create=create, datadir=datadir, storage=storage) as dataset:
            destination_path = _create_destination_directory(client, dataset, destination)

            client.check_external_storage()  # TODO: This is not required for external storages

            files = _download_files(
                client=client,
                urls=urls,
                dataset=dataset,
                importer=importer,
                destination=destination_path,
                revision=revision,
                sources=sources,
                extract=extract,
                force=force,
                **kwargs,
            )

            # Remove all files that are under a .git directory
            paths_to_avoid = [f.entity_path for f in files if ".git" in str(f.entity_path).split(os.path.sep)]
            if paths_to_avoid:
                files = [f for f in files if f.entity_path not in paths_to_avoid]
                communication.warn(
                    "Ignored adding paths under a .git directory:\n\t" + "\n\t".join(str(p) for p in paths_to_avoid)
                )

            files_to_commit = {f.get_absolute_commit_path(client.path) for f in files if not f.gitignored}

            if not force:
                files, files_to_commit = _check_ignored_files(client, files_to_commit, files)

            # all files at this point can be force-added

            if not overwrite:
                files, files_to_commit = _check_existing_files(client, dataset, files_to_commit, files)

            move_files_to_dataset(client, files)

            # Track non-symlinks in LFS
            if client.check_external_storage():
                client.track_paths_in_storage(*files_to_commit)

            # Force-add to include possible ignored files
            if len(files_to_commit) > 0:
                client.repository.add(*files_to_commit, renku_pointers_path(client), force=True)

            n_staged_changes = len(client.repository.staged_changes)
            if n_staged_changes == 0:
                communication.warn("No new file was added to project")

            if not files:
                if create:
                    raise errors.UsageError("There are no files to create a dataset")

                return dataset

            dataset_files = _generate_dataset_files(client, dataset, files, clear_files_before)

            dataset.add_or_update_files(dataset_files)
            datasets_provenance = DatasetsProvenance()
            datasets_provenance.add_or_update(dataset, creator=get_git_user(client.repository))

        get_database().commit()
    except errors.DatasetNotFound:
        raise errors.DatasetNotFound(
            message='Dataset "{0}" does not exist.\n'
            'Use "renku dataset create {0}" to create the dataset or retry '
            '"renku dataset add {0}" command with "--create" option for '
            "automatic dataset creation.".format(dataset_name)
        )
    except (FileNotFoundError, errors.GitCommandError) as e:
        raise errors.ParameterError("Could not find paths/URLs: \n{0}".format("\n".join(urls))) from e
    else:
        return dataset


def _download_files(
    *,
    client: "LocalClient",
    urls: List[str],
    importer: Optional[ImporterApi] = None,
    dataset: Dataset,
    destination: Path,
    extract: bool,
    revision: Optional[str],
    sources: List[Union[str, Path]],
    force: bool = False,
    **kwargs,
) -> List["DatasetAddMetadata"]:
    """Process file URLs for adding to a dataset."""
    if dataset.storage and any([urlparse(dataset.storage).scheme != urlparse(url).scheme for url in urls]):
        raise errors.ParameterError(
            f"The scheme of some urls {urls} does not match the defined storage url {dataset.storage}."
        )

    if importer:
        return importer.download_files(client=client, destination=destination, extract=extract)

    if len(urls) == 0:
        raise errors.ParameterError("No URL is specified")
    if sources and len(urls) > 1:
        raise errors.ParameterError("Cannot use '--source' with multiple URLs.")

    files = []

    for url in urls:
        _, is_git = check_url(url)

        if not is_git and sources:
            raise errors.ParameterError("Cannot use '-s/--src/--source' with URLs or local files.")

        provider = ProviderFactory.get_add_provider(uri=url)

        new_files = provider.add(
            client=client,
            uri=url,
            destination=destination,
            revision=revision,
            sources=sources,
            dataset=dataset,
            extract=extract,
            force=force,
            **kwargs,
        )

        files.extend(new_files)

    return files


def _check_available_space(client: "LocalClient", urls: List[str], total_size: Optional[int] = None):
    """Check that there is enough space available on the device for download."""
    if total_size is None:
        total_size = 0
        for url in urls:
            try:
                response = requests.head(url, allow_redirects=True)
                total_size += int(response.headers.get("content-length", 0))
            except errors.RequestError:
                pass
    usage = shutil.disk_usage(client.path)

    if total_size > usage.free:
        mb = 2**20
        message = "Insufficient disk space (required: {:.2f} MB" "/available: {:.2f} MB). ".format(
            total_size / mb, usage.free / mb
        )
        raise errors.OperationError(message)


def _create_destination_directory(
    client: "LocalClient", dataset: Dataset, destination: Optional[Union[Path, str]] = None
) -> Path:
    """Create directory for dataset add."""
    dataset_datadir = client.path / dataset.get_datadir()

    if dataset_datadir.is_symlink():
        dataset_datadir.unlink()

    # NOTE: Make sure that dataset's data dir exists because we check for existence of a destination later to decide
    # what will be its name
    dataset_datadir.mkdir(parents=True, exist_ok=True)

    destination = destination or ""
    relative_path = cast(str, get_relative_path(destination, base=dataset_datadir, strict=True))
    return dataset_datadir / relative_path


def _check_ignored_files(client: "LocalClient", files_to_commit: Set[str], files: List["DatasetAddMetadata"]):
    """Check if any files added were ignored."""
    ignored_files = set(client.find_ignored_paths(*files_to_commit))
    if ignored_files:
        ignored_sources = []
        for file in files:
            if not file.gitignored and file.get_absolute_commit_path(client.path) in ignored_files:
                ignored_sources.append(file.source)

        communication.warn(
            "Theses paths are ignored by one of your .gitignore files (use '--force' flag if you really want to add "
            "them):\n\t" + "\n\t".join([str(p) for p in ignored_sources])
        )

        files_to_commit = files_to_commit.difference(ignored_files)
        files = [f for f in files if f.get_absolute_commit_path(client.path) not in ignored_files]

    return files, files_to_commit


def _check_existing_files(
    client: "LocalClient", dataset: Dataset, files_to_commit: Set[str], files: List["DatasetAddMetadata"]
):
    """Check if files added already exist."""
    existing_files = set()
    for path in files_to_commit:
        relative_path = Path(path).relative_to(client.path)
        if dataset.find_file(relative_path):
            existing_files.add(path)

    if existing_files:
        communication.warn(
            "These existing files were not overwritten (use '--overwrite' flag to overwrite them):\n\t"
            + "\n\t".join([str(p) for p in existing_files])
        )

        files_to_commit = files_to_commit.difference(existing_files)
        files = [f for f in files if f.get_absolute_commit_path(client.path) not in existing_files]

    return files, files_to_commit


def move_files_to_dataset(client: "LocalClient", files: List["DatasetAddMetadata"]):
    """Copy/Move files into a dataset's directory."""
    for file in files:
        if not file.has_action:
            continue

        # Remove existing file if any; required as a safety-net to avoid corrupting external files
        delete_dataset_file(file.destination, follow_symlinks=True)
        file.destination.parent.mkdir(parents=True, exist_ok=True)

        if file.action == DatasetAddAction.COPY:
            shutil.copy(file.source, file.destination)
        elif file.action == DatasetAddAction.MOVE:
            shutil.move(file.source, file.destination, copy_function=shutil.copy)  # type: ignore
        elif file.action == DatasetAddAction.SYMLINK:
            create_external_file(client=client, target=file.source, path=file.destination)
        else:
            raise errors.OperationError(f"Invalid action {file.action}")


def _generate_dataset_files(
    client: "LocalClient", dataset: Dataset, files: List["DatasetAddMetadata"], clear_files_before: bool = False
):
    """Generate DatasetFile entries from file dict."""
    dataset_files = []
    for file in files:
        dataset_file = DatasetFile.from_path(
            client=client, path=file.entity_path, source=file.url, based_on=file.based_on
        )
        dataset_files.append(dataset_file)

    if clear_files_before:
        dataset.clear_files()
    return dataset_files
