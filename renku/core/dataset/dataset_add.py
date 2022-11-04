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

import itertools
import os.path
import shutil
from pathlib import Path
from typing import Generator, List, Optional, Union, cast

from renku.core import errors
from renku.core.dataset.context import DatasetContext
from renku.core.dataset.datasets_provenance import DatasetsProvenance
from renku.core.dataset.pointer_file import create_external_file
from renku.core.dataset.providers.api import ImporterApi
from renku.core.dataset.providers.factory import ProviderFactory
from renku.core.dataset.providers.models import DatasetAddAction, DatasetAddMetadata
from renku.core.interface.storage import IStorage
from renku.core.storage import check_external_storage, track_paths_in_storage
from renku.core.util import communication, requests
from renku.core.util.dataset import check_url
from renku.core.util.git import get_git_user
from renku.core.util.os import delete_dataset_file, get_files, get_relative_path, hash_file
from renku.core.util.urls import is_uri_subfolder
from renku.domain_model.dataset import Dataset, DatasetFile, RemoteEntity
from renku.domain_model.project_context import project_context


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
    sources = sources or []

    if not create and storage:
        raise errors.ParameterError("Storage can be set only when creating a dataset")

    try:
        with DatasetContext(name=dataset_name, create=create, datadir=datadir, storage=storage) as dataset:
            destination_path = _create_destination_directory(dataset, destination)

            check_external_storage()

            # NOTE: This is not required for external storages
            if not dataset.storage:
                _check_available_space(urls, total_size=total_size)

            datadir = cast(Path, project_context.path / dataset.get_datadir())
            if create and datadir.exists() and not dataset.storage:
                # NOTE: Add datadir to paths to add missing files on create
                for file in get_files(datadir):
                    urls.append(str(file))

            files = _get_files_metadata(
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

            files = filter_files(dataset=dataset, files=files, force=force, overwrite=overwrite)
            if not files:
                if create:
                    raise errors.UsageError("There are no files to create a dataset")
                else:
                    communication.warn("No new file was added to project")
                    return dataset

            # NOTE: All files at this point can be force-added

            move_files_to_dataset(dataset=dataset, files=files)
            add_files_to_repository(dataset=dataset, files=files)
            update_dataset_metadata(dataset=dataset, files=files, clear_files_before=clear_files_before)

            DatasetsProvenance().add_or_update(dataset, creator=get_git_user(repository=project_context.repository))
    except errors.DatasetNotFound:
        raise errors.DatasetNotFound(
            message="Dataset '{0}' does not exist.\n"
            "Use 'renku dataset create {0}' to create the dataset or retry 'renku dataset add {0}' command "
            "with '--create' option for automatic dataset creation.".format(dataset_name)
        )
    except (FileNotFoundError, errors.GitCommandError) as e:
        raise errors.ParameterError("Could not find paths/URLs: \n{0}".format("\n".join(urls))) from e
    else:
        project_context.database.commit()
        return dataset


def _get_files_metadata(
    *,
    urls: List[str],
    importer: Optional[ImporterApi] = None,
    dataset: Dataset,
    destination: Path,
    extract: bool,
    revision: Optional[str],
    sources: List[Union[str, Path]],
    force: bool = False,
    **kwargs,
) -> List[DatasetAddMetadata]:
    """Process file URLs for adding to a dataset."""
    if importer:
        return importer.download_files(destination=destination, extract=extract)

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


def _check_available_space(urls: List[str], total_size: Optional[int] = None):
    """Check that there is enough space available on the device for download."""
    if total_size is None:
        total_size = 0
        for url in urls:
            try:
                response = requests.head(url, allow_redirects=True)
                total_size += int(response.headers.get("content-length", 0))
            except errors.RequestError:
                pass
    usage = shutil.disk_usage(project_context.path)

    if total_size > usage.free:
        mb = 2**20
        message = "Insufficient disk space (required: {:.2f} MB" "/available: {:.2f} MB). ".format(
            total_size / mb, usage.free / mb
        )
        raise errors.OperationError(message)


def _create_destination_directory(dataset: Dataset, destination: Optional[Union[Path, str]] = None) -> Path:
    """Create directory for dataset add."""
    dataset_datadir = project_context.path / dataset.get_datadir()

    if dataset_datadir.is_symlink():
        dataset_datadir.unlink()

    # NOTE: Make sure that dataset's data dir exists because we check for existence of a destination later to decide
    # what will be its name
    dataset_datadir.mkdir(parents=True, exist_ok=True)

    destination = destination or ""
    relative_path = cast(str, get_relative_path(destination, base=dataset_datadir, strict=True))
    return dataset_datadir / relative_path


def filter_files(
    dataset: Dataset, files: List[DatasetAddMetadata], force: bool, overwrite: bool
) -> List[DatasetAddMetadata]:
    """Filter ignored and overwritten files."""

    def remove_git_files(files_to_filter: List[DatasetAddMetadata]):
        """Remove all files that are under a .git directory."""
        git_paths = [f.entity_path for f in files_to_filter if str(f.entity_path).startswith(".git")]
        if not git_paths:
            return files_to_filter

        communication.warn("Ignored adding paths under a .git directory:\n\t" + "\n\t".join(str(p) for p in git_paths))
        return [f for f in files_to_filter if f.entity_path not in git_paths]

    def check_ignored_files(files_to_filter: Generator[DatasetAddMetadata, None, None]):
        """Check if any files added were ignored."""
        paths = {f.get_absolute_commit_path(project_context.path): f for f in files_to_filter}

        ignored_paths = project_context.repository.get_ignored_paths(*paths)
        if ignored_paths:
            ignored_sources = [file.source for path, file in paths.items() if path in ignored_paths]

            communication.warn(
                "Theses paths are ignored by one of your .gitignore files (use '--force' flag if you really want to "
                "add them):\n\t" + "\n\t".join([str(p) for p in ignored_sources])
            )

        return (file for path, file in paths.items() if path not in ignored_paths)

    def check_existing_files(files_to_filter: Generator[DatasetAddMetadata, None, None]):
        """Check if files added already exist."""
        files_list = list(files_to_filter)
        existing_paths = [f.entity_path for f in files_list if dataset.find_file(f.entity_path)]
        if existing_paths:
            communication.warn(
                "These existing files were not overwritten (use '--overwrite' flag to overwrite them):\n\t"
                + "\n\t".join([str(p) for p in existing_paths])
            )

        return (f for f in files_list if f.entity_path not in existing_paths)

    files = remove_git_files(files)

    # NOTE: Don't filter ignored or existing files that will be added to a remote storage
    remote_files = (f for f in files if f.metadata_only)
    local_files = (f for f in files if not f.metadata_only)

    # NOTE: Data directory of datasets with a storage backend is always ignored, so, filtering files is meaningless
    if not force and not dataset.storage:
        local_files = check_ignored_files(local_files)

    if not overwrite:
        local_files = check_existing_files(local_files)

    files = list(itertools.chain(local_files, remote_files))

    return files


def get_dataset_file_path_within_dataset(dataset: Dataset, entity_path: Union[Path, str]) -> Path:
    """Return a dataset file's path relative to the dataset's datadir.

    NOTE: Dataset files can reside outside the dataset's datadir.
    """
    assert not os.path.isabs(entity_path), f"Entity path cannot be absolute: {entity_path}"

    entity_path = Path(entity_path)

    try:
        return entity_path.relative_to(dataset.get_datadir())
    except ValueError:
        return entity_path


def get_upload_uri(dataset: Dataset, entity_path: Union[Path, str]) -> str:
    """Return the remote storage path that a dataset file would be located.

    Args:
        dataset(Dataset): Dataset with a backend storage.
        entity_path(Union[Path, str]): Dataset file's path (entity path); it is relative to the project's root.

    Returns:
        str: URI within remote storage.
    """
    assert dataset.storage, "Cannot get URI for datasets with no backend storage"

    base = dataset.storage.rstrip("/")
    path_within_dataset = get_dataset_file_path_within_dataset(dataset=dataset, entity_path=entity_path)

    return f"{base}/{path_within_dataset}"


def move_files_to_dataset(dataset: Dataset, files: List[DatasetAddMetadata]):
    """Copy/Move files into a dataset's directory."""

    def move_file(file: DatasetAddMetadata, storage: Optional[IStorage]):
        if not file.has_action:
            return

        if file.action in (
            DatasetAddAction.COPY,
            DatasetAddAction.MOVE,
            DatasetAddAction.SYMLINK,
            DatasetAddAction.DOWNLOAD,
        ):
            # NOTE: Remove existing file if any; required as a safety-net to avoid corrupting external files
            delete_dataset_file(file.destination, follow_symlinks=True)
            file.destination.parent.mkdir(parents=True, exist_ok=True)

        track_in_lfs = True

        # NOTE: If file is in a sub-directory of a dataset's remote storage URI, only update the metadata
        if file.remote_storage:
            if dataset.storage and is_uri_subfolder(dataset.storage, file.url):
                file.action = DatasetAddAction.METADATA_ONLY
            else:
                file.action = DatasetAddAction.DOWNLOAD

        if file.action == DatasetAddAction.COPY:
            shutil.copy(file.source, file.destination)
        elif file.action == DatasetAddAction.MOVE:
            shutil.move(file.source, file.destination, copy_function=shutil.copy)  # type: ignore
        elif file.action == DatasetAddAction.SYMLINK:
            create_external_file(target=file.source, path=file.destination)
            # NOTE: Don't track symlinks to external files in LFS
            track_in_lfs = False
        elif file.action == DatasetAddAction.DOWNLOAD:
            assert file.provider, f"Storage provider isn't set for {file} with DOWNLOAD action"
            storage = file.provider.get_storage()
            storage.download(file.url, file.destination)
        elif file.metadata_only:
            # NOTE: Nothing to do when adding file to a dataset with a parent remote storage
            pass
        else:
            raise errors.OperationError(f"Invalid action {file.action}")

        if track_in_lfs and not dataset.storage:
            track_paths_in_storage(file.destination)

        # NOTE: We always copy the files to the dataset's data dir. If dataset has a storage backend, we also upload the
        # file to the remote storage.
        if storage:
            if file.metadata_only:
                assert file.based_on, f"wasBasedOn isn't set for {file} with METADATA_ONLY action"
                file_uri = file.based_on.url
                md5_hash = file.based_on.checksum
            else:
                file_uri = get_upload_uri(dataset=dataset, entity_path=file.entity_path)
                storage.upload(source=file.destination, uri=file_uri)
                md5_hash = hash_file(file.destination, hash_type="md5") or ""

            file.based_on = RemoteEntity(url=file_uri, path=file.entity_path, checksum=md5_hash)

    dataset_storage = None
    if dataset.storage:
        provider = ProviderFactory.get_storage_provider(uri=dataset.storage)
        dataset_storage = provider.get_storage()

    for dataset_file in files:
        # TODO: Parallelize copy/download/upload
        move_file(file=dataset_file, storage=dataset_storage)


def add_files_to_repository(dataset: Dataset, files: List[DatasetAddMetadata]):
    """Track files in project's repository."""
    # NOTE: There is nothing to track for remote storages
    if dataset.storage:
        communication.info("Nothing to add to the project for datasets with a storage backend")
        return

    # NOTE: Don't commit files that will be uploaded to a remote storage
    paths_to_commit = [f.get_absolute_commit_path(project_context.path) for f in files]

    repository = project_context.repository

    # Force-add to include possible ignored files
    if len(paths_to_commit) > 0:
        repository.add(*paths_to_commit, project_context.pointers_path, force=True)

    n_staged_changes = len(repository.staged_changes)
    if n_staged_changes == 0:
        communication.warn("No new file was added to project")


def update_dataset_metadata(dataset: Dataset, files: List[DatasetAddMetadata], clear_files_before: bool):
    """Add newly-added files to the dataset's metadata."""
    dataset_files = []
    for file in files:
        dataset_file = DatasetFile.from_path(path=file.entity_path, source=file.url, based_on=file.based_on)
        dataset_files.append(dataset_file)

    if clear_files_before:
        dataset.clear_files()

    dataset.add_or_update_files(dataset_files)
