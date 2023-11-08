# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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
import tempfile
from pathlib import Path
from typing import Dict, Generator, List, Optional, Tuple, Union, cast

from renku.command.command_builder.command import inject
from renku.core import errors
from renku.core.dataset.context import DatasetContext
from renku.core.dataset.datasets_provenance import DatasetsProvenance
from renku.core.dataset.pointer_file import create_external_file
from renku.core.dataset.providers.api import ImporterApi
from renku.core.dataset.providers.factory import ProviderFactory
from renku.core.dataset.providers.local import LocalProvider
from renku.core.dataset.providers.models import DatasetAddAction, DatasetAddMetadata
from renku.core.interface.dataset_gateway import IDatasetGateway
from renku.core.interface.storage import IStorage
from renku.core.lfs import check_external_storage, track_paths_in_storage
from renku.core.util import communication, requests
from renku.core.util.git import get_git_user
from renku.core.util.os import get_absolute_path, get_file_size, get_files, get_relative_path, hash_file, is_subpath
from renku.core.util.urls import check_url, is_uri_subfolder, resolve_uri
from renku.core.util.util import parallel_execute
from renku.domain_model.constant import NON_EXISTING_ENTITY_CHECKSUM
from renku.domain_model.dataset import Dataset, DatasetFile, RemoteEntity
from renku.domain_model.project_context import project_context


def add_to_dataset(
    dataset_slug: str,
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
        with DatasetContext(slug=dataset_slug, create=create, datadir=datadir, storage=storage) as dataset:
            destination_path = _create_destination_directory(dataset, destination)

            check_external_storage()

            # NOTE: This is not required for cloud storages
            if not dataset.storage:
                _check_available_space(urls, total_size=total_size)

            datadir = cast(Path, project_context.path / dataset.get_datadir())
            if create and datadir.exists() and not dataset.storage:
                # NOTE: Add datadir to paths to add missing files on create
                for file in get_files(datadir):
                    urls.append(str(file))

            files = get_files_metadata(
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

            copy_files_to_dataset(dataset=dataset, files=files)
            add_files_to_repository(dataset=dataset, files=files)
            update_dataset_metadata(dataset=dataset, files=files, clear_files_before=clear_files_before)

            DatasetsProvenance().add_or_update(dataset, creator=get_git_user(repository=project_context.repository))
    except errors.DatasetNotFound:
        raise errors.DatasetNotFound(
            message="Dataset '{0}' does not exist.\n"
            "Use 'renku dataset create {0}' to create the dataset or retry 'renku dataset add {0}' command "
            "with '--create' option for automatic dataset creation.".format(dataset_slug)
        )
    except (FileNotFoundError, errors.GitCommandError) as e:
        raise errors.ParameterError("Could not find paths/URLs: \n{}".format("\n".join(urls))) from e
    else:
        project_context.database.commit()
        return dataset


def get_files_metadata(
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
        is_remote, is_git = check_url(url)

        if not is_git and sources:
            raise errors.ParameterError("Cannot use '-s/--src/--source' with URLs or local files.")

        dataset_add_action = DatasetAddAction.NONE

        if is_remote:
            provider = ProviderFactory.get_add_provider(uri=url)
        else:
            # NOTE: If URI is in the local file system, check to see if it's part of a mounted dataset/provider
            cloud_dataset, remote_url = get_cloud_dataset_from_path(path=url)
            if cloud_dataset:
                url = remote_url
                provider = ProviderFactory.get_storage_provider(uri=cloud_dataset.storage)
                # NOTE: Update metadata if destination dataset is the same as source dataset, otherwise copy the file
                # since it's already in the local filesystem
                dataset_add_action = DatasetAddAction.COPY
            else:
                provider = LocalProvider(uri=url)

        new_files = provider.get_metadata(
            uri=url,
            destination=destination,
            revision=revision,
            sources=sources,
            dataset=dataset,
            extract=extract,
            force=force,
            dataset_add_action=dataset_add_action,
            **kwargs,
        )

        files.extend(new_files)

    return files


@inject.autoparams("dataset_gateway")
def has_cloud_storage(dataset_gateway: IDatasetGateway) -> bool:
    """Return if a project has any dataset with cloud storage with its data directory mounted or pulled."""
    # NOTE: ``exists`` return False for symlinks if their target doesn't exist, but it's fine here since it means the
    # dataset's mounted/pulled location doesn't exist.
    return any(
        dataset
        for dataset in dataset_gateway.get_all_active_datasets()
        if dataset.storage and (project_context.path / dataset.get_datadir()).exists()
    )


@inject.autoparams("dataset_gateway")
def get_cloud_dataset_from_path(
    path: Union[Path, str], dataset_gateway: IDatasetGateway, missing_ok: bool = False
) -> Tuple[Optional[Dataset], Optional[str]]:
    """Check the path against datasets' storage and return a dataset (if any)."""
    if not has_cloud_storage():
        return None, None

    # NOTE: If path is inside the datadir of a dataset with storage backend and the dataset isn't mounted, we should
    # still add whatever is in the path (because it might have been pulled)

    path = Path(get_absolute_path(path))

    if not missing_ok and not path.exists() and not os.path.lexists(path):
        return None, None

    for dataset in dataset_gateway.get_all_active_datasets():
        if not dataset.storage:
            continue

        datadir = project_context.path / dataset.get_datadir()
        resolved_path = path.resolve()

        # NOTE: Resolve ``path`` because ``datadir`` is resolved and resolved paths might have been on a different
        # location (e.g. on macOS /tmp resolves to /private/tmp)
        resolved_relative_path = get_relative_path(resolved_path, base=datadir.resolve())

        if is_subpath(path, base=datadir) or resolved_relative_path is not None:
            if resolved_relative_path == ".":
                resolved_relative_path = ""
            storage_uri = dataset.storage.rstrip("/")
            remote_url = f"{storage_uri}/{resolved_relative_path}"
            return dataset, remote_url
        elif is_subpath(resolved_path, Path(dataset.storage).resolve()):  # NOTE: For local backend storage
            return dataset, str(resolved_path)

    return None, None


def _check_available_space(urls: List[str], total_size: Optional[int] = None):
    """Check that there is enough space available on the device for download."""
    if total_size is None:
        total_size = 0
        for url in urls:
            is_remote, _ = check_url(url)
            if not is_remote:
                continue

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


def copy_file(file: DatasetAddMetadata, dataset: Dataset, storage: Optional[IStorage]) -> List[Optional[Path]]:
    """Copy/move/link a file to dataset's data directory."""
    if not file.has_action:
        return []

    # NOTE: If file is in a subdirectory of a dataset's remote storage URI, only update the metadata
    if file.from_cloud_storage:
        if dataset.storage and is_uri_subfolder(resolve_uri(dataset.storage), file.url):
            file.action = DatasetAddAction.METADATA_ONLY
        else:
            file.action = DatasetAddAction.DOWNLOAD

    if file.action in (
        DatasetAddAction.COPY,
        DatasetAddAction.MOVE,
        DatasetAddAction.SYMLINK,
        DatasetAddAction.DOWNLOAD,
    ):
        try:
            file.destination.parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise errors.InvalidFileOperation(f"Cannot create destination '{file.destination.parent}': {e}")

    file_to_upload: Union[Path, str] = file.source.resolve()
    delete_source = False
    track_in_lfs = True

    try:
        if file.action == DatasetAddAction.DOWNLOAD:
            # NOTE: Download to a temporary location if dataset has a cloud storage because it's usually mounted as
            # read-only and download would fail. It's ok not to move it to dataset's data dir since it'll be uploaded.
            dst: Union[Path, str]
            if storage:
                fd, dst = tempfile.mkstemp()
                os.close(fd)
            else:
                dst = file.destination

            assert file.provider, f"Storage provider isn't set for {file} with DOWNLOAD action"
            download_storage = file.provider.get_storage()
            download_storage.download(file.url, dst)
            file_to_upload = dst
        elif file.action == DatasetAddAction.COPY:
            shutil.copy(file.source, file.destination)
        elif file.action == DatasetAddAction.MOVE:
            # NOTE: Set ``delete_source`` in case move fails due to a dataset's read-only mounted data directory
            delete_source = True
            shutil.move(file.source, file.destination, copy_function=shutil.copy)  # type: ignore
            delete_source = False
            file_to_upload = file.destination
        elif file.action == DatasetAddAction.SYMLINK:
            create_external_file(target=file.source, path=file.destination)
            # NOTE: Don't track symlinks to external files in LFS
            track_in_lfs = False
        elif file.metadata_only:
            # NOTE: Nothing to do when adding file to a dataset with a parent remote storage
            pass
        else:
            raise errors.OperationError(f"Invalid action {file.action}")
    except OSError as e:
        # NOTE: It's ok if copying data to a read-only mounted cloud storage fails
        if "Read-only file system" in str(e) and storage:
            pass
        else:
            dst = get_relative_path(file.destination, project_context.path) or file.destination
            raise errors.InvalidFileOperation(f"Cannot copy/move '{dst}': {e}")

    if file.size is None:
        file.size = get_file_size(file_to_upload)

    if storage:
        # NOTE: Don't track files in a dataset with cloud storage in LFS
        track_in_lfs = False

        if file.metadata_only:
            assert file.based_on, f"wasBasedOn isn't set for {file} with METADATA_ONLY action"
            file_uri = file.based_on.url
            md5_hash: Optional[str] = file.based_on.checksum
        else:
            file_uri = get_upload_uri(dataset=dataset, entity_path=file.entity_path)
            md5_hash = hash_file(file_to_upload, hash_type="md5")

            # NOTE: If dataset has a storage backend, upload the file to the remote storage.
            storage.upload(source=file_to_upload, uri=file_uri)

        file.based_on = RemoteEntity(url=file_uri, path=file.entity_path, checksum=md5_hash)

    if delete_source:
        file.source.unlink(missing_ok=True)

    return [file.destination] if track_in_lfs else []


def copy_files_to_dataset(dataset: Dataset, files: List[DatasetAddMetadata]):
    """Copy/Move files into a dataset's directory."""

    dataset_storage = None
    if dataset.storage:
        provider = ProviderFactory.get_storage_provider(uri=dataset.storage)
        dataset_storage = provider.get_storage()

    lfs_files = parallel_execute(copy_file, files, rate=5, dataset=dataset, storage=dataset_storage)

    if lfs_files and not dataset.storage:
        track_paths_in_storage(*lfs_files)


def add_files_to_repository(dataset: Dataset, files: List[DatasetAddMetadata]):
    """Track files in project's repository."""
    # NOTE: There is nothing to track for remote storages
    if dataset.storage:
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
    # NOTE: For datasets with cloud storage backend, we use MD5 hash as checksum instead of git hash.
    if dataset.storage:
        checksums: Dict[Union[Path, str], Optional[str]] = {
            f.entity_path: f.based_on.checksum for f in files if f.based_on
        }
    else:
        repo_paths: List[Union[Path, str]] = [
            file.entity_path for file in files if (project_context.path / file.entity_path).exists()
        ]
        checksums = project_context.repository.get_object_hashes(repo_paths)

    dataset_files = []

    for file in files:
        dataset_file = DatasetFile.from_path(
            path=file.entity_path,
            source=file.url,
            based_on=file.based_on,
            size=file.size,
            checksum=checksums.get(file.entity_path) or NON_EXISTING_ENTITY_CHECKSUM,
        )
        dataset_files.append(dataset_file)

    if clear_files_before:
        dataset.clear_files()

    dataset.add_or_update_files(dataset_files)
