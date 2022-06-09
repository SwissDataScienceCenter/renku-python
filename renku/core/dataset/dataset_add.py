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


import concurrent.futures
import glob
import os
import shutil
import time
import urllib
from collections import defaultdict
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, NamedTuple, Optional, Set, Tuple, Union, cast

from renku.command.command_builder.command import inject
from renku.core import errors
from renku.core.constant import CACHE
from renku.core.dataset.constant import renku_pointers_path
from renku.core.dataset.context import DatasetContext
from renku.core.dataset.datasets_provenance import DatasetsProvenance
from renku.core.dataset.pointer_file import create_external_file
from renku.core.dataset.providers.models import ProviderDataset
from renku.core.interface.client_dispatcher import IClientDispatcher
from renku.core.interface.database_dispatcher import IDatabaseDispatcher
from renku.core.util import communication, requests
from renku.core.util.git import clone_repository, get_cache_directory_for_repository, get_git_user
from renku.core.util.metadata import is_external_file, make_project_temp_dir
from renku.core.util.os import delete_file, get_absolute_path, get_files, get_relative_path, is_subpath
from renku.core.util.urls import check_url, provider_check, remove_credentials
from renku.domain_model.dataset import Dataset, DatasetFile, RemoteEntity, get_dataset_data_dir
from renku.infrastructure.repository import Repository

if TYPE_CHECKING:
    from renku.core.management.client import LocalClient


class AddAction(Enum):
    """Types of action when adding a file to a dataset."""

    COPY = auto()
    MOVE = auto()
    SYMLINK = auto()
    NONE = auto()


class NewDatasetFile(NamedTuple):
    """Represent a new file that will be added to a dataset."""

    entity_path: Path
    url: str
    action: AddAction
    source: Path
    destination: Path
    based_on: Optional[RemoteEntity] = None

    @property
    def has_action(self) -> bool:
        """Returns if file action is not NONE."""
        return self.action != AddAction.NONE

    def get_absolute_commit_path(self, client_path: Path) -> str:
        """Return path of the file in the repository."""
        return os.path.join(client_path, self.entity_path)


@inject.autoparams("client_dispatcher", "database_dispatcher")
def add_data_to_dataset(
    dataset_name: str,
    urls: List[str],
    client_dispatcher: IClientDispatcher,
    database_dispatcher: IDatabaseDispatcher,
    force: bool = False,
    create: bool = False,
    overwrite: bool = False,
    sources: Optional[List[Union[str, Path]]] = None,
    destination: str = "",
    checksums: Optional[List[str]] = None,
    ref: Optional[str] = None,
    external: bool = False,
    extract: bool = False,
    is_import: bool = False,
    is_renku_import: bool = False,
    destination_names: Optional[List[str]] = None,
    repository: Optional[Repository] = None,
    clear_files_before: bool = False,
    total_size: Optional[int] = None,
    with_metadata: Optional[ProviderDataset] = None,
    dataset_datadir: Optional[str] = None,
) -> Dataset:
    """Import the data into the data directory."""
    client = client_dispatcher.current_client
    sources = sources or []

    if len(urls) == 0:
        raise errors.UsageError("No URL is specified")
    if sources and len(urls) > 1:
        raise errors.UsageError('Cannot use "--source" with multiple URLs.')

    _check_available_space(client, urls, total_size=total_size)

    try:
        with DatasetContext(name=dataset_name, create=create) as dataset:
            destination_path = _create_destination_directory(client, dataset, destination)

            client.check_external_storage()

            files = _process_urls(
                client=client,
                dataset=dataset,
                urls=urls,
                destination=destination_path,
                ref=ref,
                sources=sources,
                destination_names=destination_names,
                checksums=checksums,
                external=external,
                extract=extract,
                is_import=is_import,
                is_renku_import=is_renku_import,
                repository=repository,
                dataset_datadir=dataset_datadir,
            )

            # Remove all files that are under a .git directory
            paths_to_avoid = [f.entity_path for f in files if ".git" in str(f.entity_path).split(os.path.sep)]
            if paths_to_avoid:
                files = [f for f in files if f.entity_path not in paths_to_avoid]
                communication.warn(
                    "Ignored adding paths under a .git directory:\n\t" + "\n\t".join(str(p) for p in paths_to_avoid)
                )

            files_to_commit = {f.get_absolute_commit_path(client.path) for f in files}

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

            if with_metadata:
                dataset.update_metadata_from(with_metadata)
        database_dispatcher.current_database.commit()
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


def _process_urls(
    client: "LocalClient",
    dataset: Dataset,
    urls: List[str],
    destination: Path,
    ref: Optional[str] = None,
    sources: Optional[List[Union[str, Path]]] = None,
    destination_names: Optional[List[str]] = None,
    checksums: Optional[List[str]] = None,
    external: bool = False,
    extract: bool = False,
    is_import: bool = False,
    is_renku_import: bool = False,
    repository: Optional[Repository] = None,
    dataset_datadir: Optional[str] = None,
) -> List[NewDatasetFile]:
    """Process file URLs for adding to a dataset."""
    files = []
    tracked_external_warnings = []

    if is_import:  # Importing a non-git dataset
        if not destination_names:
            raise errors.ParameterError("'destination_names' has to be set when using 'is_import=True'")
        files = _add_from_urls(
            client, urls=urls, destination_names=destination_names, destination=destination, extract=extract
        )
    elif is_renku_import:
        assert len(urls) == 1, f"Only a single URL must be specified when importing from Renku: {urls}"
        assert repository is not None, "Repository must be passed for Renku imports"
        assert dataset_datadir, "Source dataset's datadir is not set"

        files = _add_from_renku(
            client=client,
            url=urls[0],
            sources=cast(List[str], sources) or [],
            destination=destination,
            checksums=checksums,
            repository=repository,
            dataset_datadir=dataset_datadir,
        )
    else:
        for url in urls:
            is_remote, is_git, url = check_url(url)
            if is_git and is_remote:  # Remote repository
                new_files = _add_from_git(
                    client=client,
                    url=url,
                    sources=sources,
                    destination=destination,
                    ref=ref,
                    repository=repository,
                )
            else:
                if sources:
                    raise errors.UsageError("Cannot use '-s/--src/--source' with URLs or local files.")

                if not is_remote:  # Local path, might be a repository
                    if is_git:
                        communication.warn(
                            "Adding data from local Git repository: Use remote's Git URL instead to enable lineage "
                            "information and updates."
                        )
                    u = urllib.parse.urlparse(url)
                    new_files, warnings = _add_from_local(
                        client, dataset=dataset, path=u.path, external=external, destination=destination
                    )
                    tracked_external_warnings.extend(warnings)
                else:  # Remote URL
                    new_files = _add_from_url(client=client, url=url, destination=destination, extract=extract)

            files.extend(new_files)

    if tracked_external_warnings:
        message = "\n\t".join(tracked_external_warnings)
        communication.warn(f"Warning: The following files cannot be added as external:\n\t{message}")

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
    dataset_datadir = client.path / get_dataset_data_dir(client, dataset)
    # NOTE: Make sure that dataset's data dir exists because we check for existence of a destination later to decide
    # what will be its name
    dataset_datadir.mkdir(parents=True, exist_ok=True)

    destination = destination or ""
    relative_path = cast(str, get_relative_path(destination, base=dataset_datadir, strict=True))
    return dataset_datadir / relative_path


def _check_ignored_files(client: "LocalClient", files_to_commit: Set[str], files: List[NewDatasetFile]):
    """Check if any files added were ignored."""
    ignored_files = set(client.find_ignored_paths(*files_to_commit))
    if ignored_files:
        ignored_sources = []
        for file in files:
            if file.get_absolute_commit_path(client.path) in ignored_files:
                ignored_sources.append(file.source)

        communication.warn(
            "Theses paths are ignored by one of your .gitignore files (use '--force' flag if you really want to add "
            "them):\n\t" + "\n\t".join([str(p) for p in ignored_sources])
        )

        files_to_commit = files_to_commit.difference(ignored_files)
        files = [f for f in files if f.get_absolute_commit_path(client.path) not in ignored_files]

    return files, files_to_commit


def _check_existing_files(
    client: "LocalClient", dataset: Dataset, files_to_commit: Set[str], files: List[NewDatasetFile]
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


def move_files_to_dataset(client: "LocalClient", files: List[NewDatasetFile]):
    """Copy/Move files into a dataset's directory."""
    for file in files:
        if not file.has_action:
            continue

        # Remove existing file if any; required as a safety-net to avoid corrupting external files
        delete_file(file.destination, follow_symlinks=True)
        file.destination.parent.mkdir(parents=True, exist_ok=True)

        if file.action == AddAction.COPY:
            shutil.copy(file.source, file.destination)
        elif file.action == AddAction.MOVE:
            shutil.move(file.source, file.destination, copy_function=shutil.copy)  # type: ignore
        elif file.action == AddAction.SYMLINK:
            create_external_file(client=client, target=file.source, path=file.destination)
        else:
            raise errors.OperationError(f"Invalid action {file.action}")


def _generate_dataset_files(
    client: "LocalClient", dataset: Dataset, files: List[NewDatasetFile], clear_files_before: bool = False
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


def _add_from_git(
    client: "LocalClient",
    url: str,
    sources: Optional[List[Union[Path, str]]],
    destination: Path,
    ref: Optional[str] = None,
    repository: Optional[Repository] = None,
) -> List[NewDatasetFile]:
    """Process adding resources from another git repository."""
    from renku.core.management.client import LocalClient

    destination_exists = destination.exists()
    destination_is_dir = destination.is_dir()

    if not repository:
        repository = clone_repository(
            url=url,
            path=get_cache_directory_for_repository(client=client, url=url),
            checkout_revision=ref,
            depth=None,
            clean=True,
        )

    def check_sources_are_within_remote_repo():
        if not sources:
            return
        for source in sources:
            if not is_subpath(path=source, base=repository.path):
                raise errors.ParameterError(f"Path '{source}' is not within the repository")

    def get_source_paths() -> Set[Path]:
        """Return all paths from the repo that match a source pattern."""
        if not sources:
            return set(repository.path.glob("*"))  # type: ignore

        paths = set()
        for source in sources:
            # NOTE: Normalized source to resolve .. references (if any). This preserves wildcards.
            normalized_source = os.path.normpath(source)
            absolute_source = os.path.join(repository.path, normalized_source)  # type: ignore
            # NOTE: Path.glob("root/**") does not return correct results (e.g. it include ``root`` in the result)
            subpaths = set(Path(p) for p in glob.glob(absolute_source))
            if len(subpaths) == 0:
                raise errors.ParameterError("No such file or directory", param_hint=str(source))
            paths |= subpaths

        return paths

    def should_copy(source_paths: List[Path]) -> bool:
        n_paths = len(source_paths)
        has_multiple_sources = n_paths > 1
        source_is_dir = has_multiple_sources or (n_paths == 1 and source_paths[0].is_dir())

        if source_is_dir and destination_exists and not destination_is_dir:
            raise errors.ParameterError(f"Destination is not a directory: '{destination}'")

        return has_multiple_sources or (destination_exists and destination_is_dir)

    def get_metadata(src: Path, dst: Path) -> Optional[NewDatasetFile]:
        path_in_src_repo = src.relative_to(repository.path)  # type: ignore
        path_in_dst_repo = dst.relative_to(client.path)

        already_copied = path_in_dst_repo in new_files  # A path with the same destination is already copied
        new_files[path_in_dst_repo].append(path_in_src_repo)
        if already_copied:
            return None

        checksum = repository.get_object_hash(revision="HEAD", path=path_in_src_repo)  # type: ignore
        if not checksum:
            raise errors.FileNotFound(f"Cannot find '{file}' in the remote project")

        return NewDatasetFile(
            entity_path=path_in_dst_repo,
            url=remove_credentials(url),
            based_on=RemoteEntity(checksum=checksum, path=path_in_src_repo, url=url),
            action=AddAction.MOVE,
            source=src,
            destination=dst,
        )

    check_sources_are_within_remote_repo()

    results = []
    new_files: Dict[Path, List[Path]] = defaultdict(list)

    paths = get_source_paths()
    LocalClient(path=repository.path).pull_paths_from_storage(*paths)
    is_copy = should_copy(list(paths))

    for path in paths:
        dst_root = destination / path.name if is_copy else destination

        for file in get_files(path):
            src = file
            relative_path = file.relative_to(path)
            dst = dst_root / relative_path

            metadata = get_metadata(src, dst)
            if metadata:
                results.append(metadata)

    duplicates = [v for v in new_files.values() if len(v) > 1]
    if duplicates:
        files = {str(p) for paths in duplicates for p in paths}
        files_str = "/n/t".join(sorted(files))
        communication.warn(f"The following files overwrite each other in the destination project:/n/t{files_str}")

    return results


def _add_from_renku(
    client: "LocalClient",
    url: str,
    sources: List[str],
    destination: Path,
    repository: Repository,
    dataset_datadir: str,
    checksums: Optional[List[str]] = None,
) -> List[NewDatasetFile]:
    """Process adding resources from another git repository."""
    from renku.core.management.client import LocalClient

    assert destination.exists() and destination.is_dir(), "Destination dir must exist when importing a dataset"
    if checksums is not None:
        assert len(checksums) == len(sources), "Each source must have a corresponding checksum"  # type: ignore

    def add_file(src_entity_path: str, content_path: Path, checksum) -> None:
        """
        Create a NewDatasetFile.

        Args:
            src_entity_path: Entity path from the source dataset which is a relative path.
            content_path: Absolute path of the file content when copied with a checksum.
            checksum: Entity checksum.
        """
        try:
            relative_path = Path(src_entity_path).relative_to(dataset_datadir)
        except ValueError:  # Files that are not in dataset's data directory
            relative_path = Path(src_entity_path)

        dst = destination / relative_path
        path_in_dst_repo = dst.relative_to(client.path)

        already_copied = path_in_dst_repo in new_files  # A path with the same destination is already copied
        new_files[path_in_dst_repo].append(src_entity_path)
        if already_copied:
            return

        if is_external_file(path=src_entity_path, client_path=repository.path):  # type: ignore
            source = (repository.path / src_entity_path).resolve()
            action = AddAction.SYMLINK
        else:
            source = content_path
            action = AddAction.MOVE

        checksum = checksum or repository.get_object_hash(revision="HEAD", path=src_entity_path)  # type: ignore
        if not checksum:
            raise errors.FileNotFound(f"Cannot find '{file}' in the remote project")

        new_file = NewDatasetFile(
            entity_path=path_in_dst_repo,
            url=remove_credentials(url),
            based_on=RemoteEntity(checksum=checksum, path=src_entity_path, url=url),
            action=action,
            source=source,
            destination=dst,
        )
        results.append(new_file)

    results: List[NewDatasetFile] = []
    new_files: Dict[Path, List[str]] = defaultdict(list)

    if checksums is None:
        LocalClient(path=repository.path).pull_paths_from_storage(*(repository.path / p for p in sources))

        for file in sources:
            add_file(file, content_path=repository.path / file, checksum=None)
    else:  # NOTE: Renku dataset import with a tag
        content_path_root = make_project_temp_dir(client.path)
        content_path_root.mkdir(parents=True, exist_ok=True)
        filename = 1

        # NOTE: This is required to enable LFS filters when getting file content
        repository.install_lfs(skip_smudge=False)  # type: ignore
        # NOTE: Git looks at the current attributes files when loading LFS files which won't includes deleted files, so,
        # we need to include all files that were in LFS at some point
        git_attributes = repository.get_historical_changes_patch(".gitattributes")
        all_additions = [a.replace("+", "", 1) for a in git_attributes if a.startswith("+") and "filter=lfs" in a]
        (repository.path / ".gitattributes").write_text(os.linesep.join(all_additions))

        for file, checksum in zip(sources, checksums):  # type: ignore
            content_path = content_path_root / str(filename)
            filename += 1

            try:
                repository.copy_content_to_file(path=file, checksum=checksum, output_path=content_path)  # type: ignore
            except errors.FileNotFound:
                raise errors.FileNotFound(f"Cannot find '{file}' with hash '{checksum}' in the remote project")

            add_file(file, content_path=content_path, checksum=checksum)

    duplicates = [v for v in new_files.values() if len(v) > 1]
    if duplicates:
        files = {str(p) for paths in duplicates for p in paths}
        files_str = "/n/t".join(sorted(files))
        communication.warn(f"The following files overwrite each other in the destination project:/n/t{files_str}")

    return results


def _add_from_urls(
    client: "LocalClient", urls: List[str], destination: Path, destination_names: List[str], extract: bool
) -> List[NewDatasetFile]:
    """Add files from urls."""
    if destination.exists() and not destination.is_dir():
        raise errors.ParameterError(f"Destination is not a directory: '{destination}'")

    destination.mkdir(parents=True, exist_ok=True)

    listeners = communication.get_listeners()

    def subscribe_communication_listeners(function, **kwargs):
        try:
            for communicator in listeners:
                communication.subscribe(communicator)
            return function(**kwargs)
        finally:
            for communicator in listeners:
                communication.unsubscribe(communicator)

    files = []
    max_workers = min((os.cpu_count() or 1) - 1, 4) or 1
    with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:
        futures = {
            executor.submit(
                subscribe_communication_listeners,
                _add_from_url,
                client=client,
                url=url,
                destination=destination,
                extract=extract,
                filename=name,
                multiple=True,
            )
            for url, name in zip(urls, destination_names)
        }

        for future in concurrent.futures.as_completed(futures):
            files.extend(future.result())

    return files


def _add_from_url(
    client: "LocalClient", url: str, destination: Path, extract: bool, filename=None, multiple: bool = False
) -> List[NewDatasetFile]:
    """Process adding from url and return the location on disk."""
    from renku.core.util import requests

    url = provider_check(url)

    try:
        start = time.time() * 1e3

        tmp_root, paths = requests.download_file(
            base_directory=client.renku_path / CACHE, url=url, filename=filename, extract=extract
        )

        exec_time = (time.time() * 1e3 - start) // 1e3
        # If execution time was less or equal to zero seconds,
        # block the thread a bit to avoid being rate limited.
        if exec_time == 0:
            time.sleep(min((os.cpu_count() or 1) - 1, 4) or 1)
    except errors.RequestError as e:  # pragma nocover
        raise errors.OperationError("Cannot download from {}".format(url)) from e

    paths = [p for p in paths if not p.is_dir()]

    if len(paths) > 1 or multiple:
        if destination.exists() and not destination.is_dir():
            raise errors.ParameterError(f"Destination is not a directory: '{destination}'")
        destination.mkdir(parents=True, exist_ok=True)
    elif len(paths) == 1:
        tmp_root = paths[0].parent if destination.exists() else paths[0]

    paths = [(src, destination / src.relative_to(tmp_root)) for src in paths if not src.is_dir()]

    return [
        NewDatasetFile(
            entity_path=dst.relative_to(client.path),
            url=remove_credentials(url),
            action=AddAction.MOVE,
            source=src,
            destination=dst,
        )
        for src, dst in paths
    ]


def _add_from_local(
    client: "LocalClient", dataset: Dataset, path: Union[str, Path], external: bool, destination: Path
) -> Tuple[List[NewDatasetFile], List[str]]:
    """Add a file or directory from a local filesystem."""
    action = AddAction.SYMLINK if external else AddAction.COPY
    absolute_dataset_data_dir = (client.path / get_dataset_data_dir(client, dataset)).resolve()
    source_root = Path(get_absolute_path(path))
    is_within_repo = is_subpath(path=source_root, base=client.path)
    warnings = []

    def check_recursive_addition(src: Path):
        if src.resolve() == absolute_dataset_data_dir:
            raise errors.ParameterError(f"Cannot recursively add path containing dataset's data directory: {path}")

    def get_destination_root():
        destination_exists = destination.exists()
        destination_is_dir = destination.is_dir()

        if client.is_protected_path(source_root):
            raise errors.ProtectedFiles([source_root])

        check_recursive_addition(source_root)

        if not source_root.exists():
            raise errors.ParameterError(f"Cannot find source file: {path}")
        if source_root.is_dir() and destination_exists and not destination_is_dir:
            raise errors.ParameterError(f"Cannot copy directory '{path}' to non-directory '{destination}'")

        return destination / source_root.name if destination_exists and destination_is_dir else destination

    def get_metadata(src: Path) -> NewDatasetFile:
        is_tracked = client.repository.contains(src)

        if is_tracked or (is_within_repo and not external):
            path_in_repo = src.relative_to(client.path)
            if external:
                warnings.append(str(path_in_repo))
            return NewDatasetFile(
                entity_path=path_in_repo,
                url=str(path_in_repo),
                action=AddAction.NONE,
                source=path_in_repo,
                destination=path_in_repo,
            )
        else:
            relative_path = src.relative_to(source_root)
            dst = destination_root / relative_path

            return NewDatasetFile(
                entity_path=dst.relative_to(client.path),
                url=os.path.relpath(src, client.path),
                action=action,
                source=src,
                destination=dst,
            )

    destination_root = get_destination_root()

    if source_root.is_dir():
        metadata = []
        for file in source_root.rglob("*"):
            if client.is_protected_path(file):
                raise errors.ProtectedFiles([file])

            if file.is_dir():
                check_recursive_addition(file)
                continue
            metadata.append(get_metadata(file))

        return metadata, warnings
    else:
        return [get_metadata(source_root)], warnings
