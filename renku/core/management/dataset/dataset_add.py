# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Union

from renku.core import errors
from renku.core.management.command_builder.command import inject
from renku.core.management.dataset.constant import CACHE, renku_pointers_path
from renku.core.management.dataset.context import DatasetContext
from renku.core.management.dataset.datasets_provenance import DatasetsProvenance
from renku.core.management.dataset.pointer_file import create_external_file
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.interface.database_dispatcher import IDatabaseDispatcher
from renku.core.metadata.repository import Repository
from renku.core.models.dataset import Dataset, DatasetFile, RemoteEntity, get_dataset_data_dir
from renku.core.utils import communication, requests
from renku.core.utils.git import clone_repository, get_cache_directory_for_repository, get_git_user
from renku.core.utils.metadata import is_external_file
from renku.core.utils.os import delete_file, get_absolute_path, get_files, get_relative_path, is_subpath
from renku.core.utils.urls import check_url, provider_check, remove_credentials

if TYPE_CHECKING:
    from renku.core.management.client import LocalClient


@inject.autoparams("client_dispatcher", "database_dispatcher")
def add_data_to_dataset(
    dataset_name: str,
    urls: List[str],
    client_dispatcher: IClientDispatcher,
    database_dispatcher: IDatabaseDispatcher,
    force: bool = False,
    create: bool = False,
    overwrite: bool = False,
    sources: Optional[List[str]] = None,
    destination: str = "",
    ref: str = None,
    external: bool = False,
    extract: bool = False,
    all_at_once: bool = False,
    destination_names: List[str] = None,
    repository: Repository = None,
    clear_files_before: bool = False,
    total_size: Optional[int] = None,
    with_metadata: Optional[Dataset] = None,
):
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
            destination = _create_destination_directory(client, dataset, destination)

            client.check_external_storage()

            files = _process_urls(
                client,
                dataset,
                urls,
                destination,
                ref,
                sources,
                destination_names,
                external,
                extract,
                all_at_once,
                repository,
            )

            # Remove all files that are under a .git directory
            paths_to_avoid = [f["path"] for f in files if ".git" in str(f["path"]).split(os.path.sep)]
            if paths_to_avoid:
                files = [f for f in files if f["path"] not in paths_to_avoid]
                communication.warn(
                    "Ignored adding paths under a .git directory:\n  " + "\n  ".join(str(p) for p in paths_to_avoid)
                )

            files_to_commit = {str(client.path / f["path"]) for f in files}

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
                return

            dataset_files = _generate_dataset_files(client, dataset, files, clear_files_before)

            dataset.add_or_update_files(dataset_files)
            datasets_provenance = DatasetsProvenance()
            datasets_provenance.add_or_update(dataset, creator=get_git_user(client.repository))

            if with_metadata:
                dataset.update_metadata_from(with_metadata)
        database_dispatcher.current_database.commit()
        return dataset
    except errors.DatasetNotFound:
        raise errors.DatasetNotFound(
            message='Dataset "{0}" does not exist.\n'
            'Use "renku dataset create {0}" to create the dataset or retry '
            '"renku dataset add {0}" command with "--create" option for '
            "automatic dataset creation.".format(dataset_name)
        )
    except (FileNotFoundError, errors.GitCommandError) as e:
        raise errors.ParameterError("Could not find paths/URLs: \n{0}".format("\n".join(urls))) from e


def _process_urls(
    client: "LocalClient",
    dataset: Dataset,
    urls: List[str],
    destination: Path,
    ref: Optional[str] = None,
    sources: Optional[List[str]] = None,
    destination_names: Optional[List[str]] = None,
    external: bool = False,
    extract: bool = False,
    all_at_once: bool = False,
    repository: Repository = None,
):
    """Process file URLs for adding to a dataset."""
    files = []
    if all_at_once:  # Importing a non-git dataset
        files = _add_from_urls(
            client, urls=urls, destination_names=destination_names, destination=destination, extract=extract
        )
    else:
        for url in urls:
            is_remote, is_git, url = check_url(url)
            if is_git and is_remote:  # Remote repository
                new_files = _add_from_git(
                    client, url=url, sources=sources, destination=destination, ref=ref, repository=repository
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
                    new_files = _add_from_local(
                        client, dataset=dataset, path=u.path, external=external, destination=destination
                    )
                else:  # Remote URL
                    new_files = _add_from_url(client=client, url=url, destination=destination, extract=extract)

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
):
    """Create directory for dataset add."""
    dataset_datadir = client.path / get_dataset_data_dir(client, dataset)
    # NOTE: Make sure that dataset's data dir exists because we check for existence of a destination later to decide
    # what will be its name
    dataset_datadir.mkdir(parents=True, exist_ok=True)

    destination = destination or Path(".")
    destination = get_relative_path(destination, base=dataset_datadir, strict=True)
    destination = dataset_datadir / destination
    return destination


def _check_ignored_files(client: "LocalClient", files_to_commit: Set[str], files: List[Dict]):
    """Check if any files added were ignored."""
    ignored_files = client.find_ignored_paths(*files_to_commit)
    if ignored_files:
        ignored_files = set(ignored_files)
        files_to_commit = files_to_commit.difference(ignored_files)
        ignored_sources = []
        for file_ in files:
            if str(client.path / file_["path"]) in ignored_files:
                operation = file_.get("operation")
                if operation:
                    src, _, _ = operation
                    ignored_sources.append(src)
                else:
                    ignored_sources.append(file_["path"])

        files = [f for f in files if str(client.path / f["path"]) in files_to_commit]
        communication.warn(
            "Theses paths are ignored by one of your .gitignore "
            + 'files (use "--force" flag if you really want to add '
            + "them):\n  "
            + "\n  ".join([str(p) for p in ignored_sources])
        )

    return files, files_to_commit


def _check_existing_files(client: "LocalClient", dataset: Dataset, files_to_commit: Set[str], files: List[Dict]):
    """Check if files added already exist."""
    existing_files = []
    for path in files_to_commit:
        relative_path = Path(path).relative_to(client.path)
        if dataset.find_file(relative_path):
            existing_files.append(path)

    if existing_files:
        files_to_commit = files_to_commit.difference(existing_files)
        files = [f for f in files if str(client.path / f["path"]) in files_to_commit]
        communication.warn(
            "These existing files were not overwritten "
            + '(use "--overwrite" flag to overwrite them):\n  '
            + "\n  ".join([str(p) for p in existing_files])
        )

    return files, files_to_commit


def move_files_to_dataset(client: "LocalClient", files: List[Dict]):
    """Copy/Move files into a dataset's directory."""
    for data in files:
        operation = data.pop("operation", None)
        if not operation:
            continue

        src, dst, action = operation

        # Remove existing file if any; required as a safety-net to avoid corrupting external files
        delete_file(dst, follow_symlinks=True)
        dst.parent.mkdir(parents=True, exist_ok=True)

        if action == "copy":
            shutil.copy(src, dst)
        elif action == "move":
            shutil.move(src, dst, copy_function=shutil.copy)
        elif action == "symlink":
            create_external_file(client, src, dst)
            data["is_external"] = True
        else:
            raise errors.OperationError(f"Invalid action {action}")


def _generate_dataset_files(
    client: "LocalClient", dataset: Dataset, files: List[Dict], clear_files_before: bool = False
):
    """Generate DatasetFile entries from file dict."""
    dataset_files = []
    for data in files:
        dataset_file = DatasetFile.from_path(
            client=client, path=data["path"], source=data["source"], based_on=data.get("based_on")
        )
        dataset_files.append(dataset_file)

    if clear_files_before:
        dataset.clear_files()
    return dataset_files


def _add_from_git(
    client: "LocalClient",
    url: str,
    sources: List[Union[Path, str]],
    destination: Path,
    ref: Optional[str] = None,
    repository: Repository = None,
):
    """Process adding resources from another git repository."""
    from renku.core.management.client import LocalClient

    destination_exists = destination.exists()
    destination_is_dir = destination.is_dir()

    def check_sources_are_within_remote_repo():
        for source in sources:
            if get_relative_path(path=source, base=repository.path) is None:
                raise errors.ParameterError(f"Path '{source}' is not within the repository")

    def get_paths_from_remote_repo() -> Set[Path]:
        """Return all paths from the repo that match a source pattern."""
        if not sources:
            return set(repository.path.glob("*"))

        paths = set()
        for source in sources:
            # NOTE: Normalized source to resolve .. references (if any). This preserves wildcards.
            normalized_source = os.path.normpath(source)
            absolute_source = os.path.join(repository.path, normalized_source)
            # NOTE: Path.glob("root/**") does not return correct results (e.g. it include ``root`` in the result)
            subpaths = set(Path(p) for p in glob.glob(absolute_source))
            if len(subpaths) == 0:
                raise errors.ParameterError("No such file or directory", param_hint=str(source))
            paths |= subpaths

        return paths

    def get_destination_root(n_paths, path: Path):
        has_multiple_paths = n_paths > 1
        multiple_sources = has_multiple_paths or path.is_dir()

        if multiple_sources and destination_exists and not destination_is_dir:
            raise errors.ParameterError(f"Destination is not a directory: '{destination}'")

        return (
            destination / path.name
            if has_multiple_paths or (destination_exists and destination_is_dir)
            else destination
        )

    def get_metadata(src, dst) -> Optional[Dict]:
        path_in_src_repo = src.relative_to(repository.path)
        path_in_dst_repo = dst.relative_to(client.path)

        if path_in_dst_repo in new_files:  # A path with the same destination is already copied
            return

        new_files.add(path_in_dst_repo)

        if is_external_file(path=src, client_path=repository.path):
            operation = (src.resolve(), dst, "symlink")
        else:
            operation = (src, dst, "move")

        checksum = repository.get_object_hash(revision="HEAD", path=path_in_src_repo)
        based_on = RemoteEntity(checksum=checksum, path=path_in_src_repo, url=url)

        return {
            "path": path_in_dst_repo,
            "source": remove_credentials(url),
            "parent": client,
            "based_on": based_on,
            "operation": operation,
        }

    if not repository:
        repository = clone_repository(
            url=url,
            path=get_cache_directory_for_repository(client=client, url=url),
            checkout_revision=ref,
            depth=None,
            clean=True,
        )

    check_sources_are_within_remote_repo()
    paths = get_paths_from_remote_repo()
    n_paths = len(paths)

    LocalClient(repository.path).pull_paths_from_storage(*paths)

    results = []
    new_files = set()
    for path in paths:
        dst_root = get_destination_root(n_paths=n_paths, path=path)

        for file in get_files(path):
            src = file
            relative_path = file.relative_to(path)
            dst = dst_root / relative_path

            metadata = get_metadata(src, dst)
            if metadata:
                results.append(metadata)

    return results


def _add_from_urls(
    client: "LocalClient", urls: List[str], destination: Path, destination_names: List[str], extract: bool
):
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
    max_workers = min(os.cpu_count() - 1, 4) or 1
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
):
    """Process adding from url and return the location on disk."""
    from renku.core.utils import requests

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
            time.sleep(min(os.cpu_count() - 1, 4) or 1)
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
        {
            "operation": (src, dst, "move"),
            "path": dst.relative_to(client.path),
            "source": remove_credentials(url),
            "parent": client,
        }
        for src, dst in paths
    ]


def _add_from_local(
    client: "LocalClient", dataset: Dataset, path: Union[str, Path], external: bool, destination: Path
) -> List[Dict]:
    """Add a file or directory from a local filesystem."""
    action = "symlink" if external else "copy"
    absolute_dataset_data_dir = (client.path / get_dataset_data_dir(client, dataset)).resolve()
    source_root = Path(get_absolute_path(path))
    is_with_repo = is_subpath(path=source_root, base=client.path)

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

    def get_metadata(src: Path) -> Dict:
        if is_with_repo:
            path_in_repo = src.relative_to(client.path)
            return {"path": path_in_repo, "source": path_in_repo, "parent": client}
        else:
            relative_path = src.relative_to(source_root)
            dst = destination_root / relative_path

            return {
                "path": dst.relative_to(client.path),
                "source": os.path.relpath(src, client.path),
                "parent": client,
                "operation": (src, dst, action),
            }

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

        return metadata
    else:
        return [get_metadata(source_root)]
