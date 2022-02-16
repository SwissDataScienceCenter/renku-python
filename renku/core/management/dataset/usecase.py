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
"""Dataset business logic."""


import concurrent.futures
import os
import shutil
import time
import urllib
import uuid
from glob import glob
from pathlib import Path
from typing import Any, Dict, List, Optional, OrderedDict

from renku.core import errors
from renku.core.management.command_builder.command import inject
from renku.core.management.dataset.constant import SUPPORTED_SCHEMES, renku_dataset_images_path, renku_pointers_path
from renku.core.management.dataset.datasets_provenance import DatasetsProvenance
from renku.core.management.dataset.request_model import ImageRequestModel
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.interface.dataset_gateway import IDatasetGateway
from renku.core.metadata.immutable import DynamicProxy
from renku.core.metadata.repository import Repository
from renku.core.models.dataset import Dataset, DatasetFile, RemoteEntity, get_dataset_data_dir, is_dataset_name_valid
from renku.core.models.provenance.agent import Person
from renku.core.models.provenance.annotation import Annotation
from renku.core.utils import communication
from renku.core.utils.git import clone_repository, get_cache_directory_for_repository, get_git_user
from renku.core.utils.metadata import is_external_file
from renku.core.utils.urls import get_slug, remove_credentials


@inject.autoparams("client_dispatcher")
def create_dataset(
    name: str,
    client_dispatcher: IClientDispatcher,
    title: Optional[str] = None,
    description: Optional[str] = None,
    creators: Optional[List[Person]] = None,
    keywords: Optional[List[str]] = None,
    images: Optional[List[ImageRequestModel]] = None,
    update_provenance: bool = True,
    custom_metadata: Optional[Dict[str, Any]] = None,
):
    """Create a dataset."""
    client = client_dispatcher.current_client

    if not is_dataset_name_valid(name):
        valid_name = get_slug(name, lowercase=False)
        raise errors.ParameterError(f'Dataset name "{name}" is not valid (Hint: "{valid_name}" is valid).')

    datasets_provenance = DatasetsProvenance()

    if datasets_provenance.get_by_name(name=name):
        raise errors.DatasetExistsError(f"Dataset exists: '{name}'")

    if not title:
        title = name

    if creators is None:
        creators = [get_git_user(client.repository)]

    keywords = keywords or []

    annotations = None

    if custom_metadata:
        annotations = [Annotation(id=Annotation.generate_id(), source="renku", body=custom_metadata)]

    dataset = Dataset(
        identifier=None,
        name=name,
        title=title,
        description=description,
        creators=creators,
        keywords=keywords,
        project_id=client.project.id,
        annotations=annotations,
    )

    if images:
        set_dataset_images(client, dataset, images)

    if update_provenance:
        datasets_provenance.add_or_update(dataset)

    return dataset


def set_dataset_images(client, dataset: Dataset, images: List[ImageRequestModel]):
    """Set a dataset's images."""
    if not images:
        images = []

    image_folder = renku_dataset_images_path(client) / dataset.initial_identifier
    image_folder.mkdir(exist_ok=True, parents=True)
    previous_images = dataset.images or []

    dataset.images = []
    images_updated = False
    for img in images:
        img_object = img.to_image_object(dataset)

        if not img_object:
            continue
        dataset.images.append(img_object)
        images_updated = True

    new_urls = [i.content_url for i in dataset.images]

    for prev in previous_images:
        # NOTE: Delete images if they were removed
        if prev.content_url in new_urls or urllib.parse.urlparse(prev.content_url).netloc:
            continue

        path = prev.content_url
        if not os.path.isabs(path):
            path = os.path.normpath(os.path.join(client.path, path))

        os.remove(path)

    return images_updated or dataset.images != previous_images


def update_dataset_custom_metadata(dataset: Dataset, custom_metadata: Dict):
    """Update custom metadata on a dataset."""

    existing_metadata = [a for a in dataset.annotations if a.source != "renku"]

    existing_metadata.append(Annotation(id=Annotation.generate_id(), body=custom_metadata, source="renku"))

    dataset.annotations = existing_metadata


@inject.autoparams("client_dispatcher")
def add_data_to_dataset(
    dataset,
    urls,
    client_dispatcher: IClientDispatcher,
    force=False,
    overwrite=False,
    sources=(),
    destination="",
    ref=None,
    external=False,
    extract=False,
    all_at_once=False,
    destination_names=None,
    repository: Repository = None,
    clear_files_before=False,
):
    """Import the data into the data directory."""
    client = client_dispatcher.current_client

    dataset_datadir = get_dataset_data_dir(client, dataset)

    destination = destination or Path(".")
    destination = _resolve_path(dataset_datadir, destination)
    destination = client.path / dataset_datadir / destination

    if destination.exists() and not destination.is_dir():
        raise errors.ParameterError(f'Destination is not a directory: "{destination}"')

    client.check_external_storage()

    files = []
    if all_at_once:  # Importing a dataset
        files = _add_from_urls(urls=urls, destination_names=destination_names, destination=destination, extract=extract)
    else:
        for url in urls:
            is_remote, is_git, url = _check_url(url)
            if is_git and is_remote:  # Remote repository
                sources = sources or ()
                new_files = _add_from_git(
                    url=url, sources=sources, destination=destination, ref=ref, repository=repository
                )
            else:
                if sources:
                    raise errors.UsageError('Cannot use "--source" with URLs or local files.')

                if not is_remote:  # Local path, might be a repository
                    if is_git:
                        communication.warn(
                            "Adding data from local Git repository: "
                            + "Use remote's Git URL instead to enable "
                            + "lineage information and updates."
                        )
                    u = urllib.parse.urlparse(url)
                    new_files = _add_from_local(
                        client, dataset=dataset, path=u.path, external=external, destination=destination
                    )
                else:  # Remote URL
                    new_files = client._add_from_url(url=url, destination=destination, extract=extract)

            files.extend(new_files)

    # Remove all files that are under a .git directory
    paths_to_avoid = [f["path"] for f in files if ".git" in str(f["path"]).split(os.path.sep)]
    if paths_to_avoid:
        files = [f for f in files if f["path"] not in paths_to_avoid]
        communication.warn(
            "Ignored adding paths under a .git directory:\n  " + "\n  ".join(str(p) for p in paths_to_avoid)
        )

    files_to_commit = {str(client.path / f["path"]) for f in files}

    if not force:
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

    # all files at this point can be force-added

    if not overwrite:
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

    for data in files:
        operation = data.pop("operation", None)
        if not operation:
            continue

        src, dst, action = operation

        # Remove existing file if any
        remove_file(dst)
        dst.parent.mkdir(parents=True, exist_ok=True)

        if action == "copy":
            shutil.copy(src, dst)
        elif action == "move":
            shutil.move(src, dst, copy_function=shutil.copy)
        elif action == "symlink":
            _create_external_file(client, src, dst)
            data["is_external"] = True
        else:
            raise errors.OperationError(f"Invalid action {action}")

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

    # Generate the DatasetFiles
    dataset_files = []
    for data in files:
        dataset_file = DatasetFile.from_path(
            client=client, path=data["path"], source=data["source"], based_on=data.get("based_on")
        )
        dataset_files.append(dataset_file)

    if clear_files_before:
        dataset.clear_files()
    dataset.add_or_update_files(dataset_files)
    datasets_provenance = DatasetsProvenance()
    datasets_provenance.add_or_update(dataset, creator=get_git_user(client.repository))


def _add_from_git(client, url, sources, destination, ref, repository: Repository = None):
    """Process adding resources from another git repository."""
    from renku.core.management.client import LocalClient

    u = urllib.parse.urlparse(url)

    sources = _resolve_paths(u.path, sources)

    if not repository:
        repository = clone_repository(
            url=url,
            path=get_cache_directory_for_repository(client=client, url=url),
            checkout_revision=ref,
            depth=None,
            clean=True,
        )

    repo_path = repository.path

    # Get all files from repo that match sources
    files = set()
    used_sources = set()
    for file in repository.head.commit.traverse():
        path = file.path
        result = _get_src_and_dst(path, repo_path, sources, destination, used_sources)

        if result:
            files.add(result)

    unused_sources = set(sources.keys()) - used_sources
    if unused_sources:
        unused_sources = {str(s) for s in unused_sources}
        raise errors.ParameterError("No such file or directory", param_hint=unused_sources)

    # Create metadata and move files to dataset
    results = []
    remote_client = LocalClient(repo_path)

    remote_client.pull_paths_from_storage(*(src for _, src, _ in files))

    new_files = []

    for path, src, dst in files:
        if not src.is_dir():
            path_in_dst_repo = dst.relative_to(client.path)
            if path_in_dst_repo in new_files:  # A path with the same destination is already copied
                continue

            new_files.append(path_in_dst_repo)

            if is_external_file(path=src, client_path=remote_client.path):
                operation = (src.resolve(), dst, "symlink")
            else:
                operation = (src, dst, "move")

            checksum = remote_client.repository.get_object_hash(revision="HEAD", path=path)
            based_on = RemoteEntity(checksum=checksum, path=path, url=url)

            results.append(
                {
                    "path": path_in_dst_repo,
                    "source": remove_credentials(url),
                    "parent": client,
                    "based_on": based_on,
                    "operation": operation,
                }
            )

    return results


def _add_from_urls(client, urls, destination, destination_names, extract):
    """Add files from urls."""
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
            )
            for url, name in zip(urls, destination_names)
        }

        for future in concurrent.futures.as_completed(futures):
            files.extend(future.result())

    return files


def _add_from_url(client, url, destination, extract, filename=None):
    """Process adding from url and return the location on disk."""
    from renku.core.utils import requests

    url = _provider_check(url)

    try:
        start = time.time() * 1e3

        tmp_root, paths = requests.download_file(
            base_directory=client.renku_path / client.CACHE, url=url, filename=filename, extract=extract
        )

        exec_time = (time.time() * 1e3 - start) // 1e3
        # If execution time was less or equal to zero seconds,
        # block the thread a bit to avoid being rate limited.
        if exec_time == 0:
            time.sleep(min(os.cpu_count() - 1, 4) or 1)
    except errors.RequestError as e:  # pragma nocover
        raise errors.OperationError("Cannot download from {}".format(url)) from e

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


def _add_from_local(client, dataset, path, external, destination):
    """Add a file or directory from a local filesystem."""
    src = Path(os.path.abspath(path))

    if not src.exists():
        raise errors.ParameterError(f"Cannot find file/directory: {path}")

    dst = destination / src.name

    # if we have a directory, recurse
    if src.is_dir():
        if dst.exists() and not dst.is_dir():
            raise errors.ParameterError(f'Cannot copy directory to a file: "{dst}"')
        if src == (client.path / get_dataset_data_dir(client, dataset)).resolve():
            raise errors.ParameterError(f"Cannot add dataset's data directory recursively: {path}")

        if client.is_protected_path(src):
            raise errors.ProtectedFiles([src])

        files = []
        for f in src.iterdir():
            files.extend(
                _add_from_local(client, dataset=dataset, path=os.path.abspath(f), external=external, destination=dst)
            )
        return files
    else:
        # Check if file is in the project and return it
        path_in_repo = None
        if is_external_file(path=src, client_path=client.path):
            path_in_repo = path
        else:
            try:
                path_in_repo = src.relative_to(client.path)
            except ValueError:
                pass
            else:
                if client.is_protected_path(src):
                    raise errors.ProtectedFiles([src])

        if path_in_repo:
            return [{"path": path_in_repo, "source": path_in_repo, "parent": client}]

    action = "symlink" if external else "copy"
    return [
        {
            "path": dst.relative_to(client.path),
            "source": os.path.relpath(str(src), str(client.path)),
            "parent": client,
            "operation": (src, dst, action),
        }
    ]


@inject.autoparams("client_dispatcher", "dataset_gateway")
def move_files(client_dispatcher: IClientDispatcher, dataset_gateway: IDatasetGateway, files, to_dataset):
    """Move files and their metadata from one or more datasets to a target dataset."""
    client = client_dispatcher.current_client

    datasets = [d.copy() for d in dataset_gateway.get_all_datasets()]
    if to_dataset:
        # NOTE: Use the same dataset object or otherwise a race happens if dataset is in both source and destination
        to_dataset: Dataset = next(d for d in datasets if d.name == to_dataset)
    modified_datasets = {}

    progress_name = "Updating dataset metadata"
    communication.start_progress(progress_name, total=len(files))
    try:
        for src, dst in files.items():
            src = src.relative_to(client.path)
            dst = dst.relative_to(client.path)
            # NOTE: Files are moved at this point, so, we use can use dst
            new_dataset_file = DatasetFile.from_path(client, dst)
            for dataset in datasets:
                removed = dataset.unlink_file(src, missing_ok=True)
                if removed:
                    modified_datasets[dataset.name] = dataset
                    new_dataset_file.based_on = removed.based_on
                    new_dataset_file.source = removed.source
                    if not to_dataset:
                        dataset.add_or_update_files(new_dataset_file)

                # NOTE: Update dataset if it contains a destination that is being overwritten
                modified = dataset.find_file(dst)
                if modified:
                    modified_datasets[dataset.name] = dataset
                    dataset.add_or_update_files(new_dataset_file)

            if to_dataset:
                to_dataset.add_or_update_files(new_dataset_file)

            communication.update_progress(progress_name, amount=1)
    finally:
        communication.finalize_progress(progress_name)

    datasets_provenance = DatasetsProvenance()
    for dataset in modified_datasets.values():
        datasets_provenance.add_or_update(dataset, creator=get_git_user(client.repository))
    if to_dataset:
        datasets_provenance.add_or_update(to_dataset, creator=get_git_user(client.repository))


@inject.autoparams("client_dispatcher")
def update_dataset_local_files(client_dispatcher: IClientDispatcher, records: List[DynamicProxy], delete=False):
    """Update files metadata from the git history."""
    client = client_dispatcher.current_client

    updated_files: List[DynamicProxy] = []
    deleted_files: List[DynamicProxy] = []
    progress_text = "Checking for local updates"

    try:
        communication.start_progress(progress_text, len(records))
        check_paths = []
        records_to_check = []

        for file in records:
            communication.update_progress(progress_text, 1)

            if file.based_on or file.is_external:
                continue

            if not (client.path / file.entity.path).exists():
                deleted_files.append(file)
                continue

            check_paths.append(file.entity.path)
            records_to_check.append(file)

        checksums = client.repository.get_object_hashes(check_paths)

        for file in records_to_check:
            current_checksum = checksums.get(file.entity.path)
            if not current_checksum:
                deleted_files.append(file)
            elif current_checksum != file.entity.checksum:
                updated_files.append(file)
    finally:
        communication.finalize_progress(progress_text)

    if updated_files or (deleted_files and delete):
        client._update_datasets_metadata(updated_files, deleted_files, delete)

    return updated_files, deleted_files


@inject.autoparams("client_dispatcher")
def update_dataset_git_files(client_dispatcher: IClientDispatcher, files: List[DynamicProxy], ref, delete=False):
    """Update files and dataset metadata according to their remotes.

    :param files: List of files to be updated
    :param delete: Indicates whether to delete files or not

    :return: List of files that should be deleted
    """
    from renku.core.management.client import LocalClient

    client = client_dispatcher.current_client

    visited_repos = {}
    updated_files: List[DynamicProxy] = []
    deleted_files: List[DynamicProxy] = []

    progress_text = "Checking files for updates"

    try:
        communication.start_progress(progress_text, len(files))
        for file in files:
            communication.update_progress(progress_text, 1)
            if not file.based_on:
                continue

            based_on = file.based_on
            url = based_on.url
            if url in visited_repos:
                remote_repository, remote_client = visited_repos[url]
            else:
                remote_repository = clone_repository(
                    url=url, path=get_cache_directory_for_repository(client=client, url=url), checkout_revision=ref
                )
                remote_client = LocalClient(remote_repository.path)
                visited_repos[url] = remote_repository, remote_client

            checksum = remote_repository.get_object_hash(path=based_on.path, revision="HEAD")
            found = bool(checksum)
            changed = found and based_on.checksum != checksum

            src = remote_repository.path / based_on.path
            dst = client.renku_path.parent / file.entity.path

            if changed:
                if src.exists():
                    # Fetch file if it is tracked by Git LFS
                    remote_client.pull_paths_from_storage(remote_client.path / based_on.path)
                    if is_external_file(path=src, client_path=remote_client.path):
                        remove_file(dst)
                        _create_external_file(client, src.resolve(), dst)
                    else:
                        shutil.copy(src, dst)
                    file.based_on = RemoteEntity(checksum=checksum, path=based_on.path, url=based_on.url)
                    updated_files.append(file)
                else:
                    # File was removed or renamed
                    found = False

            if not found:
                if delete:
                    remove_file(dst)
                deleted_files.append(file)
    finally:
        communication.finalize_progress(progress_text)

    if not updated_files and (not delete or not deleted_files):
        # Nothing to commit or update
        return [], deleted_files

    # Commit changes in files

    file_paths = {str(client.path / f.entity.path) for f in updated_files + deleted_files}
    # Force-add to include possible ignored files that are in datasets
    client.repository.add(*file_paths, force=True)

    client._update_datasets_metadata(updated_files, deleted_files, delete)

    return updated_files, deleted_files


def update_external_files(client, records: List[DynamicProxy]):
    """Update files linked to external storage."""
    updated_files_paths = []
    updated_datasets = {}

    for file in records:
        if file.is_external:
            path = client.path / file.entity.path
            link = path.parent / os.readlink(path)
            pointer_file = client.path / link
            pointer_file = _update_pointer_file(client, pointer_file)
            if pointer_file is not None:
                relative = os.path.relpath(pointer_file, path.parent)
                os.remove(path)
                os.symlink(relative, path)
                updated_files_paths.append(str(path))
                updated_datasets[file.dataset.name] = file.dataset

    if not updated_files_paths:
        return

    client.repository.add(*updated_files_paths, force=True)
    client.repository.add(renku_pointers_path(client), force=True)

    datasets_provenance = DatasetsProvenance()

    for dataset in updated_datasets.values():
        for file in dataset.files:
            if str(client.path / file.entity.path) in updated_files_paths:
                new_file = DatasetFile.from_path(client=client, path=file.entity.path, source=file.source)
                dataset.add_or_update_files(new_file)

        datasets_provenance.add_or_update(dataset, creator=get_git_user(client.repository))


def _create_pointer_file(client, target, checksum=None):
    """Create a new pointer file."""
    target = Path(target).resolve()

    if checksum is None:
        checksum = _calculate_checksum(client.repository, target)
        assert checksum is not None, f"Cannot calculate checksum for '{target}'"

    while True:
        filename = f"{uuid.uuid4()}-{checksum}"
        path = renku_pointers_path(client) / filename
        if not path.exists():
            break

    try:
        os.symlink(target, path)
    except FileNotFoundError:
        raise errors.ParameterError("Cannot find external file {}".format(target))

    return path


def _update_pointer_file(client, pointer_file_path):
    """Update a pointer file."""
    try:
        target = pointer_file_path.resolve(strict=True)
    except FileNotFoundError:
        target = pointer_file_path.resolve()
        raise errors.ParameterError("External file not found: {}".format(target))

    checksum = _calculate_checksum(client.repository, target)
    current_checksum = pointer_file_path.name.split("-")[-1]

    if checksum == current_checksum:
        return

    os.remove(pointer_file_path)
    return _create_pointer_file(client, target, checksum=checksum)


def remove_file(filepath):
    """Remove a file/symlink and its pointer file (for external files)."""
    path = Path(filepath)
    try:
        link = path.parent / os.readlink(path)
    except FileNotFoundError:
        return
    except OSError:  # not a symlink but a normal file
        os.remove(path)
        return

    os.remove(path)

    try:
        os.remove(link)
    except FileNotFoundError:
        pass


def _calculate_checksum(repository, filepath):
    try:
        return repository.hash_objects([filepath])[0]
    except errors.GitCommandError:
        raise


def _create_external_file(client, src, dst):
    """Create a new external file."""
    try:
        pointer_file = _create_pointer_file(client, target=src)
        relative = os.path.relpath(pointer_file, dst.parent)
        os.symlink(relative, dst)
    except OSError as e:
        raise errors.OperationError("Could not create symbolic link") from e


def _get_src_and_dst(path, repo_path, sources, dst_root, used_sources):
    """Get source and destination paths."""
    is_wildcard = False
    matched_pattern = None

    if not sources:
        source = Path(".")
    else:
        source = None
        for s in sources.keys():
            try:
                Path(path).relative_to(s)
            except ValueError:
                if glob.globmatch(path, str(s), flags=glob.GLOBSTAR):
                    is_wildcard = True
                    source = Path(path)
                    used_sources.add(s)
                    matched_pattern = str(s)
                    break
            else:
                source = Path(s)
                used_sources.add(source)
                break

        if not source:
            return

    if is_wildcard:
        # Search to see if a parent of the path matches the pattern and return it
        while glob.globmatch(str(source.parent), matched_pattern, flags=glob.GLOBSTAR) and source != source.parent:
            source = source.parent

    src = repo_path / path
    source_name = source.name
    relative_path = Path(path).relative_to(source)

    if src.is_dir() and is_wildcard:
        sources[source] = None
        used_sources.add(source)

    dst = dst_root / source_name / relative_path

    return path, src, dst


def _ensure_dropbox(url):
    """Ensure dropbox url is set for file download."""
    if not isinstance(url, urllib.parse.ParseResult):
        url = urllib.parse.urlparse(url)

    query = url.query or ""
    if "dl=0" in url.query:
        query = query.replace("dl=0", "dl=1")
    else:
        query += "dl=1"

    url = url._replace(query=query)
    return url


def _provider_check(url):
    """Check additional provider related operations."""
    from renku.core.utils import requests

    url = requests.get_redirect_url(url)
    url = urllib.parse.urlparse(url)

    if "dropbox.com" in url.netloc:
        url = _ensure_dropbox(url)

    return urllib.parse.urlunparse(url)


def _resolve_paths(root_path, paths):
    """Check if paths are within a root path and resolve them."""
    result = OrderedDict()  # Used as an ordered-set
    for path in paths:
        r = _resolve_path(root_path, path)
        result[r] = None
    return result


def _resolve_path(root_path, path):
    """Check if a path is within a root path and resolve it."""
    try:
        root_path = Path(root_path).resolve()
        path = os.path.abspath(root_path / path)
        return Path(path).relative_to(root_path)
    except ValueError:
        raise errors.ParameterError("File {} is not within path {}".format(path, root_path))


def _check_url(url):
    """Check if a url is local/remote and if it contains a git repository."""
    from renku.core.utils import requests

    u = urllib.parse.urlparse(url)

    if u.scheme not in SUPPORTED_SCHEMES:
        raise errors.UrlSchemeNotSupported('Scheme "{}" not supported'.format(u.scheme))

    is_remote = u.scheme not in ("", "file") or url.startswith("git@")
    is_git = False

    if is_remote:
        is_git = u.path.endswith(".git")
        if not is_git:
            url = requests.get_redirect_url(url)
    elif os.path.isdir(u.path) or os.path.isdir(os.path.realpath(u.path)):
        try:
            Repository(u.path, search_parent_directories=True)
        except errors.GitError:
            pass
        else:
            is_git = True

    return is_remote, is_git, url
