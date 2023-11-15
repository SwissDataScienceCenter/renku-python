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
"""Web dataset provider."""

import urllib
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

from renku.core import errors
from renku.core.constant import CACHE
from renku.core.dataset.dataset_add import copy_file
from renku.core.dataset.providers.api import AddProviderInterface, ProviderApi, ProviderPriority
from renku.core.util import communication
from renku.core.util.os import delete_dataset_file
from renku.core.util.urls import check_url, remove_credentials
from renku.core.util.util import parallel_execute
from renku.domain_model.project_context import project_context
from renku.infrastructure.immutable import DynamicProxy

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import DatasetAddMetadata, DatasetUpdateMetadata


class WebProvider(ProviderApi, AddProviderInterface):
    """A provider for downloading data from web URLs."""

    priority = ProviderPriority.LOWEST
    name = "Web"
    is_remote = True

    @staticmethod
    def supports(uri: str) -> bool:
        """Whether or not this provider supports a given URI."""
        is_remote, is_git = check_url(uri)
        return is_remote and not is_git

    def get_metadata(
        self,
        uri: str,
        destination: Path,
        *,
        extract: bool = False,
        filename: Optional[str] = None,
        multiple: bool = False,
        **kwargs,
    ) -> List["DatasetAddMetadata"]:
        """Get metadata of files that will be added to a dataset."""
        dataset = kwargs.get("dataset")
        if dataset and dataset.storage and urlparse(dataset.storage).scheme != urlparse(uri).scheme:
            raise errors.ParameterError(
                f"The scheme of the url {uri} does not match the defined storage url {dataset.storage}."
            )

        return download_file(
            project_path=project_context.path,
            uri=uri,
            destination=destination,
            extract=extract,
            filename=filename,
            multiple=multiple,
        )

    def update_files(
        self,
        files: List[DynamicProxy],
        dry_run: bool,
        delete: bool,
        context: Dict[str, Any],
        **kwargs,
    ) -> List["DatasetUpdateMetadata"]:
        """Update dataset files from the remote provider."""
        from renku.core.dataset.providers.models import DatasetAddMetadata, DatasetUpdateAction, DatasetUpdateMetadata

        progress_text = "Checking for local updates"
        results: List[DatasetUpdateMetadata] = []

        download_cache: Dict[str, DatasetAddMetadata] = {}
        potential_updates: List[Tuple[DatasetAddMetadata, DynamicProxy]] = []

        try:
            communication.start_progress(progress_text, len(files))
            for file in files:
                if not file.source:
                    continue
                destination = project_context.path / file.dataset.get_datadir()
                try:
                    if file.entity.path not in download_cache:
                        downloaded_files = download_file(
                            project_path=project_context.path, uri=file.source, destination=destination
                        )

                        if not any(f.entity_path == file.entity.path for f in downloaded_files):
                            # File probably comes from an extracted download
                            downloaded_files = download_file(
                                project_path=project_context.path,
                                uri=file.source,
                                destination=destination,
                                extract=True,
                            )

                        download_cache.update({str(f.entity_path): f for f in downloaded_files})
                except errors.OperationError:
                    results.append(DatasetUpdateMetadata(entity=file, action=DatasetUpdateAction.DELETE))
                else:
                    metadata = download_cache.get(file.entity.path)

                    if not metadata:
                        results.append(DatasetUpdateMetadata(entity=file, action=DatasetUpdateAction.DELETE))

                        if not dry_run and delete:
                            delete_dataset_file(file.entity.path, follow_symlinks=True)
                            project_context.repository.add(file.entity.path, force=True)
                    else:
                        potential_updates.append((metadata, file))

        finally:
            communication.finalize_progress(progress_text)

        if not potential_updates:
            return results

        check_paths: List[Union[Path, str]] = [
            str(u[0].source.relative_to(project_context.path)) for u in potential_updates
        ]
        # Stage files temporarily so we can get hashes
        project_context.repository.add(*check_paths, force=True)
        hashes = project_context.repository.get_object_hashes(check_paths)
        project_context.repository.remove(*check_paths, index=True)

        for metadata, file in potential_updates:
            if file.entity.checksum != hashes.get(metadata.source):
                results.append(DatasetUpdateMetadata(entity=file, action=DatasetUpdateAction.UPDATE))
                if not dry_run:
                    copy_file(metadata, file.dataset, storage=None)
        return results


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
    from renku.core.util import requests

    url = requests.get_redirect_url(url)
    url = urllib.parse.urlparse(url)

    if "dropbox.com" in url.netloc:
        url = _ensure_dropbox(url)

    return urllib.parse.urlunparse(url)


def download_file(
    uri: str,
    filename: Optional[str] = None,
    *,
    project_path: Path,
    destination: Path,
    extract: bool = False,
    multiple: bool = False,
) -> List["DatasetAddMetadata"]:
    """Download a file from a URI and return its metadata."""
    from renku.core.dataset.providers.models import DatasetAddAction, DatasetAddMetadata
    from renku.core.util import requests

    uri = requests.get_redirect_url(uri)  # TODO: Check that this is not duplicate
    uri = _provider_check(uri)

    with project_context.with_path(project_path):
        try:
            # NOTE: If execution time was less than the delay, block the request until delay seconds are passed
            tmp_root, paths = requests.download_file(
                base_directory=project_context.metadata_path / CACHE, url=uri, filename=filename, extract=extract
            )
        except errors.RequestError as e:  # pragma nocover
            raise errors.OperationError(f"Cannot download from {uri}") from e

        paths = [p for p in paths if not p.is_dir()]

        if len(paths) > 1 or multiple:
            if destination.exists() and not destination.is_dir():
                raise errors.ParameterError(f"Destination is not a directory: '{destination}'")
            destination.mkdir(parents=True, exist_ok=True)
        elif len(paths) == 1:
            tmp_root = paths[0].parent if destination.exists() else paths[0]

        paths = [(src, destination / src.relative_to(tmp_root)) for src in paths if not src.is_dir()]

        return [
            DatasetAddMetadata(
                entity_path=dst.relative_to(project_context.path),
                url=remove_credentials(uri),
                action=DatasetAddAction.MOVE,
                source=src,
                destination=dst,
            )
            for src, dst in paths
        ]


def download_files(
    urls: Tuple[str, ...], destination: Path, names: Tuple[str, ...], extract: bool
) -> List["DatasetAddMetadata"]:
    """Download multiple files and return their metadata."""
    assert len(urls) == len(names), f"Number of URL and names don't match {len(urls)} != {len(names)}"

    if destination.exists() and not destination.is_dir():
        raise errors.ParameterError(f"Destination is not a directory: '{destination}'")

    destination.mkdir(parents=True, exist_ok=True)

    return parallel_execute(
        download_file,
        urls,
        names,
        project_path=project_context.path,
        destination=destination,
        extract=extract,
        multiple=True,
    )
