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
"""Web dataset provider."""

import concurrent.futures
import os
import urllib
from pathlib import Path
from typing import TYPE_CHECKING, List, Tuple
from urllib.parse import urlparse

from renku.core import errors
from renku.core.constant import CACHE
from renku.core.dataset.providers.api import AddProviderInterface, ProviderApi, ProviderPriority
from renku.core.util import communication
from renku.core.util.contexts import wait_for
from renku.core.util.dataset import check_url
from renku.core.util.urls import remove_credentials
from renku.domain_model.project_context import project_context

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import DatasetAddMetadata


class WebProvider(ProviderApi, AddProviderInterface):
    """A provider for downloading data from web URLs."""

    priority = ProviderPriority.LOWEST
    name = "Web"

    @staticmethod
    def supports(uri: str) -> bool:
        """Whether or not this provider supports a given URI."""
        is_remote, is_git = check_url(uri)
        return is_remote and not is_git

    def add(
        self,
        uri: str,
        destination: Path,
        *,
        extract: bool = False,
        filename: str = None,
        multiple: bool = False,
        **kwargs,
    ) -> List["DatasetAddMetadata"]:
        """Add files from a URI to a dataset."""
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
    project_path: Path,
    uri: str,
    destination: Path,
    extract: bool = False,
    filename: str = None,
    multiple: bool = False,
    delay: float = 0,
) -> List["DatasetAddMetadata"]:
    """Download a file from a URI and return its metadata."""
    from renku.core.dataset.providers.models import DatasetAddAction, DatasetAddMetadata
    from renku.core.util import requests

    uri = requests.get_redirect_url(uri)  # TODO: Check that this is not duplicate
    uri = _provider_check(uri)

    with project_context.with_path(project_path):
        try:
            # NOTE: If execution time was less than the delay, block the request until delay seconds are passed
            with wait_for(delay):
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
    n_cpus = os.cpu_count() or 1
    max_workers = min(n_cpus + 4, 8)
    with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:
        futures = {
            executor.submit(
                subscribe_communication_listeners,
                download_file,
                project_path=project_context.path,
                uri=url,
                destination=destination,
                extract=extract,
                filename=name,
                multiple=True,
                delay=max_workers,  # NOTE: Rate limit to 1 request/second
            )
            for url, name in zip(urls, names)
        }

        for future in concurrent.futures.as_completed(futures):
            files.extend(future.result())

    return files
