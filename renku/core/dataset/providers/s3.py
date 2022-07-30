# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""S3 dataset provider."""

from pathlib import Path
import re
import urllib
from typing import List, TYPE_CHECKING, Tuple, Type

from renku.core import errors
from renku.core.dataset.providers.api import ProviderApi, ProviderCredentials, ProviderPriority
from renku.core.dataset.providers.models import DatasetAddAction, DatasetAddMetadata
from renku.core.plugin import hookimpl
from renku.core.util.dispatcher import get_repository, get_storage
from renku.core.util.metadata import prompt_for_credentials
from renku.core.util.urls import get_scheme
from renku.domain_model.dataset import RemoteEntity
from renku.domain_model.dataset_provider import IDatasetProviderPlugin
from renku.infrastructure.repository import Repository

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import ProviderParameter
    from renku.core.management.client import LocalClient
    from renku.domain_model.dataset import Dataset


class S3Provider(ProviderApi, IDatasetProviderPlugin):
    """S3 provider."""

    priority = ProviderPriority.NORMAL
    name = "S3"

    def __init__(self, uri: str):
        super().__init__(uri=uri)
        bucket, _ = extract_bucket_and_path(uri=self.uri)
        self._bucket: str = bucket

    @staticmethod
    def supports(uri: str) -> bool:
        """Whether or not this provider supports a given URI."""
        return get_scheme(uri) == "s3"

    @staticmethod
    def supports_create() -> bool:
        """Whether this provider supports creating a dataset."""
        return True

    @staticmethod
    def supports_add() -> bool:
        """Whether this provider supports adding data to datasets."""
        return True

    @staticmethod
    def add(client: "LocalClient", uri: str, destination: Path, **kwargs) -> List["DatasetAddMetadata"]:
        """Add files from a URI to a dataset."""
        provider = S3Provider(uri=uri)
        credentials = S3Credentials(provider=provider)
        prompt_for_credentials(credentials)

        storage = get_storage(provider=provider, credentials=credentials)
        storage.exists(uri)
        storage.mount(uri, mount_location=destination)

        hashes = storage.get_hashes(uri=uri)
        return [
            DatasetAddMetadata(
                entity_path=Path(destination) / hash.path,
                url=hash.base_uri,
                action=DatasetAddAction.NONE,
                based_on=RemoteEntity(checksum=hash.hash, url=hash.base_uri, path=hash.path),
                source=Path(hash.full_uri),
                destination=Path(destination),
            )
            for hash in hashes
        ]

    @property
    def bucket(self) -> str:
        """Return S3 bucket name."""
        return self._bucket

    def on_create(self, dataset: "Dataset") -> None:
        """Hook to perform provider-specific actions on a newly-created dataset."""
        credentials = S3Credentials(provider=self)
        prompt_for_credentials(credentials)

        storage = get_storage(provider=self, credentials=credentials)

        # NOTE: The underlying rclone tool cannot tell if a directory within a S3 bucket exists or not
        if not storage.exists(self.uri):
            raise errors.ParameterError(f"S3 bucket '{self.bucket}' doesn't exists.")

        repository = get_repository()
        repository.add_ignored_pattern(pattern=dataset.get_datadir())

    @classmethod
    @hookimpl
    def dataset_provider(cls) -> "Type[S3Provider]":
        """The definition of the provider."""
        return cls


class S3Credentials(ProviderCredentials):
    """S3-specific credentials."""

    def __init__(self, provider: S3Provider):
        super().__init__(provider=provider)

    @staticmethod
    def get_credentials_names() -> Tuple[str, ...]:
        """Return list of the required credentials for a provider."""
        return "Access Key ID", "Secret Access Key"


def extract_bucket_and_path(uri: str) -> Tuple[str, str]:
    """Extract bucket name and path within the bucket from a given URI.

    NOTE: We only support s3://<bucket-name>/<path> at the moment.
    """
    parsed_uri = urllib.parse.urlparse(uri)

    if parsed_uri.scheme.lower() != "s3" or not parsed_uri.netloc:
        raise errors.ParameterError(f"Invalid S3 URI: {uri}")

    return parsed_uri.netloc, parsed_uri.path
