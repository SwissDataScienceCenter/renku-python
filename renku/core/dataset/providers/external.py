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
"""External dataset provider."""

from __future__ import annotations

import urllib
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Tuple, cast

from renku.core import errors
from renku.core.dataset.providers.api import (
    AddProviderInterface,
    ProviderApi,
    ProviderCredentials,
    ProviderPriority,
    StorageProviderInterface,
)
from renku.core.dataset.providers.common import get_metadata
from renku.core.dataset.providers.models import DatasetAddAction
from renku.core.interface.storage import IStorage
from renku.core.util.os import get_absolute_path
from renku.core.util.urls import get_scheme
from renku.domain_model.project_context import project_context
from renku.infrastructure.storage.factory import StorageFactory

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import DatasetAddMetadata
    from renku.domain_model.dataset import Dataset


class ExternalProvider(ProviderApi, StorageProviderInterface, AddProviderInterface):
    """External provider for remote filesystem."""

    priority = ProviderPriority.HIGHEST
    name = "External"
    is_remote = True

    def __init__(self, uri: str):
        super().__init__(uri=get_uri_absolute_path(uri).rstrip("/"))

    @staticmethod
    def supports(uri: str) -> bool:
        """External doesn't support any URI for addition. It's only for storage backends."""
        return False

    @staticmethod
    def supports_storage(uri: str) -> bool:
        """Whether or not this provider supports a given URI storage."""
        return get_scheme(uri) in ("file", "")

    @property
    def path(self) -> str:
        """Return External path."""
        return self.uri

    def get_metadata(
        self, uri: str, destination: Path, dataset_add_action: DatasetAddAction = DatasetAddAction.NONE, **_
    ) -> List["DatasetAddMetadata"]:
        """Get metadata of files that will be added to a dataset."""
        files = get_metadata(provider=self, uri=uri, destination=destination, dataset_add_action=dataset_add_action)
        for file in files:
            if file.url and not file.url.startswith("file:"):
                file.url = f"file://{file.url}"
                if file.based_on:
                    file.based_on.url = file.url
        return files

    def convert_to_storage_uri(self, uri: str) -> str:
        """Convert backend-specific URI to a URI that is usable by the IStorage implementation."""
        return f"file://{get_uri_absolute_path(uri=uri)}"

    def get_credentials(self) -> "ExternalCredentials":
        """Return an instance of provider's credential class."""
        return ExternalCredentials(provider=self)

    def get_storage(self, credentials: Optional["ProviderCredentials"] = None) -> "IStorage":
        """Return the storage manager for the provider."""
        external_configuration = {
            "type": "local",
        }

        if not credentials:
            credentials = self.get_credentials()

        return StorageFactory.get_storage(
            storage_scheme="file",
            provider=self,
            credentials=credentials,
            configuration=external_configuration,
        )

    def on_create(self, dataset: "Dataset") -> None:
        """Hook to perform provider-specific actions when creating a dataset."""
        storage = self.get_storage(credentials=None)

        # NOTE: The underlying rclone tool cannot tell if a directory within a External bucket exists or not
        if not storage.exists(self.uri):
            raise errors.ParameterError(f"External path '{self.path}' doesn't exists.")

        project_context.repository.add_ignored_pattern(pattern=str(dataset.get_datadir()))


class ExternalCredentials(ProviderCredentials):
    """External-specific credentials."""

    def __init__(self, provider: ExternalProvider):
        super().__init__(provider=provider)

    @staticmethod
    def get_credentials_names() -> Tuple[str, ...]:
        """Return a tuple of the required credentials for a provider."""
        return tuple()

    @property
    def provider(self) -> ExternalProvider:
        """Return the associated provider instance."""
        return cast(ExternalProvider, self._provider)

    def get_credentials_section_name(self) -> str:
        """Get section name for storing credentials.

        NOTE: This methods should be overridden by subclasses to allow multiple credentials per providers if needed.
        """
        return self.provider.uri


def get_uri_absolute_path(uri: str) -> str:
    """Return absolute path to the external directory without resolving symlinks.

    Support formats are ``file://<path>``, file:<path> or just ``<path>``.

    Args:
        uri(str): URI to get path from.

    Returns:
        str: Expanded/non-expanded URI's absolute path.
    """
    return get_absolute_path(urllib.parse.urlparse(uri).path, expand=True)
