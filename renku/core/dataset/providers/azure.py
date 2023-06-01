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
"""Azure dataset provider."""

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
from renku.core.util.metadata import get_canonical_key, prompt_for_credentials
from renku.core.util.urls import get_scheme
from renku.domain_model.project_context import project_context
from renku.infrastructure.storage.factory import StorageFactory

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import DatasetAddMetadata
    from renku.domain_model.dataset import Dataset


class AzureProvider(ProviderApi, StorageProviderInterface, AddProviderInterface):
    """Azure provider."""

    priority = ProviderPriority.HIGHEST
    name = "Azure"
    is_remote = True

    def __init__(self, uri: str):
        super().__init__(uri=uri)

        account, endpoint, container, _ = parse_azure_uri(uri=self.uri)

        self._account: str = account
        self._endpoint: str = endpoint
        self._container = container

    @staticmethod
    def supports(uri: str) -> bool:
        """Whether or not this provider supports a given URI."""
        return get_scheme(uri) == "azure"

    @staticmethod
    def supports_storage(uri: str) -> bool:
        """Whether or not this provider supports a given URI storage."""
        return AzureProvider.supports(uri=uri)

    def get_metadata(
        self, uri: str, destination: Path, dataset_add_action: DatasetAddAction = DatasetAddAction.NONE, **kwargs
    ) -> List["DatasetAddMetadata"]:
        """Get metadata of files that will be added to a dataset."""
        return get_metadata(provider=self, uri=uri, destination=destination, dataset_add_action=dataset_add_action)

    def convert_to_storage_uri(self, uri: str) -> str:
        """Convert backend-specific URI to a URI that is usable by the IStorage implementation."""
        _, _, container, path = parse_azure_uri(uri=uri)
        return f"azure://{container}/{path}"

    def get_credentials(self) -> "AzureCredentials":
        """Return an instance of provider's credential class."""
        return AzureCredentials(provider=self)

    def get_storage(self, credentials: Optional["ProviderCredentials"] = None) -> "IStorage":
        """Return the storage manager for the provider."""
        azure_configuration = {
            "type": "azureblob",
        }

        if not credentials:
            credentials = self.get_credentials()
            prompt_for_credentials(credentials)

        return StorageFactory.get_storage(
            storage_scheme="azure",
            provider=self,
            credentials=credentials,
            configuration=azure_configuration,
        )

    @property
    def account(self) -> str:
        """Azure account name."""
        return self._account

    @property
    def endpoint(self) -> str:
        """Return Azure container endpoint."""
        return self._endpoint

    @property
    def container(self) -> str:
        """Return Azure container name."""
        return self._container

    def on_create(self, dataset: "Dataset") -> None:
        """Hook to perform provider-specific actions on a newly-created dataset."""
        credentials = self.get_credentials()
        prompt_for_credentials(credentials)
        storage = self.get_storage(credentials=credentials)

        # NOTE: The underlying rclone tool cannot tell if a directory within an Azure container exists or not
        if not storage.exists(self.uri):
            raise errors.ParameterError(f"Azure container '{self.container}' doesn't exists.")

        project_context.repository.add_ignored_pattern(pattern=str(dataset.get_datadir()))


class AzureCredentials(ProviderCredentials):
    """Azure-specific credentials."""

    def __init__(self, provider: AzureProvider):
        super().__init__(provider=provider)

        # NOTE: Set account name so that users don't need to re-enter it
        self.data[get_canonical_key("Account")] = self.provider.account

    @staticmethod
    def get_credentials_names() -> Tuple[str, ...]:
        """Return a tuple of the required credentials for a provider."""
        return "Account", "Key"

    @property
    def provider(self) -> AzureProvider:
        """Return the associated provider instance."""
        return cast(AzureProvider, self._provider)

    def get_credentials_section_name(self) -> str:
        """Get section name for storing credentials.

        NOTE: This methods should be overridden by subclasses to allow multiple credentials per providers if needed.
        """
        return f"{self.provider.account}.{self.provider.endpoint}"


def parse_azure_uri(uri: str) -> Tuple[str, str, str, str]:
    """Extract account, endpoint, container, and path within the container from a given URI.

    NOTE: We support azure://<account-name>.<endpoint>/<container-name>/<path> or
    azure://<account-name>/<container-name>/<path>.
    """
    parsed_uri = urllib.parse.urlparse(uri)

    if parsed_uri.scheme.lower() != "azure":
        raise errors.ParameterError(
            f"Invalid scheme in Azure URI: {uri}.\n"
            "Valid format is 'azure://<account-name>/<container-name>/<path>' or "
            "'azure://<account-name>.<endpoint>/<container-name>/<path>'",
            show_prefix=False,
        )

    account, _, endpoint = parsed_uri.netloc.partition(".")
    path = parsed_uri.path.strip("/")
    container, _, path = path.partition("/")

    if not account or not container:
        raise errors.ParameterError(
            f"Missing account and/or container name in Azure URI: {uri}.\n"
            "Valid format is 'azure://<account-name>/<container-name>/<path>' or "
            "'azure://<account-name>.<endpoint>/<container-name>/<path>'",
            show_prefix=False,
        )

    endpoint = endpoint.lower() or "blob.core.windows.net"

    return account, endpoint, container, path.strip("/")
