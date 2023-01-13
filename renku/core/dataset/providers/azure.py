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
"""Azure dataset provider."""

import urllib
from typing import TYPE_CHECKING, List, Optional, Tuple, cast

from renku.command.command_builder import inject
from renku.core import errors
from renku.core.dataset.providers.api import ProviderApi, ProviderCredentials, ProviderPriority
from renku.core.dataset.providers.cloud import CloudStorageAddProvider
from renku.core.dataset.providers.models import ProviderParameter
from renku.core.interface.storage import IStorage, IStorageFactory
from renku.core.util.metadata import get_canonical_key, prompt_for_credentials
from renku.core.util.urls import get_scheme
from renku.domain_model.project_context import project_context

if TYPE_CHECKING:
    from renku.domain_model.dataset import Dataset


class AzureProvider(ProviderApi, CloudStorageAddProvider):
    """Azure provider."""

    priority = ProviderPriority.HIGHEST
    name = "Azure"

    def __init__(self, uri: Optional[str]):
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
    def get_add_parameters() -> List["ProviderParameter"]:
        """Returns parameters that can be set for add."""
        from renku.core.dataset.providers.models import ProviderParameter

        return [
            ProviderParameter(
                "storage",
                flags=["storage"],
                default=None,
                help="Uri for the Azure container when creating the dataset at the same time when running 'add'",
                multiple=False,
                type=str,
            ),
        ]

    def get_credentials(self) -> "AzureCredentials":
        """Return an instance of provider's credential class."""
        return AzureCredentials(provider=self)

    @inject.autoparams("storage_factory")
    def get_storage(
        self, storage_factory: "IStorageFactory", credentials: Optional["ProviderCredentials"] = None
    ) -> "IStorage":
        """Return the storage manager for the provider."""
        azure_configuration = {
            "type": "azureblob",
            "endpoint": self.endpoint,
        }

        def create_renku_storage_azure_uri(uri: str) -> str:
            """Create an Azure URI to work with the Renku storage handler."""
            _, _, container, path = parse_azure_uri(uri=uri)

            return f"azure://{container}/{path}"

        if not credentials:
            credentials = self.get_credentials()
            prompt_for_credentials(credentials)

        return storage_factory.get_storage(
            storage_scheme="azure",
            provider=self,
            credentials=credentials,
            configuration=azure_configuration,
            uri_convertor=create_renku_storage_azure_uri,
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

    account, _, endpoint = parsed_uri.netloc.partition(".")

    if parsed_uri.scheme.lower() != "azure" or not account:
        raise errors.ParameterError(
            f"Invalid Azure URI: {uri}. Valid format is 'azure://<account-name>.<endpoint>/<container-name>/<path>' or "
            "azure://<account-name>/<container-name>/<path>"
        )

    endpoint = endpoint.lower() or "blob.core.windows.net"

    path = parsed_uri.path.strip("/")
    container, _, path = path.partition("/")

    return account, endpoint, container, path.strip("/")
