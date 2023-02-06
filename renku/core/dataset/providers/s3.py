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
"""S3 dataset provider."""

import urllib
from typing import TYPE_CHECKING, List, Optional, Tuple, cast

from renku.command.command_builder import inject
from renku.core import errors
from renku.core.dataset.providers.api import ProviderApi, ProviderCredentials, ProviderPriority
from renku.core.dataset.providers.cloud import CloudStorageAddProvider
from renku.core.dataset.providers.models import ProviderParameter
from renku.core.interface.storage import IStorage, IStorageFactory
from renku.core.util.metadata import prompt_for_credentials
from renku.core.util.urls import get_scheme
from renku.domain_model.project_context import project_context

if TYPE_CHECKING:
    from renku.domain_model.dataset import Dataset


class S3Provider(ProviderApi, CloudStorageAddProvider):
    """S3 provider."""

    priority = ProviderPriority.HIGHEST
    name = "S3"

    def __init__(self, uri: Optional[str]):
        super().__init__(uri=uri)

        endpoint, bucket, _ = parse_s3_uri(uri=self.uri)

        self._bucket: str = bucket
        self._endpoint: str = endpoint

    @staticmethod
    def supports(uri: str) -> bool:
        """Whether or not this provider supports a given URI."""
        return get_scheme(uri) == "s3"

    @staticmethod
    def get_add_parameters() -> List["ProviderParameter"]:
        """Returns parameters that can be set for add."""
        from renku.core.dataset.providers.models import ProviderParameter

        return [
            ProviderParameter(
                "storage",
                flags=["storage"],
                default=None,
                help="Uri for the S3 bucket when creating the dataset at the same time when running 'add'",
                multiple=False,
                type=str,
            ),
        ]

    def get_credentials(self) -> "S3Credentials":
        """Return an instance of provider's credential class."""
        return S3Credentials(provider=self)

    @inject.autoparams("storage_factory")
    def get_storage(
        self, storage_factory: "IStorageFactory", credentials: Optional["ProviderCredentials"] = None
    ) -> "IStorage":
        """Return the storage manager for the provider."""
        s3_configuration = {
            "type": "s3",
            "provider": "AWS",
            "endpoint": self.endpoint,
        }

        def create_renku_storage_s3_uri(uri: str) -> str:
            """Create a S3 URI to work with the Renku storage handler."""
            _, bucket, path = parse_s3_uri(uri=uri)

            return f"s3://{bucket}/{path}"

        if not credentials:
            credentials = self.get_credentials()
            prompt_for_credentials(credentials)

        return storage_factory.get_storage(
            storage_scheme="s3",
            provider=self,
            credentials=credentials,
            configuration=s3_configuration,
            uri_convertor=create_renku_storage_s3_uri,
        )

    @property
    def bucket(self) -> str:
        """Return S3 bucket name."""
        return self._bucket

    @property
    def endpoint(self) -> str:
        """Return S3 bucket endpoint."""
        return self._endpoint

    def on_create(self, dataset: "Dataset") -> None:
        """Hook to perform provider-specific actions on a newly-created dataset."""
        credentials = self.get_credentials()
        prompt_for_credentials(credentials)
        storage = self.get_storage(credentials=credentials)

        # NOTE: The underlying rclone tool cannot tell if a directory within a S3 bucket exists or not
        if not storage.exists(self.uri):
            raise errors.ParameterError(f"S3 bucket '{self.bucket}' doesn't exists.")

        project_context.repository.add_ignored_pattern(pattern=str(dataset.get_datadir()))


class S3Credentials(ProviderCredentials):
    """S3-specific credentials."""

    def __init__(self, provider: S3Provider):
        super().__init__(provider=provider)

    @staticmethod
    def get_credentials_names() -> Tuple[str, ...]:
        """Return a tuple of the required credentials for a provider."""
        return "Access Key ID", "Secret Access Key"

    @property
    def provider(self) -> S3Provider:
        """Return the associated provider instance."""
        return cast(S3Provider, self._provider)

    def get_credentials_section_name(self) -> str:
        """Get section name for storing credentials.

        NOTE: This methods should be overridden by subclasses to allow multiple credentials per providers if needed.
        """
        return self.provider.endpoint.lower()


def parse_s3_uri(uri: str) -> Tuple[str, str, str]:
    """Extract endpoint, bucket name, and path within the bucket from a given URI.

    NOTE: We only support s3://<endpoint>/<bucket-name>/<path> at the moment.
    """
    parsed_uri = urllib.parse.urlparse(uri)

    endpoint = parsed_uri.netloc
    path = parsed_uri.path.strip("/")

    if parsed_uri.scheme.lower() != "s3" or not endpoint:
        raise errors.ParameterError(f"Invalid S3 URI: {uri}. Valid format is 's3://<endpoint>/<bucket-name>/<path>'")

    bucket, _, path = path.partition("/")

    return endpoint, bucket, path.strip("/")
