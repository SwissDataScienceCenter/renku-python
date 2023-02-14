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
"""Storage factory implementation."""

from typing import TYPE_CHECKING, Callable, Dict

from renku.core.interface.storage import IStorage, IStorageFactory

if TYPE_CHECKING:
    from renku.core.dataset.providers.api import ProviderApi, ProviderCredentials


class StorageFactory(IStorageFactory):
    """Return an external storage."""

    @staticmethod
    def get_storage(
        storage_scheme: str,
        provider: "ProviderApi",
        credentials: "ProviderCredentials",
        configuration: Dict[str, str],
        uri_convertor: Callable[[str], str],
    ) -> "IStorage":
        """Return a storage that handles provider.

        Args:
            storage_scheme(str): Storage name.
            provider(ProviderApi): The backend provider.
            credentials(ProviderCredentials): Credentials for the provider.
            configuration(Dict[str, str]): Storage-specific configuration that are passed to the IStorage implementation
            uri_convertor(Callable[[str], str]): A function that converts backend-specific URI to a URI that is usable
                by the IStorage implementation.

        Returns:
            An instance of IStorage.
        """
        from .rclone import RCloneStorage

        return RCloneStorage(
            storage_scheme=storage_scheme,
            provider=provider,
            credentials=credentials,
            provider_configuration=configuration,
            provider_uri_convertor=uri_convertor,
        )
