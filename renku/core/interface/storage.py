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
"""External storage interface."""

import abc
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Union

if TYPE_CHECKING:
    from renku.core.dataset.providers.api import ProviderApi, ProviderCredentials


@dataclass
class FileHash:
    """The has for a file at a specific location."""

    base_uri: str
    path: str
    hash: Optional[str] = None
    hash_type: Optional[str] = None
    modified_datetime: Optional[str] = None

    @property
    def full_uri(self) -> str:
        """Return the full uri to the file."""
        return str(Path(self.base_uri) / Path(self.path))


class IStorageFactory(abc.ABC):
    """Interface to get an external storage."""

    @staticmethod
    @abc.abstractmethod
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
        raise NotImplementedError


class IStorage(abc.ABC):
    """Interface for the external storage handler."""

    def __init__(
        self,
        storage_scheme: str,
        provider: "ProviderApi",
        credentials: "ProviderCredentials",
        provider_configuration: Dict[str, str],
        provider_uri_convertor: Callable[[str], str],
    ):
        self._storage_scheme: str = storage_scheme
        self._provider: "ProviderApi" = provider
        self._credentials: "ProviderCredentials" = credentials
        self._provider_configuration: Dict[str, str] = provider_configuration
        self._provider_uri_convertor: Callable[[str], str] = provider_uri_convertor

    @property
    def credentials(self) -> "ProviderCredentials":
        """Return the provider credentials for this storage handler."""
        return self._credentials

    @property
    def provider(self) -> "ProviderApi":
        """Return the dataset provider for this storage handler."""
        return self._provider

    @property
    def storage_scheme(self) -> str:
        """Storage's URI scheme."""
        return self._storage_scheme

    @abc.abstractmethod
    def download(self, uri: str, destination: Union[Path, str]) -> None:
        """Download data from ``uri`` to ``destination``."""
        raise NotImplementedError

    @abc.abstractmethod
    def exists(self, uri: str) -> bool:
        """Checks if a remote storage URI exists."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_configurations(self) -> Dict[str, str]:
        """Get required configurations to access the storage."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_hashes(self, uri: str, hash_type: str = "md5") -> List[FileHash]:
        """Get the hashes of all files at the uri."""
        raise NotImplementedError

    @abc.abstractmethod
    def mount(self, path: Union[Path, str]) -> None:
        """Mount the provider's URI to the given path."""
        raise NotImplementedError

    @abc.abstractmethod
    def upload(self, source: Union[Path, str], uri: str) -> None:
        """Upload data from ``source`` to ``uri``."""
        raise NotImplementedError
