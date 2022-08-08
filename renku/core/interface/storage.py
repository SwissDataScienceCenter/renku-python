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
from typing import TYPE_CHECKING, List, Optional, Union

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
    def get_storage(provider: "ProviderApi", credentials: "ProviderCredentials") -> "IStorage":
        """Return a storage that handles provider."""
        raise NotImplementedError


class IStorage(abc.ABC):
    """Interface for the external storage handler."""

    def __init__(self, provider: "ProviderApi", credentials: "ProviderCredentials"):
        self._provider: "ProviderApi" = provider
        self._credentials: "ProviderCredentials" = credentials

    @property
    def provider(self) -> "ProviderApi":
        """Return the dataset provider for this storage handler."""
        return self._provider

    @property
    def credentials(self) -> "ProviderCredentials":
        """Return the provider credentials for this storage handler."""
        return self._credentials

    @abc.abstractmethod
    def set_configurations(self):
        """Set required configurations to access the storage."""
        raise NotImplementedError

    @abc.abstractmethod
    def exists(self, uri: str) -> bool:
        """Checks if a remote storage URI exists."""
        raise NotImplementedError

    @abc.abstractmethod
    def mount(self, uri: str, mount_location: Path):
        """Mount the uri at the specified mount location."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_hashes(self, uri: str, sources: Optional[List[Union[str, Path]]]) -> List[FileHash]:
        """Get the hashes of all files at the uri.

        If 'sources' is passed only get the hashes for the specific files at tht uri specified by 'sources'.
        The return value is a list of hashes.
        """
        raise NotImplementedError
