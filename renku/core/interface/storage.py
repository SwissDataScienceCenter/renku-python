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
    def credentials(self) -> "ProviderCredentials":
        """Return the provider credentials for this storage handler."""
        return self._credentials

    @property
    def provider(self) -> "ProviderApi":
        """Return the dataset provider for this storage handler."""
        return self._provider

    @abc.abstractmethod
    def copy(self, source: Union[Path, str], destination: Union[Path, str]) -> None:
        """Copy data from ``source`` to ``destination``."""
        raise NotImplementedError

    @abc.abstractmethod
    def exists(self, uri: str) -> bool:
        """Checks if a remote storage URI exists."""
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
    def set_configurations(self) -> None:
        """Set required configurations to access the storage."""
        raise NotImplementedError
