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
"""API for providers."""

import abc
from collections import UserDict
from enum import IntEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type, Union

from renku.core import errors
from renku.core.plugin import hookimpl
from renku.core.util.util import NO_VALUE, NoValueType
from renku.domain_model.dataset_provider import IDatasetProviderPlugin

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import (
        DatasetAddMetadata,
        ProviderDataset,
        ProviderDatasetFile,
        ProviderParameter,
    )
    from renku.core.interface.storage import IStorage
    from renku.domain_model.dataset import Dataset, DatasetTag


class ProviderPriority(IntEnum):
    """Defines the order in which a provider is checked to see if it supports a URI.

    Providers that support more specific URIs should have a higher priority so that they are checked first.
    """

    HIGHEST = 1
    HIGHER = 2
    HIGH = 3
    NORMAL = 4
    LOW = 5
    LOWER = 6
    LOWEST = 7


class ProviderApi(IDatasetProviderPlugin):
    """Interface defining provider methods."""

    priority: Optional[ProviderPriority] = None
    name: Optional[str] = None

    def __init__(self, uri: Optional[str], **kwargs):
        self._uri: str = uri or ""

    def __init_subclass__(cls, **kwargs):
        for required_property in ("priority", "name"):
            if getattr(cls, required_property, None) is None:
                raise NotImplementedError(f"{required_property} must be set for {cls}")

    def __repr__(self):
        return f"<DataProvider {self.name}>"

    @classmethod
    @hookimpl
    def dataset_provider(cls) -> "Type[ProviderApi]":
        """The definition of the provider."""
        return cls

    @staticmethod
    @abc.abstractmethod
    def supports(uri: str) -> bool:
        """Whether or not this provider supports a given URI."""
        raise NotImplementedError

    @property
    def uri(self) -> str:
        """Return provider's URI."""
        return self._uri


class AddProviderInterface(abc.ABC):
    """Interface defining providers that can add data to a dataset."""

    @staticmethod
    def get_add_parameters() -> List["ProviderParameter"]:
        """Returns parameters that can be set for add."""
        return []

    @abc.abstractmethod
    def add(self, uri: str, destination: Path, **kwargs) -> List["DatasetAddMetadata"]:
        """Add files from a URI to a dataset."""
        raise NotImplementedError


class ExportProviderInterface(abc.ABC):
    """Interface defining export providers."""

    @staticmethod
    def get_export_parameters() -> List["ProviderParameter"]:
        """Returns parameters that can be set for export."""
        return []

    @abc.abstractmethod
    def get_exporter(self, dataset: "Dataset", *, tag: Optional["DatasetTag"], **kwargs) -> "ExporterApi":
        """Get export manager."""
        raise NotImplementedError


class ImportProviderInterface(abc.ABC):
    """Interface defining import providers."""

    @staticmethod
    def get_import_parameters() -> List["ProviderParameter"]:
        """Returns parameters that can be set for import."""
        return []

    @abc.abstractmethod
    def get_importer(self, **kwargs) -> "ImporterApi":
        """Get import manager."""
        raise NotImplementedError


class StorageProviderInterface(abc.ABC):
    """Interface defining backend storage providers."""

    @abc.abstractmethod
    def get_storage(self, credentials: Optional["ProviderCredentials"] = None) -> "IStorage":
        """Return the storage manager for the provider."""
        raise NotImplementedError

    @abc.abstractmethod
    def on_create(self, dataset: "Dataset") -> None:
        """Hook to perform provider-specific actions on a newly-created dataset."""
        raise NotImplementedError


class ImporterApi(abc.ABC):
    """Interface defining importer methods."""

    def __init__(self, uri: str, original_uri: str):
        self._uri: str = uri
        self._original_uri: str = original_uri
        self._provider_dataset_files: Optional[List["ProviderDatasetFile"]] = None
        self._provider_dataset: Optional["ProviderDataset"] = None

    @property
    def provider_dataset(self) -> "ProviderDataset":
        """Return the remote dataset. This is only valid after a call to ``fetch_provider_dataset``."""
        if self._provider_dataset is None:
            raise errors.DatasetImportError("Dataset is not fetched")

        return self._provider_dataset

    @property
    def provider_dataset_files(self) -> List["ProviderDatasetFile"]:
        """Return list of dataset files. This is only valid after a call to ``fetch_provider_dataset``."""
        if self._provider_dataset_files is None:
            raise errors.DatasetImportError("Dataset is not fetched")

        return self._provider_dataset_files

    @property
    def uri(self) -> str:
        """Return url of this record."""
        return self._uri

    @property
    def original_uri(self) -> str:
        """Return original URI of this record without any conversion to DOI."""
        return self._original_uri

    @property
    def latest_uri(self) -> str:
        """Get URI of the latest version."""
        raise NotImplementedError

    @property
    def version(self) -> str:
        """Get record version."""
        raise NotImplementedError

    @abc.abstractmethod
    def fetch_provider_dataset(self) -> "ProviderDataset":
        """Deserialize this record to a ``ProviderDataset``."""
        raise NotImplementedError

    @abc.abstractmethod
    def is_latest_version(self) -> bool:
        """Check if record is at last possible version."""
        raise NotImplementedError

    def is_version_equal_to(self, dataset: Any) -> bool:
        """Check if a dataset has the same version as the record."""
        return self.version == getattr(dataset, "version", object())

    @abc.abstractmethod
    def download_files(self, destination: Path, extract: bool) -> List["DatasetAddMetadata"]:
        """Download dataset files from the remote provider."""
        raise NotImplementedError

    @abc.abstractmethod
    def tag_dataset(self, name: str) -> None:
        """Create a tag for the dataset ``name`` if the remote dataset has a tag/version."""
        raise NotImplementedError

    @abc.abstractmethod
    def copy_extra_metadata(self, new_dataset: "Dataset") -> None:
        """Copy provider specific metadata once the dataset is created."""
        raise NotImplementedError


class ExporterApi(abc.ABC):
    """Interface defining exporter methods."""

    def __init__(self, dataset: "Dataset"):
        super().__init__()
        self._dataset: "Dataset" = dataset

    @property
    def dataset(self) -> "Dataset":
        """Return the dataset to be exported."""
        return self._dataset

    @staticmethod
    def requires_access_token() -> bool:
        """Return if export requires an access token."""
        return True

    @abc.abstractmethod
    def set_access_token(self, access_token):
        """Set access token."""
        pass

    @abc.abstractmethod
    def get_access_token_url(self) -> str:
        """Endpoint for creation of access token."""
        pass

    @abc.abstractmethod
    def export(self, **kwargs) -> str:
        """Execute export process."""
        raise NotImplementedError


class ProviderCredentials(abc.ABC, UserDict):
    """Credentials of a provider.

    NOTE: An empty string, "", is a valid value. ``NO_VALUE`` means that the value for a key is not set.
    """

    def __init__(self, provider: ProviderApi):
        super().__init__()
        self._provider: ProviderApi = provider
        self.data: Dict[str, Union[str, NoValueType]] = {
            key: NO_VALUE for key in self.get_canonical_credentials_names()
        }

    @staticmethod
    @abc.abstractmethod
    def get_credentials_names() -> Tuple[str, ...]:
        """Return a tuple of the required credentials for a provider."""
        raise NotImplementedError

    @property
    def provider(self):
        """Return the associated provider instance."""
        return self._provider

    def get_credentials_names_with_no_value(self) -> Tuple[str, ...]:
        """Return a tuple of credential keys that don't have a valid value."""
        return tuple(key for key, value in self.items() if value is NO_VALUE)

    def get_canonical_credentials_names(self) -> Tuple[str, ...]:
        """Return canonical credentials names that can be used as config keys."""
        from renku.core.util.metadata import get_canonical_key

        return tuple(get_canonical_key(key) for key in self.get_credentials_names())

    def get_credentials_section_name(self) -> str:
        """Get section name for storing credentials.

        NOTE: This methods should be overridden by subclasses to allow multiple credentials per providers if needed.
        """
        return self.provider.name.lower()  # type: ignore

    def read(self) -> Dict[str, Union[str, NoValueType]]:
        """Read credentials from the config and return them. Set non-existing values to None."""
        from renku.core.util.metadata import read_credentials

        section = self.get_credentials_section_name()

        def read_and_convert_credentials(key) -> Union[str, NoValueType]:
            value = read_credentials(section=section, key=key)
            return NO_VALUE if value is None else value

        data = {key: read_and_convert_credentials(key) for key in self.get_canonical_credentials_names()}
        self.data.update(data)

        return self.data

    def store(self) -> None:
        """Store credentials globally."""
        from renku.core.util.metadata import store_credentials

        section = self.get_credentials_section_name()

        for key, value in self.items():
            if value is not None:
                store_credentials(section=section, key=key, value=value)
