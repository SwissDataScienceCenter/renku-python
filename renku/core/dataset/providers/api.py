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
"""API for providers."""

import abc
from enum import IntEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, List, Optional

from renku.core import errors

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import (
        DatasetAddMetadata,
        ProviderDataset,
        ProviderDatasetFile,
        ProviderParameter,
    )
    from renku.core.management.client import LocalClient
    from renku.domain_model.dataset import Dataset, DatasetTag


class ProviderPriority(IntEnum):
    """Defines the order in which a provider is checked to see if it supports a URI."""

    HIGHEST = 1
    HIGHER = 2
    HIGH = 3
    NORMAL = 4
    LOW = 5
    LOWER = 6
    LOWEST = 7


class ProviderApi(abc.ABC):
    """Interface defining provider methods."""

    priority: Optional[ProviderPriority] = None
    name: Optional[str] = None

    def __init_subclass__(cls, **kwargs):
        for required_property in ("priority", "name"):
            if getattr(cls, required_property, None) is None:
                raise NotImplementedError(f"{required_property} must be set for {cls}")

    def __repr__(self):
        return f"<DatasetProvider {self.name}>"

    @staticmethod
    @abc.abstractmethod
    def supports(uri: str) -> bool:
        """Whether or not this provider supports a given URI."""
        raise NotImplementedError

    @staticmethod
    def supports_add() -> bool:
        """Whether this provider supports adding data to datasets."""
        return False

    @staticmethod
    def supports_export() -> bool:
        """Whether this provider supports dataset export."""
        return False

    @staticmethod
    def supports_import() -> bool:
        """Whether this provider supports dataset import."""
        return False

    @staticmethod
    def add(client: "LocalClient", uri: str, destination: Path, **kwargs) -> List["DatasetAddMetadata"]:
        """Add files from a URI to a dataset."""
        raise NotImplementedError

    def get_exporter(self, dataset: "Dataset", *, tag: Optional["DatasetTag"], **kwargs) -> "ExporterApi":
        """Get export manager."""
        raise NotImplementedError

    def get_importer(self, uri, **kwargs) -> "ImporterApi":
        """Get import manager."""
        raise NotImplementedError

    @staticmethod
    def get_add_parameters() -> List["ProviderParameter"]:
        """Returns parameters that can be set for add."""
        return []

    @staticmethod
    def get_export_parameters() -> List["ProviderParameter"]:
        """Returns parameters that can be set for export."""
        return []

    @staticmethod
    def get_import_parameters() -> List["ProviderParameter"]:
        """Returns parameters that can be set for import."""
        return []


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
            raise errors.ImportError("Dataset is not fetched")

        return self._provider_dataset

    @property
    def provider_dataset_files(self) -> List["ProviderDatasetFile"]:
        """Return list of dataset files. This is only valid after a call to ``fetch_provider_dataset``."""
        if self._provider_dataset_files is None:
            raise errors.ImportError("Dataset is not fetched")

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
    def download_files(self, client: "LocalClient", destination: Path, extract: bool) -> List["DatasetAddMetadata"]:
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
