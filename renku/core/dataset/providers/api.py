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
"""API for providers."""

import abc
from collections import UserDict, defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Protocol, Tuple, Type, Union

from renku.core import errors
from renku.core.constant import ProviderPriority
from renku.core.plugin import hookimpl
from renku.core.util import communication
from renku.core.util.os import delete_dataset_file
from renku.core.util.urls import is_uri_subfolder, resolve_uri
from renku.domain_model.constant import NO_VALUE, NoValueType
from renku.domain_model.dataset import RemoteEntity
from renku.domain_model.dataset_provider import IDatasetProviderPlugin
from renku.domain_model.project_context import project_context

if TYPE_CHECKING:
    from renku.core.dataset.providers.models import (
        DatasetAddMetadata,
        DatasetUpdateMetadata,
        ProviderDataset,
        ProviderDatasetFile,
        ProviderParameter,
    )
    from renku.core.interface.storage import IStorage
    from renku.domain_model.dataset import Dataset, DatasetTag
    from renku.infrastructure.immutable import DynamicProxy


class ProviderApi(IDatasetProviderPlugin):
    """Interface defining provider methods."""

    priority: Optional[ProviderPriority] = None
    name: Optional[str] = None
    is_remote: Optional[bool] = None

    def __init__(self, uri: str, **kwargs):
        self._uri: str = uri or ""

    def __init_subclass__(cls, **kwargs):
        for required_property in ("priority", "name", "is_remote"):
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
    def get_metadata(self, uri: str, destination: Path, **kwargs) -> List["DatasetAddMetadata"]:
        """Get metadata of files that will be added to a dataset."""
        raise NotImplementedError

    @abc.abstractmethod
    def update_files(
        self, files: List["DynamicProxy"], dry_run: bool, delete: bool, context: Dict[str, Any], **kwargs
    ) -> List["DatasetUpdateMetadata"]:
        """Update dataset files from the remote provider."""
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
    def get_credentials(self) -> "ProviderCredentials":
        """Return an instance of provider's credential class."""
        raise NotImplementedError

    @abc.abstractmethod
    def convert_to_storage_uri(self, uri: str) -> str:
        """Convert backend-specific URI to a URI that is usable by the IStorage implementation."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_storage(self, credentials: Optional["ProviderCredentials"] = None) -> "IStorage":
        """Return the storage manager for the provider."""
        raise NotImplementedError

    @abc.abstractmethod
    def on_create(self, dataset: "Dataset") -> None:
        """Hook to perform provider-specific actions on a newly-created dataset."""
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def supports_storage(uri: str) -> bool:
        """Whether or not this provider supports a given URI storage."""
        raise NotImplementedError

    def update_files(
        self,
        files: List["DynamicProxy"],
        dry_run: bool,
        delete: bool,
        context: Dict[str, Any],
        **kwargs,
    ) -> List["DatasetUpdateMetadata"]:
        """Update dataset files from the remote provider."""
        from renku.core.dataset.providers.models import DatasetUpdateAction, DatasetUpdateMetadata

        progress_text = f"Checking remote files for updates in dataset {files[0].dataset.slug}"

        results: List[DatasetUpdateMetadata] = []

        try:
            communication.start_progress(progress_text, len(files))

            storage = self.get_storage()

            # group files by storage to efficiently compute hashes
            storage_files_dict: Dict[str, List["DynamicProxy"]] = defaultdict(list)

            for file in files:
                if file.dataset.storage:
                    storage_files_dict[file.dataset.storage].append(file)
                elif file.based_on:
                    if not self.supports_storage(file.based_on.url):
                        raise ValueError(
                            f"Called {getattr(self, 'name', 'Storage')} provider with file {file.entity.path} "
                            "which is not supported by this provider"
                        )
                    storage_files_dict[file.based_on.url].append(file)

            for file_storage, files in storage_files_dict.items():
                hashes = storage.get_hashes(uri=file_storage)
                for file in files:
                    communication.update_progress(progress_text, 1)
                    if not file.based_on:
                        continue

                    dst = project_context.metadata_path.parent / file.entity.path

                    hash = next((h for h in hashes if h.uri == file.based_on.url), None)

                    if hash:
                        if not dry_run and (
                            not file.dataset.storage
                            or not is_uri_subfolder(resolve_uri(file.dataset.storage), file.based_on.url)
                        ):
                            # Redownload downloaded (not mounted) file
                            download_storage = self.get_storage()
                            download_storage.download(file.based_on.url, dst)
                        file.based_on = RemoteEntity(
                            checksum=hash.hash if hash.hash else "", url=hash.uri, path=hash.path
                        )
                        results.append(DatasetUpdateMetadata(entity=file, action=DatasetUpdateAction.UPDATE))
                    else:
                        if (
                            not dry_run
                            and delete
                            and (
                                not file.dataset.storage
                                or not is_uri_subfolder(resolve_uri(file.dataset.storage), file.based_on.url)
                            )
                        ):
                            # Delete downloaded (not mounted) file
                            delete_dataset_file(dst, follow_symlinks=True)
                            project_context.repository.add(dst, force=True)
                        results.append(DatasetUpdateMetadata(entity=file, action=DatasetUpdateAction.DELETE))

        finally:
            communication.finalize_progress(progress_text)

        return results


class CloudStorageProviderType(Protocol):
    """Intersection type for ``mypy`` hinting in storage classes."""

    @property
    def uri(self) -> str:
        """Return provider's URI."""
        raise NotImplementedError

    @abc.abstractmethod
    def convert_to_storage_uri(self, uri: str) -> str:
        """Convert backend-specific URI to a URI that is usable by the IStorage implementation."""
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

    def get_canonical_credentials_names_with_no_value(self) -> Tuple[str, ...]:
        """Return canonical credentials names that can be used as config keys for keys with no valid value."""
        from renku.core.util.metadata import get_canonical_key

        return tuple(get_canonical_key(key) for key in self.get_credentials_names_with_no_value())

    def get_credentials_section_name(self) -> str:
        """Get section name for storing credentials.

        NOTE: This methods should be overridden by subclasses to allow multiple credentials per providers if needed.
        NOTE: Values used in this method shouldn't depend on ProviderCredentials attributes since we don't have those
        attributes when reading credentials. It's OK to use ProviderApi attributes.
        """
        return self.provider.name.lower()  # type: ignore

    def read(self) -> Dict[str, Union[str, NoValueType]]:
        """Read credentials from the config and return them. Set non-existing values to None."""
        from renku.core.util.metadata import read_credentials

        section = self.get_credentials_section_name()

        def read_and_convert_credentials(key) -> Union[str, NoValueType]:
            value = read_credentials(section=section, key=key)
            return NO_VALUE if value is None else value

        data = {key: read_and_convert_credentials(key) for key in self.get_canonical_credentials_names_with_no_value()}
        self.data.update(data)

        return self.data

    def store(self) -> None:
        """Store credentials globally."""
        from renku.core.util.metadata import store_credentials

        section = self.get_credentials_section_name()

        for key, value in self.items():
            if value is not None:
                store_credentials(section=section, key=key, value=value)
