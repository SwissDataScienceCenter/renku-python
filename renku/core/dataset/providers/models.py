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
"""Models for providers."""

import dataclasses
import os
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING, Any, List, NamedTuple, Optional, Type

from humanize import naturalsize
from marshmallow import EXCLUDE

from renku.command.schema.dataset import DatasetSchema
from renku.domain_model.dataset import Dataset
from renku.infrastructure.immutable import DynamicProxy

if TYPE_CHECKING:
    from renku.core.dataset.providers.api import StorageProviderInterface
    from renku.domain_model.dataset import DatasetTag, RemoteEntity


class DatasetAddAction(Enum):
    """Types of action when adding a file to a dataset."""

    COPY = auto()
    MOVE = auto()
    SYMLINK = auto()
    NONE = auto()
    DOWNLOAD = auto()  # For URIs that are from a storage provider
    METADATA_ONLY = auto()  # For URIs that will be added to a dataset with a storage backend
    REMOTE_STORAGE = auto()  # For URIs that are from a remote storage provider


class DatasetUpdateAction(Enum):
    """Types of action when updating a file in a dataset."""

    UPDATE = auto()
    DELETE = auto()


@dataclasses.dataclass
class DatasetAddMetadata:
    """Metadata for a new file that will be added to a dataset."""

    entity_path: Path  # Entity path relative to the project's root
    url: str
    action: DatasetAddAction
    source: Path
    destination: Path
    provider: Optional["StorageProviderInterface"] = None
    based_on: Optional["RemoteEntity"] = None
    size: Optional[int] = None

    @property
    def has_action(self) -> bool:
        """Returns if file's action is not NONE."""
        return self.action != DatasetAddAction.NONE

    @property
    def metadata_only(self) -> bool:
        """Returns if file should be added to a remote storage."""
        return self.action == DatasetAddAction.METADATA_ONLY

    @property
    def from_cloud_storage(self) -> bool:
        """Returns if file is from a cloud storage."""
        return self.action == DatasetAddAction.REMOTE_STORAGE

    def get_absolute_commit_path(self, project_path: Path) -> str:
        """Return path of the file in the repository."""
        return os.path.join(project_path, self.entity_path)


@dataclasses.dataclass
class DatasetUpdateMetadata:
    """Metadata for updating dataset files."""

    entity: DynamicProxy
    action: DatasetUpdateAction


class ProviderParameter(NamedTuple):
    """Provider-specific parameters."""

    name: str
    default: Any = None
    flags: List[str] = []
    help: str = ""
    is_flag: bool = False
    multiple: bool = False
    type: Optional[Type] = None
    metavar: Optional[str] = None


class ProviderDataset(Dataset):
    """A Dataset that is imported from a provider."""

    def __init__(self, **kwargs):
        kwargs.setdefault("initial_identifier", "invalid-initial-id")
        super().__init__(**kwargs)
        self.dataset_files = []  # TODO Make this a property
        self._tag: Optional["DatasetTag"] = None

    @classmethod
    def from_jsonld(cls, data, schema_class=None) -> "ProviderDataset":
        """Create an instance from JSON-LD data."""
        assert isinstance(data, (dict, list)), f"Invalid data type: {data}"

        schema_class = schema_class or DatasetSchema
        self = schema_class(flattened=True).load(data)
        return self

    @classmethod
    def from_dataset(cls, dataset: "Dataset") -> "ProviderDataset":
        """Create an instance from a Dataset."""
        return ProviderDataset(
            annotations=dataset.annotations,
            creators=dataset.creators,
            dataset_files=[],
            date_created=dataset.date_created,
            date_published=dataset.date_published,
            date_removed=dataset.date_removed,
            derived_from=dataset.derived_from,
            description=dataset.description,
            id=dataset.id,
            identifier=dataset.identifier,
            images=dataset.images,
            in_language=dataset.in_language,
            initial_identifier=dataset.initial_identifier,
            keywords=dataset.keywords,
            license=dataset.license,
            slug=dataset.slug,
            project_id=dataset.project_id,
            same_as=dataset.same_as,
            name=dataset.name,
            version=dataset.version,
            storage=dataset.storage,
        )

    @property
    def files(self):
        """Return list of existing files."""
        raise NotImplementedError("ProviderDataset has no files.")

    @property
    def tag(self) -> Optional["DatasetTag"]:
        """Return dataset's tag."""
        return self._tag

    @tag.setter
    def tag(self, value):
        """Set dataset's tag."""
        self._tag = value


class ProviderDatasetFile:
    """Store metadata for dataset files that will be downloaded from a provider."""

    def __init__(
        self, source: Optional[str], filename: str, checksum: str, filesize: Optional[int], filetype: str, path: str
    ):
        self.checksum: str = checksum
        self.filename: str = filename
        self.filetype: str = filetype
        self.path: str = path
        self.filesize: Optional[int] = filesize
        self.filesize_str: Optional[str] = (
            naturalsize(filesize).upper().replace("BYTES", " B") if filesize is not None else None
        )
        self.source: Optional[str] = source


class ProviderDatasetSchema(DatasetSchema):
    """ProviderDataset schema."""

    class Meta:
        """Meta class."""

        model = ProviderDataset
        unknown = EXCLUDE
