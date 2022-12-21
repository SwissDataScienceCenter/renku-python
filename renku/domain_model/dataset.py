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
"""Models representing datasets."""

import copy
import os
import posixpath
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Union, cast
from urllib.parse import quote, urlparse
from uuid import uuid4

import marshmallow

from renku.core import errors
from renku.core.util.datetime8601 import fix_datetime, local_now, parse_date
from renku.core.util.git import get_entity_from_revision
from renku.core.util.metadata import is_external_file
from renku.core.util.urls import get_path, get_slug
from renku.core.util.util import NO_VALUE
from renku.domain_model.project_context import project_context
from renku.infrastructure.immutable import Immutable, Slots
from renku.infrastructure.persistent import Persistent

if TYPE_CHECKING:
    from renku.domain_model.entity import Entity
    from renku.domain_model.provenance.agent import Person
    from renku.domain_model.provenance.annotation import Annotation


def is_dataset_name_valid(name: str) -> bool:
    """Check if name is a valid slug."""
    return name is not None and name == get_slug(name, lowercase=False)


def generate_default_name(title: str, version: str = None) -> str:
    """Get dataset name."""
    max_length = 24
    # For compatibility with older versions use title as name if it is valid; otherwise, use encoded title
    if is_dataset_name_valid(title):
        return title

    slug = get_slug(title)
    name = slug[:max_length]

    if version:
        max_version_length = 10
        version_slug = get_slug(version)[:max_version_length]
        name = f"{name[:-(len(version_slug) + 1)]}_{version_slug}"

    return get_slug(name)


class Url:
    """Represents a schema URL reference."""

    def __init__(
        self,
        *,
        id: Optional[str] = None,
        url: Optional[Union[str, Dict[str, str]]] = None,
        url_str: Optional[str] = None,
        url_id: Optional[str] = None,
    ):
        self.id: str
        self.url: Union[str, Dict[str, str]]
        self.url_str: Optional[str] = url_str
        self.url_id: Optional[str] = url_id

        if not url:
            self.url = self._get_default_url()
        else:
            self.url = url
            if isinstance(self.url, dict):
                if "_id" in self.url:
                    self.url["@id"] = self.url.pop("_id")
                self.url_id = self.url["@id"]
                self.url_str = None
            elif isinstance(self.url, str):
                self.url_str = self.url
                self.url_id = None

        if not id or id.startswith("_:"):
            self.id = Url.generate_id(url_str=self.url_str, url_id=self.url_id)
        else:
            self.id = id

    def __repr__(self) -> str:
        return f"<Url {self.value}>"

    @staticmethod
    def generate_id(url_str, url_id):
        """Generate an identifier for Url."""
        url = url_str or url_id
        id = urlparse(url)._replace(scheme="").geturl().strip("/") if url else uuid4().hex
        id = quote(id, safe="/")

        return f"/urls/{id}"

    @property
    def value(self) -> str:
        """Returns the url value as string."""
        return cast(str, self.url_str or self.url_id)

    def _get_default_url(self):
        """Define default value for url field."""
        if self.url_str:
            return self.url_str
        elif self.url_id:
            return {"@id": self.url_id}
        else:
            raise NotImplementedError("Either url_id or url_str has to be set")


class DatasetTag(Persistent):
    """Represents a Tag of an instance of a dataset."""

    def __init__(
        self,
        *,
        dataset_id: Url,
        date_created: Optional[datetime] = None,
        description: Optional[str] = None,
        id: Optional[str] = None,
        name: str,
    ):
        if not id:
            id = DatasetTag.generate_id(dataset_id=dataset_id.value, name=name)

        self.dataset_id: Url = dataset_id
        self.date_created: datetime = parse_date(date_created) or local_now()
        self.description: Optional[str] = description
        self.id: str = id
        self.name: str = name

    @staticmethod
    def generate_id(dataset_id: str, name: str) -> str:
        """Define default value for id field."""
        identifier = Path(dataset_id).name
        name = quote(f"{name}@{identifier}", safe="")
        return f"/dataset-tags/{name}"


class Language(Immutable):
    """Represent a language of an object."""

    __slots__ = ("alternate_name", "name")

    def __init__(self, name: str, alternate_name: Optional[str] = None, id: Optional[str] = None):
        id = id or Language.generate_id(name)
        super().__init__(alternate_name=alternate_name, id=id, name=name)

    @staticmethod
    def generate_id(name: str) -> str:
        """Generate @id field."""
        name = quote(name, safe="")
        return f"/languages/{name}"


class ImageObject(Slots):
    """Represents a schema.org `ImageObject`."""

    __slots__ = ("content_url", "id", "position")

    id: str
    content_url: str
    position: str

    def __init__(self, *, content_url: str, id: str, position: int):
        id = get_path(id)
        super().__init__(content_url=content_url, position=position, id=id)

    @staticmethod
    def generate_id(dataset_id: str, position: int) -> str:
        """Generate @id field."""
        return f"{dataset_id}/images/{position}"

    @property
    def is_absolute(self):
        """Whether content_url is an absolute or relative url."""
        return bool(urlparse(self.content_url).netloc)


class RemoteEntity(Slots):
    """Reference to an Entity in a remote repository."""

    __slots__ = ("checksum", "id", "path", "url")

    def __init__(self, *, checksum: str, id: str = None, path: Union[Path, str], url: str):
        super().__init__()
        self.checksum: str = checksum
        self.id: str = id or RemoteEntity.generate_id(checksum=checksum, path=path, url=url)
        self.path: str = str(path)
        self.url: str = url

    @staticmethod
    def generate_id(checksum: str, path: Union[Path, str], url: str) -> str:
        """Generate an id."""
        parsed_url = urlparse(url)
        prefix = quote(posixpath.join(parsed_url.netloc, parsed_url.path))
        path = quote(str(path))
        return f"/remote-entities/{prefix}/{checksum}/{path}"

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, RemoteEntity):
            return False
        return self.checksum == other.checksum and self.path == other.path and self.url == other.url

    def __hash__(self):
        return hash((self.checksum, self.path, self.url))


class DatasetFile(Slots):
    """A file in a dataset."""

    __slots__ = ("based_on", "date_added", "date_removed", "entity", "id", "is_external", "source")

    def __init__(
        self,
        *,
        entity: "Entity",
        based_on: Optional[RemoteEntity] = None,
        date_added: Optional[datetime] = None,
        date_removed: Optional[datetime] = None,
        id: Optional[str] = None,
        is_external: Optional[bool] = False,
        source: Optional[Union[Path, str]] = None,
    ):
        from renku.domain_model.entity import Entity

        assert entity is None or isinstance(entity, Entity), f"Invalid entity type: '{entity}'"

        super().__init__()

        self.based_on: Optional[RemoteEntity] = based_on
        self.date_added: datetime = fix_datetime(date_added) or local_now()
        self.date_removed: Optional[datetime] = fix_datetime(date_removed)
        self.entity: "Entity" = entity
        self.id: str = id or DatasetFile.generate_id()
        self.is_external: bool = is_external or False
        self.source: Optional[str] = str(source)

    @classmethod
    def from_path(cls, path: Union[str, Path], source=None, based_on: Optional[RemoteEntity] = None) -> "DatasetFile":
        """Return an instance from a path."""
        from renku.domain_model.entity import NON_EXISTING_ENTITY_CHECKSUM, Entity

        # NOTE: Data is added from an external storage and isn't pulled yet
        if based_on and not (project_context.path / path).exists():
            checksum = based_on.checksum if based_on.checksum else NON_EXISTING_ENTITY_CHECKSUM
            id = Entity.generate_id(checksum=checksum, path=path)
            entity = Entity(id=id, checksum=checksum, path=path)
        else:
            entity = get_entity_from_revision(repository=project_context.repository, path=path, bypass_cache=True)

        is_external = is_external_file(path=path, project_path=project_context.path)
        return cls(entity=entity, is_external=is_external, source=source, based_on=based_on)

    @staticmethod
    def generate_id():
        """Generate an identifier for DatasetFile.

        NOTE: ID should not rely on Entity properties because the same Entity can be added and removed multiple times.
        So, it should be marked by different DatasetFiles.
        """
        return f"/dataset-files/{uuid4().hex}"

    @classmethod
    def from_dataset_file(cls, other: "DatasetFile") -> "DatasetFile":
        """Return a copy with a different id."""
        self = other.copy()
        self.id = DatasetFile.generate_id()

        return self

    def copy(self) -> "DatasetFile":
        """Return a clone of this object."""
        return copy.copy(self)

    def is_equal_to(self, other: "DatasetFile"):
        """Compare content.

        NOTE: id is generated randomly and should not be included in this comparison.
        """
        return (
            self.based_on == other.based_on
            and self.date_added == other.date_added
            and self.date_removed == other.date_removed
            and self.entity == other.entity
            and self.is_external == other.is_external
            and self.source == other.source
        )

    def remove(self, date: datetime = None):
        """Create a new instance and mark it as removed."""
        date_removed = fix_datetime(date) or local_now()
        self.date_removed = date_removed

    def is_removed(self) -> bool:
        """Return true if dataset is removed and should not be accessed."""
        return self.date_removed is not None


class Dataset(Persistent):
    """Represent a dataset."""

    date_modified: Optional[datetime] = None  # type: ignore
    storage: Optional[str] = None
    datadir: Optional[str] = None

    def __init__(
        self,
        *,
        annotations: Optional[List["Annotation"]] = None,
        creators: Optional[List["Person"]] = None,
        datadir: Optional[Path] = None,
        dataset_files: Optional[List[DatasetFile]] = None,
        date_created: Optional[datetime] = None,
        date_published: Optional[datetime] = None,
        date_removed: Optional[datetime] = None,
        date_modified: Optional[datetime] = None,
        derived_from: Optional[Url] = None,
        description: Optional[str] = None,
        id: Optional[str] = None,
        identifier: Optional[str] = None,
        images: Optional[List[ImageObject]] = None,
        in_language: Optional[Language] = None,
        initial_identifier: Optional[str] = None,
        keywords: Optional[List[str]] = None,
        license: Optional[str] = None,
        name: Optional[str] = None,
        project_id: Optional[str] = None,
        same_as: Optional[Url] = None,
        storage: Optional[str] = None,
        title: Optional[str] = None,
        version: Optional[str] = None,
    ):
        if not name:
            assert title, "Either 'name' or 'title' must be set."
            name = generate_default_name(title, version)

        self._validate_name(name)
        self._validate_creator(creators)

        # if `date_published` is set, we are probably dealing with an imported dataset so `date_created` is not needed
        if date_published:
            date_created = None
        else:
            date_created = fix_datetime(date_created) or local_now()
        if initial_identifier is None:
            assert identifier is None, "Initial identifier can be None only when creating a new Dataset."
            initial_identifier = identifier = uuid4().hex

        self.identifier = identifier or uuid4().hex
        self.id = id or Dataset.generate_id(identifier=self.identifier)
        self.name: str = name

        self.creators: List["Person"] = creators or []
        # `dataset_files` includes existing files and those that have been removed in the previous version
        self.dataset_files: List[DatasetFile] = dataset_files or []
        self.date_created: Optional[datetime] = date_created
        self.date_modified: datetime = date_modified or local_now()
        self.date_published: Optional[datetime] = fix_datetime(date_published)
        self.date_removed: Optional[datetime] = fix_datetime(date_removed)
        self.derived_from: Optional[Url] = derived_from
        self.description: Optional[str] = description
        self.images: List[ImageObject] = images or []
        self.in_language: Optional[Language] = in_language
        self.initial_identifier: str = initial_identifier
        self.keywords: List[str] = keywords or []
        self.license: Optional[str] = license
        self.project_id: Optional[str] = project_id
        self.same_as: Optional[Url] = same_as
        self.storage: Optional[str] = storage
        self.title: Optional[str] = title
        self.version: Optional[str] = version
        self.annotations: List["Annotation"] = annotations or []

        if datadir:
            self.datadir: Optional[str] = str(datadir)

    @staticmethod
    def generate_id(identifier: str) -> str:
        """Generate an identifier for Dataset."""
        return f"/datasets/{identifier}"

    @staticmethod
    def _validate_name(name):
        if not is_dataset_name_valid(name):
            raise errors.ParameterError(f"Invalid dataset name: '{name}'")

    @staticmethod
    def _validate_creator(creators):
        from renku.domain_model.provenance.agent import Person, SoftwareAgent

        creators = creators or []
        for creator in creators:
            if not isinstance(creator, (Person, SoftwareAgent)):
                raise ValueError(f"Invalid creator type: {creator}")

    @property
    def files(self) -> List[DatasetFile]:
        """Return list of existing files."""
        return [f for f in self.dataset_files if not f.is_removed()]

    @property
    def creators_csv(self):
        """Comma-separated list of creators associated with dataset."""
        return ", ".join(creator.name for creator in self.creators)

    @property
    def creators_full_csv(self):
        """Comma-separated list of creators with full identity."""
        return ", ".join(creator.full_identity for creator in self.creators)

    @property
    def keywords_csv(self):
        """Comma-separated list of keywords associated with dataset."""
        return ", ".join(self.keywords)

    def get_datadir(self) -> Path:
        """Return dataset's data directory."""
        if self.datadir:
            return Path(self.datadir)

        return Path(os.path.join(project_context.datadir, self.name))

    def __repr__(self) -> str:
        return f"<Dataset {self.identifier} {self.name}>"

    def is_derivation(self) -> bool:
        """Return if a dataset has correct derived_from."""
        return self.derived_from is not None and not self.same_as and self.id != self.derived_from.url_id

    def copy(self) -> "Dataset":
        """Return a clone of this dataset."""
        try:
            self.unfreeze()
            dataset = copy.copy(self)
        finally:
            self.freeze()

        dataset.annotations = [a.copy() for a in self.annotations]
        dataset.creators = self.creators.copy()
        dataset.dataset_files = [f.copy() for f in self.dataset_files]
        dataset.images = list(dataset.images or [])
        dataset.keywords = list(dataset.keywords or [])
        return dataset

    def replace_identifier(self, identifier: str = None):
        """Replace dataset's identifier and update relevant fields.

        NOTE: Call this only for newly-created/-imported datasets that don't have a mutability chain because it sets
        `initial_identifier`.
        """
        assert self.derived_from is None, (
            f"Replacing identifier of dataset '{self.name}:{self.identifier}' "
            f"that is derived from {self.derived_from.url_id}"
        )

        self._assign_new_identifier(identifier)
        # NOTE: Do not unset `same_as` because it can be set for imported datasets

    def derive_from(
        self, dataset: "Dataset", creator: Optional["Person"], identifier: str = None, date_created: datetime = None
    ):
        """Make `self` a derivative of `dataset` and update related fields."""
        assert dataset is not None, "Cannot derive from None"
        assert self is not dataset, f"Cannot derive from the same dataset '{self.name}:{self.identifier}'"
        assert not identifier or self.id != identifier, f"Cannot derive from the same id '{self.name}:{identifier}'"

        self._assign_new_identifier(identifier)
        # NOTE: Setting `initial_identifier` is required for migration of broken projects
        self.initial_identifier = dataset.initial_identifier
        self.derived_from = Url(url_id=dataset.id)
        self.same_as = None
        self.date_created = date_created or dataset.date_created
        self.date_modified = local_now()
        self.date_published = dataset.date_published

        if creator and hasattr(creator, "email") and not any(c for c in self.creators if c.email == creator.email):
            self.creators.append(creator)

    def _assign_new_identifier(self, identifier: Optional[str]):
        identifier = identifier or uuid4().hex
        self.initial_identifier = identifier
        self.identifier = identifier
        self.id = Dataset.generate_id(identifier)
        # NOTE: We also need to re-assign the _p_oid since identifier has changed
        self.reassign_oid()

    def remove(self, date: datetime = None):
        """Mark the dataset as removed."""
        self.date_removed = fix_datetime(date) or local_now()

    def is_removed(self) -> bool:
        """Return true if dataset is removed."""
        return self.date_removed is not None

    def find_file(self, path: Union[Path, str]) -> Optional[DatasetFile]:
        """Find a file in the dataset using its relative path."""
        path = str(path)
        for file in self.dataset_files:
            if str(file.entity.path) == path and not file.is_removed():
                return file

        return None

    def update_files_from(self, current_dataset: "Dataset", date: Optional[datetime] = None):
        """Check `current_files` to reuse existing entries and mark removed files."""
        new_files: Dict[str, DatasetFile] = {f.entity.path: f for f in self.files}
        current_files: Dict[str, DatasetFile] = {f.entity.path: f for f in current_dataset.files}

        files = []

        for path, file in new_files.items():
            # Use existing entries from `current_files` to avoid creating new ids
            current_file = current_files.pop(path, None)
            if current_file and file.is_equal_to(current_file):
                files.append(current_file)
            else:
                files.append(file)

        # NOTE: Whatever remains in `current_files` are removed in the newer version
        for removed_file in current_files.values():
            removed_file = DatasetFile.from_dataset_file(removed_file)
            removed_file.remove(date)
            files.append(removed_file)

        self.dataset_files = files

    def update_metadata_from(self, other: "Dataset", exclude=None):
        """Update metadata from another dataset."""
        updatable_fields = [
            "creators",
            "date_created",
            "date_published",
            "derived_from",
            "description",
            "images",
            "in_language",
            "keywords",
            "license",
            "same_as",
            "title",
            "version",
        ]
        for name in updatable_fields:
            value = getattr(other, name)
            if exclude and name in exclude:
                continue
            setattr(self, name, value)

        if self.date_published is not None:
            self.date_created = None

    def update_metadata(self, **kwargs):
        """Updates metadata."""
        editable_attributes = ["creators", "description", "keywords", "title"]
        for name, value in kwargs.items():
            if name not in editable_attributes:
                raise errors.ParameterError(f"Cannot edit field: '{name}'")
            if value is not NO_VALUE and value != getattr(self, name):
                setattr(self, name, value)

    def unlink_file(self, path, missing_ok=False) -> Optional[DatasetFile]:
        """Mark a file as removed using its relative path."""
        assert not self.immutable, f"Dataset is immutable {self}"

        file = self.find_file(path)

        if not file:
            if not missing_ok:
                raise errors.InvalidFileOperation(f"File cannot be found: {path}")
            return None

        file.remove()

        return file

    def add_or_update_files(self, files: Union[DatasetFile, List[DatasetFile]]):
        """Add new files or update existing files."""
        assert not self.immutable, f"Dataset is immutable {self}"

        if isinstance(files, DatasetFile):
            files = [files]

        new_files = []

        for file in files:
            existing_file = self.find_file(file.entity.path)
            if not existing_file:
                new_files.append(file)
            elif file.entity.checksum != existing_file.entity.checksum or file.date_added != existing_file.date_added:
                self.dataset_files.remove(existing_file)
                new_files.append(file)

        if not new_files:
            return

        self.dataset_files += new_files
        self._p_changed = True

    def clear_files(self):
        """Remove all files."""
        self.dataset_files = []


class DatasetCreatorsJson(marshmallow.Schema):
    """Schema for the dataset creators."""

    name = marshmallow.fields.String()
    email = marshmallow.fields.String()
    affiliation = marshmallow.fields.String()


class AnnotationJson(marshmallow.Schema):
    """Schema for Annotations."""

    source = marshmallow.fields.String()
    body = marshmallow.fields.Dict()


class DatasetDetailsJson(marshmallow.Schema):
    """Serialize a dataset to a response object."""

    name = marshmallow.fields.String(required=True)
    version = marshmallow.fields.String(allow_none=True)
    created_at = marshmallow.fields.String(allow_none=True, attribute="date_created")

    title = marshmallow.fields.String()
    creators = marshmallow.fields.List(marshmallow.fields.Nested(DatasetCreatorsJson))
    description = marshmallow.fields.String()
    keywords = marshmallow.fields.List(marshmallow.fields.String())
    identifier = marshmallow.fields.String()
    storage = marshmallow.fields.String()

    annotations = marshmallow.fields.List(marshmallow.fields.Nested(AnnotationJson))

    data_directory = marshmallow.fields.Method("get_datadir")

    @staticmethod
    def get_datadir(obj):
        """Get data directory."""
        if isinstance(obj, dict):
            return str(obj.get("datadir_path", obj.get("datadir", "")))
        if hasattr(obj, "datadir_path"):
            return obj.datadir_path

        return str(obj.get_datadir())


class DatasetFileDetailsJson(marshmallow.Schema):
    """Serialize dataset files to a response object."""

    path = marshmallow.fields.String()
    created = marshmallow.fields.DateTime()
    added = marshmallow.fields.DateTime()

    size = marshmallow.fields.String()
    is_lfs = marshmallow.fields.Boolean()

    dataset_id = marshmallow.fields.String()
    dataset_name = marshmallow.fields.String()

    creators = marshmallow.fields.List(marshmallow.fields.Nested(DatasetCreatorsJson))


class ImageObjectJson(marshmallow.Schema):
    """ImageObject json schema."""

    content_url = marshmallow.fields.String()
    position = marshmallow.fields.Integer()


class ImageObjectRequestJson(marshmallow.Schema):
    """ImageObject json schema."""

    file_id = marshmallow.fields.String()
    content_url = marshmallow.fields.String()
    position = marshmallow.fields.Integer()
    mirror_locally = marshmallow.fields.Bool(dump_default=False)


def get_file_path_in_dataset(dataset: Dataset, dataset_file: DatasetFile) -> Path:
    """Return path of a file relative to dataset's data dir."""
    try:
        return (project_context.path / dataset_file.entity.path).relative_to(
            project_context.path / dataset.get_datadir()
        )
    except ValueError:  # NOTE: File is not in the dataset's data dir
        return Path(dataset_file.entity.path)
