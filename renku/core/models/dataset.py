# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union
from urllib.parse import quote, urlparse
from uuid import uuid4

from marshmallow import EXCLUDE, pre_dump

from renku.core import errors
from renku.core.management.command_builder.command import inject
from renku.core.metadata.database import Database, Index, Persistent
from renku.core.metadata.immutable import Immutable, Slots
from renku.core.models import datasets as old_datasets
from renku.core.models.calamus import DateTimeList, JsonLDSchema, Nested, Uri, fields, prov, renku, schema
from renku.core.models.datasets import generate_dataset_id, is_dataset_name_valid
from renku.core.models.entities import generate_label
from renku.core.models.entity import Entity, NewEntitySchema
from renku.core.models.provenance import agents as old_agents
from renku.core.models.provenance.agent import Agent, NewPersonSchema, Person, SoftwareAgent
from renku.core.utils import communication
from renku.core.utils.datetime8601 import fix_timezone, local_now, parse_date


class Url:
    """Represents a schema URL reference."""

    def __init__(self, *, id: str = None, url: str = None, url_str: str = None, url_id: str = None):
        self.id: str = id
        self.url: str = url
        self.url_str: str = url_str
        self.url_id: str = url_id

        if not self.url:
            self.url = self.get_default_url()
        elif isinstance(self.url, dict):
            if "_id" in self.url:
                self.url["@id"] = self.url.pop("_id")
            self.url_id = self.url["@id"]
        elif isinstance(self.url, str):
            self.url_str = self.url

        if not self.id or self.id.startswith("_:"):
            self.id = Url.generate_id(url_str=self.url_str, url_id=self.url_id)

    @classmethod
    def from_url(cls, url: Optional[old_datasets.Url]) -> Optional["Url"]:
        """Create from old Url instance."""
        return cls(url=url.url, url_id=url.url_id, url_str=url.url_str) if url else None

    def to_url(self, client) -> old_datasets.Url:
        """Convert to an old Url."""
        return old_datasets.Url(client=client, url=self.url, url_id=self.url_id, url_str=self.url_str)

    @staticmethod
    def generate_id(url_str, url_id):
        """Generate an identifier for Url."""
        url = url_str or url_id
        id = urlparse(url)._replace(scheme="").geturl().strip("/") if url else uuid4().hex
        id = quote(id, safe="/")

        return f"/urls/{id}"

    def get_default_url(self):
        """Define default value for url field."""
        if self.url_str:
            return self.url_str
        elif self.url_id:
            return {"@id": self.url_id}
        else:
            raise NotImplementedError("Either url_id or url_str has to be set")


class DatasetTag(Slots):
    """Represents a Tag of an instance of a dataset."""

    __slots__ = ("commit", "dataset", "date_created", "description", "id", "name")

    def __init__(
        self,
        *,
        commit: str,
        dataset=None,
        date_created: datetime = None,
        description: str = None,
        id: str = None,
        name: str,
    ):
        if not id or id.startswith("_:"):
            id = DatasetTag.generate_id(commit=commit, name=name)

        super().__init__(
            commit=commit,
            dataset=dataset,
            date_created=parse_date(date_created) or local_now(),
            description=description,
            id=id,
            name=name,
        )

    @classmethod
    def from_dataset_tag(cls, tag: Optional[old_datasets.DatasetTag]) -> Optional["DatasetTag"]:
        """Create from old DatasetTag instance."""
        if not tag:
            return
        return cls(
            commit=tag.commit, dataset=tag.dataset, date_created=tag.created, description=tag.description, name=tag.name
        )

    def to_dataset_tag(self, client) -> old_datasets.DatasetTag:
        """Convert to an old DatasetTag."""
        return old_datasets.DatasetTag(
            client=client,
            commit=self.commit,
            dataset=self.dataset,
            created=self.date_created,
            description=self.description,
            name=self.name,
        )

    @staticmethod
    def generate_id(commit: str, name: str) -> str:
        """Define default value for id field."""
        name = quote(f"{name}@{commit}", safe="")
        return f"/dataset-tags/{name}"


class Language(Immutable):
    """Represent a language of an object."""

    __slots__ = ("alternate_name", "name")

    def __init__(self, alternate_name: str = None, id: str = None, name: str = None):
        id = id or Language.generate_id(name)
        super().__init__(alternate_name=alternate_name, id=id, name=name)

    @classmethod
    def from_language(cls, language: Optional[old_datasets.Language]) -> Optional["Language"]:
        """Create from old Language instance."""
        return cls(alternate_name=language.alternate_name, name=language.name) if language else None

    def to_language(self) -> old_datasets.Language:
        """Convert to an old Language."""
        return old_datasets.Language(alternate_name=self.alternate_name, name=self.name)

    @staticmethod
    def generate_id(name: str) -> str:
        """Generate @id field."""
        name = quote(name, safe="")
        return f"/languages/{name}"


class ImageObject(Slots):
    """Represents a schema.org `ImageObject`."""

    __slots__ = ("content_url", "id", "position")

    def __init__(self, *, content_url: str, position: int, id: str = None):
        # TODO: Remove scheme://hostname from id
        super().__init__(content_url=content_url, position=position, id=id)

    @classmethod
    def from_image_object(cls, image_object: Optional[old_datasets.ImageObject]) -> Optional["ImageObject"]:
        """Create from old ImageObject instance."""
        if not image_object:
            return
        return cls(content_url=image_object.content_url, position=image_object.position, id=image_object.id)

    def to_image_object(self) -> old_datasets.ImageObject:
        """Convert to an old ImageObject."""
        return old_datasets.ImageObject(content_url=self.content_url, position=self.position, id=self.id)

    @staticmethod
    def generate_id(dataset: "Dataset", position: int) -> str:
        """Generate @id field."""
        return f"{dataset.id}/images/{position}"

    @property
    def is_absolute(self):
        """Whether content_url is an absolute or relative url."""
        return bool(urlparse(self.content_url).netloc)


class RemoteEntity(Slots):
    """Reference to an Entity in a remote repo."""

    __slots__ = ("commit_sha", "id", "path", "url")

    def __init__(self, *, commit_sha: str, id: str = None, path: Union[Path, str], url: str):
        super().__init__()

        self.commit_sha: str = commit_sha
        self.id: str = id or RemoteEntity.generate_id(commit_sha, path)
        self.path: str = str(path)
        self.url: str = url

    @staticmethod
    def generate_id(commit_sha: str, path: Union[Path, str]) -> str:
        """Generate an id."""
        path = quote(str(path))
        return f"/remote-entity/{commit_sha}/{path}"

    @classmethod
    def from_dataset_file(cls, dataset_file: Optional[old_datasets.DatasetFile]) -> Optional["RemoteEntity"]:
        """Create an instance by converting from renku.core.models.datasets.DatasetFile."""
        if not dataset_file:
            return
        commit_sha = dataset_file._label.rsplit("@", maxsplit=1)[-1]
        return cls(commit_sha=commit_sha, path=dataset_file.path, url=dataset_file.url)

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, RemoteEntity):
            return False
        return self.commit_sha == other.commit_sha and self.path == other.path and self.url == other.url

    def __hash__(self):
        return hash((self.commit_sha, self.path, self.url))

    def to_dataset_file(self) -> old_datasets.DatasetFile:
        """Return an instance of renku.core.models.datasets.DatasetFile."""
        label = generate_label(self.path, self.commit_sha)
        return old_datasets.DatasetFile(label=label, path=self.path, source=self.url, url=self.url)


class DatasetFile(Slots):
    """A file in a dataset."""

    __slots__ = ("based_on", "date_added", "date_removed", "entity", "id", "is_external", "source")

    def __init__(
        self,
        *,
        based_on: RemoteEntity = None,
        date_added: datetime = None,
        date_removed: datetime = None,
        entity: Entity,
        id: str = None,
        is_external: bool = False,
        source: Union[Path, str] = None,
    ):
        assert isinstance(entity, Entity), f"Invalid entity type: '{entity}'"

        super().__init__()

        self.based_on: RemoteEntity = based_on
        self.date_added: datetime = fix_timezone(date_added) or local_now()
        self.date_removed: datetime = fix_timezone(date_removed)
        self.entity: Entity = entity
        self.id: str = id or DatasetFile.generate_id()
        self.is_external: bool = is_external
        self.source: str = str(source)

    @classmethod
    def from_path(cls, client, path: Union[str, Path]) -> Optional["DatasetFile"]:
        """Return an instance from a path."""
        entity = Entity.from_revision(client=client, path=path)
        if not entity:
            return

        return cls(entity=entity, is_external=client.is_external_file(path))

    @classmethod
    @inject.params(client="LocalClient")
    def from_dataset_file(
        cls, dataset_file: old_datasets.DatasetFile, client, revision: str = None
    ) -> Optional["DatasetFile"]:
        """Create an instance by converting from renku.core.models.datasets.DatasetFile if available at revision."""
        entity = Entity.from_revision(client=client, path=dataset_file.path, revision=revision)
        if not entity:
            return

        return cls(
            based_on=RemoteEntity.from_dataset_file(dataset_file.based_on),
            date_added=dataset_file.added,
            entity=entity,
            is_external=dataset_file.external,
            source=dataset_file.source,
        )

    @staticmethod
    def generate_id():
        """Generate an identifier for DatasetFile.

        NOTE: ID should not rely on Entity properties because the same Entity can be added and removed multiple times.
        So, it should be marked by different DatasetFiles.
        """
        return f"/dataset-files/{uuid4().hex}"

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
        date_removed = fix_timezone(date) or local_now()
        self.date_removed = date_removed

    def is_removed(self) -> bool:
        """Return true if dataset is removed and should not be accessed."""
        return self.date_removed is not None

    def to_dataset_file(self, client, revision="HEAD") -> Optional[old_datasets.DatasetFile]:
        """Return an instance of renku.core.models.datasets.DatasetFile at a revision."""
        try:
            return old_datasets.DatasetFile.from_revision(
                client=client,
                revision=revision,
                added=self.date_added,
                based_on=self.based_on.to_dataset_file() if self.based_on else None,
                external=self.is_external,
                id=None,
                path=self.entity.path,
                source=self.source,
                url=None,
            )
        except KeyError:  # NOTE: cannot find a previous commit for path starting at revision
            return None


class Dataset(Persistent):
    """Represent a dataset."""

    def __init__(
        self,
        *,
        identifier: str,
        name: str,
        creators: List[Person] = None,
        date_created: datetime = None,
        date_published: datetime = None,
        date_removed: datetime = None,
        derived_from: str = None,
        description: str = None,
        files: List[DatasetFile] = None,
        id: str = None,
        images: List[ImageObject] = None,
        in_language: Language = None,
        initial_identifier: str = None,
        keywords: List[str] = None,
        license: str = None,
        same_as: Url = None,
        tags: List[DatasetTag] = None,
        title: str = None,
        version: str = None,
    ):
        if not is_dataset_name_valid(name):
            raise errors.ParameterError(f"Invalid dataset name: '{name}'")

        # if `date_published` is set, we are probably dealing with an imported dataset so `date_created` is not needed
        if date_published:
            date_created = None

        # TODO Verify identifier to be valid
        self.identifier = identifier or str(uuid4())
        self.id = id or Dataset.generate_id(identifier=self.identifier)
        self.name = name

        self.creators: List[Person] = creators or []
        self.date_created: datetime = fix_timezone(date_created) or local_now()
        self.date_published: datetime = fix_timezone(date_published)
        self.date_removed: datetime = fix_timezone(date_removed)
        self.derived_from: str = derived_from
        self.description: str = description
        """`files` includes existing files and those that have been removed in the previous version."""
        self.files: List[DatasetFile] = files or []
        self.images: List[ImageObject] = images or []
        self.in_language: Language = in_language
        self.initial_identifier: str = initial_identifier
        self.keywords: List[str] = keywords or []
        self.license: str = license
        self.same_as: Url = same_as
        self.tags: List[DatasetTag] = tags or []
        self.title: str = title
        self.version: str = version

    @staticmethod
    def generate_id(identifier: str) -> str:
        """Generate an identifier for Dataset."""
        return f"/datasets/{identifier}"

    @classmethod
    def from_dataset(cls, dataset: old_datasets.Dataset, client, revision: str, database: Database) -> "Dataset":
        """Create an instance by converting from renku.core.models.datasets.Dataset."""

        def convert_creators(creators) -> List[Person]:
            result = []
            for creator in creators:
                agent = Agent.from_agent(creator)
                existing_agent = database["agents"].get(agent.id)
                if existing_agent:
                    result.append(existing_agent)
                else:
                    result.append(agent)
                    database["agents"].add(agent)

            return result

        def convert_dataset_files(files: List[old_datasets.DatasetFile]) -> List[DatasetFile]:
            """Create instances from renku.core.models.datasets.DatasetFile."""
            dataset_files = []
            files = {f.path: f for f in files}  # NOTE: To make sure there are no duplicate paths

            for file in files.values():
                new_file = DatasetFile.from_dataset_file(file, client=client, revision=revision)
                if not new_file:
                    continue

                dataset_files.append(new_file)

            return dataset_files

        def convert_derived_from(derived_from: Optional[Url]) -> Optional[str]:
            """Return Dataset.id from `derived_from` url."""
            if not derived_from:
                return

            url = derived_from.url.get("@id")
            path = urlparse(url).path

            return Dataset.generate_id(identifier=Path(path).name)

        self = cls(
            creators=convert_creators(dataset.creators),
            date_created=dataset.date_created,
            date_published=dataset.date_published,
            date_removed=None,
            derived_from=convert_derived_from(dataset.derived_from),
            description=dataset.description,
            files=convert_dataset_files(dataset.files),
            id=None,
            identifier=dataset.identifier,
            images=[ImageObject.from_image_object(image) for image in (dataset.images or [])],
            in_language=Language.from_language(dataset.in_language),
            keywords=dataset.keywords,
            license=dataset.license,
            name=dataset.name,
            initial_identifier=dataset.initial_identifier,
            same_as=Url.from_url(dataset.same_as),
            tags=[DatasetTag.from_dataset_tag(tag) for tag in (dataset.tags or [])],
            title=dataset.title,
            version=dataset.version,
        )

        return self

    def replace_identifier(self):
        """Replace dataset's identifier and update relevant fields.

        NOTE: Call this only for newly-created/-imported datasets that don't have a mutability chain because it sets
        `initial_identifier`.
        """
        assert (
            self.derived_from is None
        ), f"Replacing identifier to dataset '{self.name}:{self.identifier}' that is derived from {self.derived_from}"

        self._assign_new_identifier()
        # NOTE: Do not unset `same_as` because it can be set for imported datasets

    def derive_from(self, dataset: "Dataset"):
        """Make `self` a derivative of `dataset` and update related fields."""
        assert dataset is not None, "Cannot derive from None"

        self._assign_new_identifier()
        # NOTE: Setting `initial_identifier` is required for migration of broken projects
        self.initial_identifier = dataset.initial_identifier
        self.derived_from = dataset.id
        self.same_as = None

    def _assign_new_identifier(self):
        identifier = str(uuid4())
        self.initial_identifier = identifier
        self.identifier = identifier
        self.id = Dataset.generate_id(identifier)

    def remove(self, date: datetime = None):
        """Mark the dataset as removed."""
        self.date_removed = fix_timezone(date) or local_now()
        self._p_changed = True

    def is_removed(self) -> bool:
        """Return true if dataset is removed."""
        return self.date_removed is not None

    def find_file(self, path: Union[Path, str]) -> Optional[DatasetFile]:
        """Find a file in files container using its relative path."""
        path = str(path)
        for file in self.files:
            if file.entity.path == path and not file.is_removed():
                return file

    def update_files_from(self, current_files: List[DatasetFile], date: datetime = None):
        """Check `current_files` to reuse existing entries and mark removed files."""
        new_files: Dict[str, DatasetFile] = {f.entity.path: f for f in self.files if not f.is_removed()}
        current_files: Dict[str, DatasetFile] = {f.entity.path: f for f in current_files if not f.is_removed()}

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
            removed_file.remove(date)
            files.append(removed_file)

        self.files = files

    def to_dataset(self, client) -> old_datasets.Dataset:
        """Return an instance of renku.core.models.datasets.Dataset."""

        def convert_creators(creators) -> List[Union[old_agents.Person, old_agents.SoftwareAgent]]:
            result = []
            for creator in creators:
                if isinstance(creator, SoftwareAgent):
                    agent = old_agents.SoftwareAgent(id=creator.id, label=creator.name)
                else:
                    assert isinstance(creator, Person)
                    agent = old_agents.Person(
                        affiliation=creator.affiliation,
                        alternate_name=creator.alternate_name,
                        email=creator.email,
                        id=creator.id,
                        label=creator.name,
                        name=creator.name,
                    )
                result.append(agent)
            return result

        if self.derived_from:
            identifier = Path(self.derived_from).name
            id = generate_dataset_id(client=client, identifier=identifier)
            derived_from = old_datasets.Url(client=client, url_id=id)
        else:
            derived_from = None

        return old_datasets.Dataset(
            name=self.name,
            client=client,
            creators=convert_creators(self.creators),
            date_created=self.date_created,
            date_published=self.date_published,
            derived_from=derived_from,
            description=self.description,
            files=self._convert_to_dataset_files(client),
            id=None,
            identifier=self.identifier,
            images=[image.to_image_object() for image in self.images] if self.images else None,
            in_language=self.in_language.to_language() if self.in_language else None,
            keywords=self.keywords,
            license=self.license,
            project=client._project,
            same_as=self.same_as.to_url(client) if self.same_as else None,
            tags=[tag.to_dataset_tag(client) for tag in self.tags],
            title=self.title,
            url=None,
            version=self.version,
        )

    def _convert_to_dataset_files(self, client):
        """Create instances of renku.core.models.datasets.DatasetFile."""
        dataset_files = []
        for file in self.files:
            dataset_file = file.to_dataset_file(client)
            if not dataset_file:
                continue

            dataset_files.append(dataset_file)

        return dataset_files


class DatasetsProvenance:
    """A set of datasets."""

    def __init__(self, database: Database):
        # A map from name to datasets for current datasets
        self._datasets: Index = database["datasets"]
        # A map from id to datasets that keeps provenance chain tails for all current and removed datasets
        self._provenance_tails: Index = database["datasets-provenance-tails"]
        self._database: Database = database

    def get_by_id(self, id: str) -> Optional[Dataset]:
        """Return a dataset by its id."""
        try:
            object = self._database.get_by_id(id)
        except errors.ObjectNotFoundError:
            pass
        else:
            assert isinstance(object, Dataset)
            return object

    def get_by_name(self, name: str) -> Optional[Dataset]:
        """Return a dataset by its name."""
        return self._datasets.get(name)

    def get_provenance(self):
        """Return the provenance for all datasets."""
        return self._provenance_tails.values()

    def get_previous_version(self, dataset: Dataset) -> Optional[Dataset]:
        """Return the previous version of a dataset if any."""
        if not dataset.derived_from:
            return
        return self.get_by_id(dataset.derived_from)

    @inject.params(client="LocalClient")
    def add_or_update(
        self,
        dataset: old_datasets.Dataset,
        client,
        revision: str = None,
        date: datetime = None,
    ):
        """Add/update a dataset according to its new content.

        NOTE: This functions always mutates the dataset.
        """
        revision = revision or "HEAD"

        new_dataset = Dataset.from_dataset(dataset, client, revision, self._database)
        # NOTE: Dataset's name never changes, so, we use it to detect if a dataset should be mutated.
        current_dataset = self.get_by_name(dataset.name)

        if current_dataset:
            assert (
                not current_dataset.is_removed()
            ), f"Adding/Updating a removed dataset '{dataset.name}:{dataset.identifier}' at '{revision}'"

            new_dataset.update_files_from(current_dataset.files, date=date)

            # NOTE: Always mutate a dataset to make sure an old identifier is not reused
            new_dataset.derive_from(current_dataset)
        else:
            assert (
                dataset.derived_from is None
            ), f"Parent dataset {dataset.derived_from} not found for '{dataset.name}:{dataset.identifier}'"

            # NOTE: This happens in migrations of broken projects
            current_dataset = self.get_by_id(new_dataset.id)
            if current_dataset:
                new_dataset.replace_identifier()

        self._datasets.add(new_dataset)
        self._provenance_tails.pop(new_dataset.derived_from, None)
        self._provenance_tails.add(new_dataset)

    def remove(self, dataset, client, revision=None, date=None):
        """Remove a dataset."""
        revision = revision or "HEAD"

        new_dataset = Dataset.from_dataset(dataset, client, revision, self._database)
        # NOTE: Dataset's name never changes, so, we use it to detect if a dataset should be mutated.
        current_dataset = self._datasets.pop(dataset.name, None)

        if current_dataset:
            assert (
                not current_dataset.is_removed()
            ), f"Removing a removed dataset '{dataset.name}:{dataset.identifier}' at '{revision}'"

            # NOTE: We always assign a new identifier to make sure an old identifier is not reused
            new_dataset.derive_from(current_dataset)
        else:
            # TODO: Should we raise here when migrating
            communication.warn(f"Deleting non-existing dataset '{dataset.name}'")

            assert (
                dataset.derived_from is None
            ), f"Parent dataset {dataset.derived_from} not found for '{dataset.name}:{dataset.identifier}'"

            # NOTE: This happens in migrations of broken projects
            current_dataset = self.get_by_id(new_dataset.id)
            if current_dataset:
                new_dataset.replace_identifier()

        new_dataset.remove(date)
        self._provenance_tails.pop(new_dataset.derived_from, None)
        self._provenance_tails.add(new_dataset)


class UrlSchema(JsonLDSchema):
    """Url schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.URL
        model = Url
        unknown = EXCLUDE

    id = fields.Id(missing=None)
    url = Uri(schema.url, missing=None)


class DatasetTagSchema(JsonLDSchema):
    """DatasetTag schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.PublicationEvent
        model = DatasetTag
        unknown = EXCLUDE

    commit = fields.String(schema.location)
    dataset = fields.String(schema.about)
    date_created = fields.DateTime(schema.startDate, missing=None, format="iso", extra_formats=("%Y-%m-%d",))
    description = fields.String(schema.description, missing=None)
    id = fields.Id()
    name = fields.String(schema.name)

    @pre_dump
    def fix_timezone(self, obj, many=False, **kwargs):
        """Pre dump hook."""
        if many:
            return [self.fix_timezone(o, many=False, **kwargs) for o in obj]
        object.__setattr__(obj, "date_created", self._fix_timezone(obj.date_created))
        return obj


class LanguageSchema(JsonLDSchema):
    """Language schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.Language
        model = Language
        unknown = EXCLUDE

    alternate_name = fields.String(schema.alternateName)
    name = fields.String(schema.name)


class ImageObjectSchema(JsonLDSchema):
    """ImageObject schema."""

    class Meta:
        """Meta class."""

        rdf_type = schema.ImageObject
        model = ImageObject
        unknown = EXCLUDE

    content_url = fields.String(schema.contentUrl)
    id = fields.Id(missing=None)
    position = fields.Integer(schema.position)


class RemoteEntitySchema(JsonLDSchema):
    """RemoteEntity schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Entity, schema.DigitalDocument]
        model = RemoteEntity
        unknown = EXCLUDE

    commit_sha = fields.String(renku.commit_sha)
    id = fields.Id()
    path = fields.String(prov.atLocation)
    url = fields.String(schema.url)


class NewDatasetFileSchema(JsonLDSchema):
    """DatasetFile schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Entity, schema.DigitalDocument]
        model = DatasetFile
        unknown = EXCLUDE

    based_on = Nested(schema.isBasedOn, RemoteEntitySchema, missing=None)
    date_added = DateTimeList(schema.dateCreated, format="iso", extra_formats=("%Y-%m-%d",))
    date_removed = fields.DateTime(prov.invalidatedAtTime, missing=None, format="iso")
    entity = Nested(prov.entity, NewEntitySchema)
    id = fields.Id()
    is_external = fields.Boolean(renku.external, missing=False)
    source = fields.String(renku.source, missing=None)
    url = fields.String(schema.url, missing=None)


class NewDatasetSchema(JsonLDSchema):
    """Dataset schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Entity, schema.Dataset]
        model = Dataset
        unknown = EXCLUDE

    creators = Nested(schema.creator, NewPersonSchema, many=True)
    date_created = fields.DateTime(schema.dateCreated, missing=None, format="iso", extra_formats=("%Y-%m-%d",))
    date_removed = fields.DateTime(prov.invalidatedAtTime, missing=None, format="iso")
    date_published = fields.DateTime(
        schema.datePublished, missing=None, format="%Y-%m-%d", extra_formats=("iso", "%Y-%m-%dT%H:%M:%S")
    )
    derived_from = Nested(prov.wasDerivedFrom, UrlSchema, missing=None)
    description = fields.String(schema.description, missing=None)
    files = Nested(schema.hasPart, NewDatasetFileSchema, many=True)
    id = fields.Id(missing=None)
    identifier = fields.String(schema.identifier)
    images = fields.Nested(schema.image, ImageObjectSchema, missing=None, many=True)
    in_language = Nested(schema.inLanguage, LanguageSchema, missing=None)
    keywords = fields.List(schema.keywords, fields.String(), missing=None)
    license = Uri(schema.license, missing=None)
    name = fields.String(schema.alternateName)
    initial_identifier = fields.String(renku.originalIdentifier)
    same_as = Nested(schema.sameAs, UrlSchema, missing=None)
    tags = Nested(schema.subjectOf, DatasetTagSchema, many=True)
    title = fields.String(schema.name)
    url = fields.String(schema.url)
    version = fields.String(schema.version, missing=None)
