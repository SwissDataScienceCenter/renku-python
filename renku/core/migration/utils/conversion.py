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
"""Utility functions for converting metadata."""

from pathlib import Path
from typing import List, Optional, Tuple, Union
from urllib.parse import urlparse

from renku.core.migration.models import v9 as old_datasets
from renku.core.util.git import get_entity_from_revision
from renku.core.util.urls import get_slug
from renku.domain_model.dataset import (
    Dataset,
    DatasetFile,
    DatasetTag,
    ImageObject,
    Language,
    RemoteEntity,
    Url,
    is_dataset_name_valid,
)
from renku.domain_model.project_context import project_context
from renku.domain_model.provenance import agent as new_agents


def _convert_dataset_identifier(identifier: str) -> str:
    """Remove - from a dataset identifier."""
    return identifier.replace("-", "")


def _convert_dataset_tag(tag: Optional[old_datasets.DatasetTag]) -> Optional[DatasetTag]:
    """Convert old DatasetTag to new DatasetTag."""
    if not tag:
        return

    # NOTE: ``dataset_id`` field will be set later when processing the migrated commit.
    return DatasetTag(
        dataset_id=Url(url_id="dummy-id"), date_created=tag.created, description=tag.description, name=tag.name
    )


def _convert_language(language: Optional[old_datasets.Language]) -> Optional[Language]:
    """Convert old Language to new Language."""
    if not language:
        return
    return Language(name=language.name, alternate_name=language.alternate_name)


def _convert_image_object(image_object: Optional[old_datasets.ImageObject], dataset_id: str) -> Optional[ImageObject]:
    """Create from old ImageObject instance."""
    if not image_object:
        return
    id = ImageObject.generate_id(dataset_id=dataset_id, position=image_object.position)
    return ImageObject(content_url=image_object.content_url, position=image_object.position, id=id)


def _create_remote_entity(dataset_file: Optional[old_datasets.DatasetFile]) -> Optional[RemoteEntity]:
    """Create RemoteEntity from old DatasetFile."""
    if not dataset_file:
        return
    commit_sha = dataset_file._label.rsplit("@", maxsplit=1)[-1]
    return RemoteEntity(checksum=commit_sha, path=dataset_file.path, url=dataset_file.url)


def _convert_dataset_file(dataset_file: old_datasets.DatasetFile, revision: str) -> Optional[DatasetFile]:
    """Convert old DatasetFile to new DatasetFile if available at revision."""
    repository = project_context.repository

    entity = get_entity_from_revision(repository=repository, path=dataset_file.path, revision=revision)
    if not entity:
        return

    return DatasetFile(
        based_on=_create_remote_entity(dataset_file.based_on),
        date_added=dataset_file.added,
        entity=entity,
        is_external=dataset_file.external,
        source=dataset_file.source,
    )


def _convert_person(person: Optional[old_datasets.Person]) -> Optional[new_agents.Person]:
    """Create a Person from and old Person."""
    if not person:
        return

    return new_agents.Person(
        affiliation=person.affiliation,
        alternate_name=person.alternate_name,
        email=person.email,
        id=None,
        name=person.name,
    )


def _convert_agent(
    agent: Optional[Union[old_datasets.Person, old_datasets.SoftwareAgent]]
) -> Optional[Union[new_agents.Person, new_agents.SoftwareAgent]]:
    """Create an instance from Person/SoftwareAgent."""
    if isinstance(agent, old_datasets.SoftwareAgent):
        return new_agents.SoftwareAgent(id=agent.id, name=agent.label)

    assert not agent or isinstance(agent, old_datasets.Person), f"Invalid type {type(agent)}"
    return _convert_person(agent)


def _convert_same_as(url: Optional[old_datasets.Url]) -> Optional[Url]:
    """Convert old Url to new Url."""
    if not url:
        return

    if url.url_str:
        parsed_url = urlparse(url.url_str)
    else:
        assert url.url_id
        parsed_url = urlparse(url.url_id)

    # NOTE: Test if dataset is imported from a renku deployment. This is good enough for all current deployments.
    if "renku" in parsed_url.netloc:
        path = _convert_dataset_identifier(parsed_url.path)
        parsed_url = parsed_url._replace(path=path)

    url_str = parsed_url.geturl()

    return Url(url_str=url_str) if url.url_str else Url(url_id=url_str)


def convert_dataset(dataset: old_datasets.Dataset, revision: str) -> Tuple[Dataset, List[DatasetTag]]:
    """Convert old Dataset to new Dataset."""

    def convert_dataset_files(files: List[old_datasets.DatasetFile]) -> List[DatasetFile]:
        """Create instances from old DatasetFile."""
        dataset_files = []
        files = {f.path: f for f in files}  # NOTE: To make sure there are no duplicate paths

        for file in files.values():
            new_file = _convert_dataset_file(dataset_file=file, revision=revision)
            if not new_file:
                continue

            dataset_files.append(new_file)

        return dataset_files

    def convert_derived_from(
        derived_from: Optional[old_datasets.Url], same_as: Optional[old_datasets.Url]
    ) -> Optional[Url]:
        """Return Dataset.id from `derived_from` url."""
        if not derived_from or same_as:
            return

        url = derived_from.url.get("@id")
        path = _convert_dataset_identifier(urlparse(url).path)

        return Url(url_id=Dataset.generate_id(identifier=Path(path).name))

    def convert_license(license):
        if not license:
            return license
        elif isinstance(license, (Url, str)):
            return license
        elif isinstance(license, dict) and len(license) == 1:
            return list(license.values())[0]
        elif isinstance(license, dict) and "@id" in license:
            return license["@id"]
        elif isinstance(license, list) and len(license) == 1:
            return license[0]

        return str(license)

    tags = [_convert_dataset_tag(tag) for tag in (dataset.tags or [])]
    name = get_slug(dataset.name) if not is_dataset_name_valid(dataset.name) else dataset.name

    identifier = _convert_dataset_identifier(dataset.identifier)
    id = Dataset.generate_id(identifier=identifier)

    return (
        Dataset(
            creators=[_convert_agent(creator) for creator in dataset.creators],
            dataset_files=convert_dataset_files(dataset.files),
            date_created=dataset.date_created,
            date_published=dataset.date_published,
            date_removed=None,
            date_modified=dataset.date_created or dataset.date_published,
            derived_from=convert_derived_from(dataset.derived_from, dataset.same_as),
            description=dataset.description,
            id=id,
            identifier=identifier,
            images=[_convert_image_object(image, dataset_id=id) for image in (dataset.images or [])],
            in_language=_convert_language(dataset.in_language),
            keywords=dataset.keywords,
            license=convert_license(dataset.license),
            name=name,
            project_id=project_context.project.id,
            initial_identifier=_convert_dataset_identifier(dataset.initial_identifier),
            same_as=_convert_same_as(dataset.same_as),
            title=dataset.title,
            version=dataset.version,
        ),
        tags,
    )
