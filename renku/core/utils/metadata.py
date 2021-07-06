# -*- coding: utf-8 -*-
#
# Copyright 2021 - Swiss Data Science Center (SDSC)
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
"""Helpers functions for metadata conversion."""

from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse

from renku.core.management.migrations.models import v9 as old_datasets
from renku.core.models.dataset import Dataset, DatasetFile, DatasetTag, ImageObject, Language, RemoteEntity, Url
from renku.core.models.entity import Entity
from renku.core.models.provenance import agent as new_agents


def convert_url(url: Optional[old_datasets.Url]) -> Optional[Url]:
    """Convert old Url to new Url."""
    if not url:
        return
    return Url(url=url.url, url_id=url.url_id, url_str=url.url_str)


def convert_dataset_tag(tag: Optional[old_datasets.DatasetTag]) -> Optional[DatasetTag]:
    """Convert old DatasetTag to new DatasetTag."""
    if not tag:
        return
    return DatasetTag(
        commit=tag.commit, dataset=tag.dataset, date_created=tag.created, description=tag.description, name=tag.name
    )


def convert_language(language: Optional[old_datasets.Language]) -> Optional[Language]:
    """Convert old Language to new Language."""
    if not language:
        return
    return Language(alternate_name=language.alternate_name, name=language.name)


def convert_image_object(image_object: Optional[old_datasets.ImageObject]) -> Optional[ImageObject]:
    """Create from old ImageObject instance."""
    if not image_object:
        return
    return ImageObject(content_url=image_object.content_url, position=image_object.position, id=image_object.id)


def create_remote_entity(dataset_file: Optional[old_datasets.DatasetFile]) -> Optional[RemoteEntity]:
    """Create RemoteEntity from old DatasetFile."""
    if not dataset_file:
        return
    commit_sha = dataset_file._label.rsplit("@", maxsplit=1)[-1]
    return RemoteEntity(commit_sha=commit_sha, path=dataset_file.path, url=dataset_file.url)


def convert_dataset_file(dataset_file: old_datasets.DatasetFile, client, revision: str) -> Optional[DatasetFile]:
    """Convert old DatasetFile to new DatasetFile if available at revision."""
    entity = Entity.from_revision(client=client, path=dataset_file.path, revision=revision)
    if not entity:
        return

    return DatasetFile(
        based_on=create_remote_entity(dataset_file.based_on),
        date_added=dataset_file.added,
        entity=entity,
        is_external=dataset_file.external,
        source=dataset_file.source,
    )


def convert_dataset(dataset: old_datasets.Dataset, client, revision: str) -> Dataset:
    """Convert old Dataset to new Dataset."""

    def convert_dataset_files(files: List[old_datasets.DatasetFile]) -> List[DatasetFile]:
        """Create instances from old DatasetFile."""
        dataset_files = []
        files = {f.path: f for f in files}  # NOTE: To make sure there are no duplicate paths

        for file in files.values():
            new_file = convert_dataset_file(dataset_file=file, client=client, revision=revision)
            if not new_file:
                continue

            dataset_files.append(new_file)

        return dataset_files

    def convert_derived_from(derived_from: Optional[old_datasets.Url]) -> Optional[str]:
        """Return Dataset.id from `derived_from` url."""
        if not derived_from:
            return

        url = derived_from.url.get("@id")
        path = urlparse(url).path

        return Dataset.generate_id(identifier=Path(path).name)

    return Dataset(
        creators=[new_agents.Agent.from_agent(creator) for creator in dataset.creators],
        dataset_files=convert_dataset_files(dataset.files),
        date_created=dataset.date_created,
        date_published=dataset.date_published,
        date_removed=None,
        derived_from=convert_derived_from(dataset.derived_from),
        description=dataset.description,
        id=None,
        identifier=dataset.identifier,
        images=[convert_image_object(image) for image in (dataset.images or [])],
        in_language=convert_language(dataset.in_language),
        keywords=dataset.keywords,
        license=dataset.license,
        name=dataset.name,
        initial_identifier=dataset.initial_identifier,
        same_as=convert_url(dataset.same_as),
        tags=[convert_dataset_tag(tag) for tag in (dataset.tags or [])],
        title=dataset.title,
        version=dataset.version,
    )
