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
"""Tag management for dataset."""

import re
from typing import List, Optional, cast

from renku.command.format.dataset_tags import DATASET_TAGS_FORMATS
from renku.core import errors
from renku.core.dataset.datasets_provenance import DatasetsProvenance
from renku.core.util import communication
from renku.domain_model.dataset import Dataset, DatasetTag, Url
from renku.infrastructure.gateway.dataset_gateway import DatasetGateway
from renku.infrastructure.immutable import DynamicProxy


def add_dataset_tag(dataset_name: str, tag: str, description="", force=False):
    """Adds a new tag to a dataset.

    Validates if the tag already exists and that the tag follows the same rules as docker tags.
    See https://docs.docker.com/engine/reference/commandline/tag/ for a documentation of docker tag syntax.

    Raises:
        ``errors.ParameterError``: If tag is too long or contains invalid characters.
    """
    if len(tag) > 128:
        raise errors.ParameterError("Tags can be at most 128 characters long.")

    if not re.match("^(?![.-])[a-zA-Z0-9_.-]{1,128}$", tag):
        raise errors.ParameterError(
            f"Tag '{tag}' is invalid.\n"
            "Only characters a-z, A-Z, 0-9, ., - and _ are allowed.\nTag can't start with a . or -"
        )
    datasets_provenance = DatasetsProvenance()
    dataset = datasets_provenance.get_by_name(dataset_name, strict=True)
    assert dataset is not None

    tags = datasets_provenance.get_all_tags(dataset)
    existing_tag = next((t for t in tags if t.name == tag), None)
    if existing_tag:
        if not force:
            raise errors.ParameterError(f"Tag '{tag}' already exists")
        datasets_provenance.remove_tag(dataset, existing_tag)

    new_tag = DatasetTag(dataset_id=Url(url_id=dataset.id), description=description, name=tag)

    datasets_provenance.add_tag(dataset, new_tag)


def list_dataset_tags(dataset_name, format):
    """List all tags for a dataset."""
    datasets_provenance = DatasetsProvenance()
    dataset = datasets_provenance.get_by_name(dataset_name, strict=True)
    assert dataset is not None

    tags = datasets_provenance.get_all_tags(dataset)
    tags = sorted(tags, key=lambda t: t.date_created)
    tags = [cast(Dataset, DynamicProxy(t)) for t in tags]
    for tag in tags:
        tag.dataset = dataset.title

    return DATASET_TAGS_FORMATS[format](tags)


def remove_dataset_tags(dataset_name: str, tags: List[str]):
    """Removes tags from a dataset."""
    datasets_provenance = DatasetsProvenance()
    dataset = datasets_provenance.get_by_name(dataset_name, strict=True)
    assert dataset is not None

    dataset_tags = datasets_provenance.get_all_tags(dataset)
    tag_names = {t.name for t in dataset_tags}
    not_found = set(tags).difference(tag_names)

    if len(not_found) > 0:
        raise errors.ParameterError(f"Tags [{', '.join(not_found)}] not found")

    for tag in dataset_tags:
        if tag.name in tags:
            datasets_provenance.remove_tag(dataset, tag)


def get_dataset_by_tag(dataset: Dataset, tag: str) -> Optional[Dataset]:
    """Return a version of dataset that has a specific tag.

    Args:
        dataset(Dataset): A dataset to return its tagged version.
        tag(str): Tag name to search for.

    Returns:
        Optional[Dataset]: The dataset pointed to by the tag or None if nothing found.
    """
    dataset_gateway = DatasetGateway()

    tags = dataset_gateway.get_all_tags(dataset)
    selected_tag = next((t for t in tags if t.name == tag), None)
    if selected_tag is None:
        return None
    return dataset_gateway.get_by_id(selected_tag.dataset_id.value)


def prompt_access_token(exporter):
    """Prompt user for an access token for a provider.

    Returns:
        The new access token
    """
    text_prompt = f"You must configure an access token\nCreate one at: {exporter.access_token_url()}\nAccess token: "
    return communication.prompt(text_prompt, type=str)


def prompt_tag_selection(tags) -> Optional[DatasetTag]:
    """Prompt user to chose a tag or <HEAD>."""
    tags = sorted(tags, key=lambda t: t.date_created)

    text_prompt = "Tag to export: \n\n<HEAD>\t[1]\n"
    text_prompt += "\n".join(f"{t.name}\t[{i}]" for i, t in enumerate(tags, start=2))
    text_prompt += "\n\nTag"
    while True:
        selection = communication.prompt(text_prompt, type=int, default=1)

        if selection == 1:
            return None
        elif 1 < selection <= len(tags) + 1:
            return tags[selection - 2]
        communication.warn(f"{selection} is not a valid choice. Selected value has to be between 1 and {len(tags) + 1}")
