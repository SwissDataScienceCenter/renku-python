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
"""Tag management for dataset."""

import re
from typing import List, Optional

from renku.core import errors
from renku.core.commands.format.dataset_tags import DATASET_TAGS_FORMATS
from renku.core.management.dataset.datasets_provenance import DatasetsProvenance
from renku.core.metadata.immutable import DynamicProxy
from renku.core.models.dataset import DatasetTag, Url
from renku.core.utils import communication


def add_dataset_tag(dataset_name: str, tag: str, description="", force=False):
    """Adds a new tag to a dataset.

    Validates if the tag already exists and that the tag follows the same rules as docker tags.
    See https://docs.docker.com/engine/reference/commandline/tag/ for a documentation of docker tag syntax.

    :raises: ``errors.ParameterError``
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

    tags = datasets_provenance.get_all_tags(dataset)
    tags = sorted(tags, key=lambda t: t.date_created)
    tags = [DynamicProxy(t) for t in tags]
    for tag in tags:
        tag.dataset = dataset.title

    return DATASET_TAGS_FORMATS[format](tags)


def remove_dataset_tags(dataset_name: str, tags: List[str]):
    """Removes tags from a dataset."""
    datasets_provenance = DatasetsProvenance()
    dataset = datasets_provenance.get_by_name(dataset_name, strict=True)

    dataset_tags = datasets_provenance.get_all_tags(dataset)
    tag_names = {t.name for t in dataset_tags}
    not_found = set(tags).difference(tag_names)

    if len(not_found) > 0:
        raise errors.ParameterError(f"Tags [{', '.join(not_found)}] not found")

    for tag in dataset_tags:
        if tag.name in tags:
            datasets_provenance.remove_tag(dataset, tag)


def prompt_access_token(exporter):
    """Prompt user for an access token for a provider.

    :return: The new access token
    """
    text_prompt = "You must configure an access token\n"
    text_prompt += "Create one at: {0}\n".format(exporter.access_token_url())
    text_prompt += "Access token"

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
        elif selection > 1 and selection <= len(tags) + 1:
            return tags[selection - 2]
        communication.warn(f"{selection} is not a valid choice. Selected value has to be between 1 and {len(tags) + 1}")
