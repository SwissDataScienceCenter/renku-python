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
"""Helpers functions for metadata management/parsing."""
import os
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Set, Tuple, Union

from renku.core import errors
from renku.core.utils.os import is_subpath

if TYPE_CHECKING:
    from renku.core.models.entity import Entity
    from renku.core.models.provenance.activity import Activity
    from renku.core.models.provenance.agent import Person


def construct_creators(creators: List[Union[dict, str]], ignore_email=False):
    """Parse input and return a list of Person."""
    creators = creators or ()

    if not isinstance(creators, Iterable) or isinstance(creators, str):
        raise errors.ParameterError("Invalid creators type")

    people = []
    no_email_warnings = []
    for creator in creators:
        person, no_email_warning = construct_creator(creator, ignore_email=ignore_email)

        people.append(person)

        if no_email_warning:
            no_email_warnings.append(no_email_warning)

    return people, no_email_warnings


def construct_creator(creator: Union[dict, str], ignore_email) -> Tuple[Optional["Person"], Optional[Union[dict, str]]]:
    """Parse input and return an instance of Person."""
    from renku.core.models.provenance.agent import Person

    if not creator:
        return None, None

    if isinstance(creator, str):
        person = Person.from_string(creator)
    elif isinstance(creator, dict):
        person = Person.from_dict(creator)
    else:
        raise errors.ParameterError("Invalid creator type")

    message = 'A valid format is "Name <email> [affiliation]"'

    if not person.name:  # pragma: no cover
        raise errors.ParameterError(f'Name is invalid: "{creator}".\n{message}')

    if not person.email:
        if not ignore_email:  # pragma: no cover
            raise errors.ParameterError(f'Email is invalid: "{creator}".\n{message}')
        else:
            no_email_warning = creator
    else:
        no_email_warning = None

    return person, no_email_warning


def get_modified_activities(
    activities: List["Activity"], repository
) -> Tuple[Set[Tuple["Activity", "Entity"]], Set[Tuple["Activity", "Entity"]]]:
    """Get lists of activities that have modified/deleted usage entities."""
    modified = set()
    deleted = set()

    paths = []

    for activity in activities:
        for usage in activity.usages:
            paths.append(usage.entity.path)

    hashes = repository.get_object_hashes(paths=paths, revision="HEAD")

    for activity in activities:
        for usage in activity.usages:
            entity = usage.entity
            current_checksum = hashes.get(entity.path, None)
            if current_checksum is None:
                deleted.add((activity, entity))
            elif current_checksum != entity.checksum:
                modified.add((activity, entity))

    return modified, deleted


def filter_overridden_activities(activities: List["Activity"]) -> List["Activity"]:
    """Filter out overridden activities from a list of activities."""
    relevant_activities = {}

    for activity in activities[::-1]:
        outputs = frozenset(g.entity.path for g in activity.generations)

        subset_of = set()
        superset_of = set()

        for k, a in relevant_activities.items():
            if outputs.issubset(k):
                subset_of.add((k, a))
            elif outputs.issuperset(k):
                superset_of.add((k, a))

        if not subset_of and not superset_of:
            relevant_activities[outputs] = activity
            continue

        if subset_of and any(activity.ended_at_time < s.ended_at_time for _, s in subset_of):
            # activity is a subset of another, newer activity, ignore it
            continue

        older_subsets = [k for k, s in superset_of if activity.ended_at_time > s.ended_at_time]

        for older_subset in older_subsets:
            # remove other activities that this activity is a superset of
            del relevant_activities[older_subset]

        relevant_activities[outputs] = activity

    return list(relevant_activities.values())


def add_activity_if_recent(activity: "Activity", activities: Set["Activity"]):
    """Add ``activity`` to ``activities`` if it's not in the set or is the latest executed instance."""
    if activity in activities:
        return

    for existing_activity in activities:
        if activity.has_identical_inputs_and_outputs_as(existing_activity):
            if activity.ended_at_time > existing_activity.ended_at_time:  # activity is newer
                activities.remove(existing_activity)
                activities.add(activity)
            return

    # NOTE: No similar activity was found
    activities.add(activity)


def is_external_file(path: Union[Path, str], client_path: Path):
    """Checks if a path is an external file."""
    from renku.core.management import RENKU_HOME
    from renku.core.management.datasets import DatasetsApiMixin

    path = client_path / path
    if not path.is_symlink() or not is_subpath(path=path, base=client_path):
        return False

    pointer = os.readlink(path)
    return str(os.path.join(RENKU_HOME, DatasetsApiMixin.POINTERS)) in pointer
