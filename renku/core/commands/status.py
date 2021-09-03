# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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
"""Renku show command."""

from collections import defaultdict
from typing import List, Set, Tuple

from git import Repo

from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.interface.activity_gateway import IActivityGateway
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.models.provenance.activity import Activity, Usage
from renku.core.utils.git import get_object_hash
from renku.core.utils.os import get_relative_path_to_cwd, get_relative_paths


def get_status_command():
    """Show a status of the repository."""
    return Command().command(_get_status).require_migration().require_clean().with_database(write=False)


@inject.autoparams()
def _get_status(client_dispatcher: IClientDispatcher, activity_gateway: IActivityGateway, paths=None):
    def get_dependant_activities_from(start_activity):
        """Return a set of activity and all its downstream activities."""
        all_activities = activity_gateway.get_downstream_activities(start_activity)
        all_activities.add(start_activity)
        return all_activities

    def mark_generations_as_stale(activity):
        for generation in activity.generations:
            generation_path = get_relative_path_to_cwd(client.path / generation.entity.path)
            stale_outputs[generation_path].add(usage_path)

    client = client_dispatcher.current_client

    paths = paths or []
    paths = get_relative_paths(base=client.path, paths=paths)

    latest_activities = activity_gateway.get_latest_activity_per_plan().values()

    modified, deleted = _get_modified_paths(activities=latest_activities, repo=client.repo)

    if not modified and not deleted:
        return None, None, None, None

    modified_inputs = set()
    stale_outputs = defaultdict(set)
    stale_activities = defaultdict(set)

    for start_activity, usage in modified:
        usage_path = get_relative_path_to_cwd(client.path / usage.entity.path)

        activities = get_dependant_activities_from(start_activity)

        if not paths or usage.entity.path in paths:  # add all downstream activities
            modified_inputs.add(usage_path)
            for activity in activities:
                if len(activity.generations) == 0:
                    stale_activities[activity.id].add(usage_path)
                else:
                    mark_generations_as_stale(activity)
        else:
            for activity in activities:
                if any(g.entity.path in paths for g in activity.generations):
                    modified_inputs.add(usage_path)
                    mark_generations_as_stale(activity)

    deleted = {get_relative_path_to_cwd(client.path / d) for d in deleted if not paths or d in paths}

    return stale_outputs, stale_activities, modified_inputs, deleted


def _get_modified_paths(activities: List[Activity], repo: Repo) -> Tuple[Set[Tuple[Activity, Usage]], Set[str]]:
    """Get modified and deleted usages/inputs of a list of activities."""
    modified = set()
    deleted = set()

    for activity in activities:
        for usage in activity.usages:
            current_checksum = get_object_hash(repo=repo, path=usage.entity.path)
            if not current_checksum:
                deleted.add(usage.entity.path)
            elif current_checksum != usage.entity.checksum:
                modified.add((activity, usage))

    return modified, deleted
