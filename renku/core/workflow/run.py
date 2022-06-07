# -*- coding: utf-8 -*-
#
# Copyright 2018-2022- Swiss Data Science Center (SDSC)
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
"""Running workflows logic."""

from collections import defaultdict
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Set, Union

from renku.command.command_builder import inject
from renku.core.interface.client_dispatcher import IClientDispatcher
from renku.core.util.os import get_relative_path_to_cwd, get_relative_paths
from renku.core.workflow.activity import (
    get_all_modified_and_deleted_activities_and_entities,
    get_downstream_generating_activities,
    is_activity_valid,
)


class StatusResult(NamedTuple):
    """Represent status of a project.

    A quadruple containing a mapping of stale outputs to modified usages, a mapping of stale activities that have no
    generation to modified usages, a set of modified usages, and a set of deleted usages.
    """

    outdated_outputs: Dict[str, Set[str]]
    outdated_activities: Dict[str, Set[str]]
    modified_inputs: Set[str]
    deleted_inputs: Set[str]


@inject.autoparams("client_dispatcher")
def get_status(
    client_dispatcher: IClientDispatcher, paths: Optional[List[Union[Path, str]]] = None, ignore_deleted: bool = False
) -> StatusResult:
    """Return status of a project.

    Args:
        client_dispatcher(IClientDispatcher): Injected client dispatcher.
        paths(Optional[List[Union[Path, str]]]): Limit the status to this list of paths (Default value = None).
        ignore_deleted(bool): Whether to ignore deleted generations (Default value = False).

    Returns:
        StatusResult: Status of the project.

    """

    def mark_generations_as_stale(activity):
        for generation in activity.generations:
            generation_path = get_relative_path_to_cwd(client.path / generation.entity.path)
            stale_outputs[generation_path].add(usage_path)

    client = client_dispatcher.current_client

    ignore_deleted = ignore_deleted or client.get_value("renku", "update_ignore_delete")

    modified, deleted = get_all_modified_and_deleted_activities_and_entities(client.repository)

    modified = {(a, e) for a, e in modified if is_activity_valid(a)}
    deleted = {(a, e) for a, e in deleted if is_activity_valid(a)}

    if not modified and not deleted:
        return StatusResult({}, {}, set(), set())

    paths = paths or []
    paths = get_relative_paths(base=client.path, paths=[Path.cwd() / p for p in paths])  # type: ignore

    modified_inputs: Set[str] = set()
    stale_outputs: Dict[str, Set[str]] = defaultdict(set)
    stale_activities: Dict[str, Set[str]] = defaultdict(set)

    for start_activity, entity in modified:
        usage_path = get_relative_path_to_cwd(client.path / entity.path)

        # NOTE: Add all downstream activities if the modified entity is in paths; otherwise, add only activities that
        # chain-generate at least one of the paths
        generation_paths = [] if not paths or entity.path in paths else paths

        activities = get_downstream_generating_activities(
            starting_activities={start_activity},
            paths=generation_paths,
            ignore_deleted=ignore_deleted,
            client_path=client.path,
        )
        if activities:
            modified_inputs.add(usage_path)

            for activity in activities:
                if len(activity.generations) == 0:
                    stale_activities[activity.id].add(usage_path)
                else:
                    mark_generations_as_stale(activity)

    deleted_paths = {e.path for _, e in deleted}
    deleted_paths = {get_relative_path_to_cwd(client.path / d) for d in deleted_paths if not paths or d in paths}

    return StatusResult(stale_outputs, stale_activities, modified_inputs, deleted_paths)
