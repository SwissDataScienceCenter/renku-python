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
"""Renku ``update`` command."""

from pathlib import Path
from typing import Optional

from renku.command.command_builder import inject
from renku.command.command_builder.command import Command
from renku.core import errors
from renku.core.errors import ParameterError
from renku.core.interface.client_dispatcher import IClientDispatcher
from renku.core.util.os import get_relative_paths
from renku.core.workflow.activity import (
    get_all_modified_and_deleted_activities_and_entities,
    get_downstream_generating_activities,
    is_activity_valid,
    sort_activities,
)
from renku.core.workflow.concrete_execution_graph import ExecutionGraph
from renku.core.workflow.execute import execute_workflow_graph


def update_command(skip_metadata_update: bool):
    """Update existing files by rerunning their outdated workflow."""
    command = Command().command(_update).require_migration().require_clean()
    if skip_metadata_update:
        command = command.with_database(write=False)
    else:
        command = command.with_database(write=True).with_commit()
    return command


@inject.autoparams()
def _update(
    update_all: bool,
    dry_run: bool,
    ignore_deleted: bool,
    client_dispatcher: IClientDispatcher,
    provider: str,
    config: Optional[str],
    paths=None,
):
    if not paths and not update_all and not dry_run:
        raise ParameterError("Either PATHS, --all/-a, or --dry-run/-n should be specified.")
    if paths and update_all:
        raise ParameterError("Cannot use PATHS and --all/-a at the same time.")

    client = client_dispatcher.current_client

    paths = paths or []
    paths = get_relative_paths(base=client.path, paths=[Path.cwd() / p for p in paths])

    modified, _ = get_all_modified_and_deleted_activities_and_entities(client.repository)
    modified_activities = {a for a, _ in modified if not a.deleted and is_activity_valid(a)}
    modified_paths = {e.path for _, e in modified}

    activities = get_downstream_generating_activities(
        starting_activities=modified_activities,
        paths=paths,
        ignore_deleted=ignore_deleted,
        client_path=client.path,
    )

    if len(activities) == 0:
        raise errors.NothingToExecuteError()

    # NOTE: When updating we only want to eliminate activities that are overridden, not their parents
    activities = sort_activities(activities, remove_overridden_parents=False)
    if dry_run:
        return activities, modified_paths

    graph = ExecutionGraph([a.plan_with_values for a in activities], virtual_links=True)
    execute_workflow_graph(dag=graph.workflow_graph, provider=provider, config=config)
