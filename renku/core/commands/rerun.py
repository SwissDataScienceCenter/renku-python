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
"""Renku ``rerun`` command."""

from typing import List, Optional

from renku.core import errors
from renku.core.commands.workflow import execute_workflow
from renku.core.management.command_builder.command import Command, inject
from renku.core.management.interface.activity_gateway import IActivityGateway
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.workflow.activity import get_activities_until_paths, sort_activities
from renku.core.management.workflow.concrete_execution_graph import ExecutionGraph
from renku.core.utils.os import get_relative_paths


def rerun_command():
    """Recreate files generated by a sequence of ``run`` commands."""
    return (
        Command()
        .command(_rerun)
        .require_migration()
        .require_clean()
        .require_nodejs()
        .with_database(write=True)
        .with_commit()
    )


@inject.autoparams()
def _rerun(
    dry_run: bool,
    sources: List[str],
    paths: List[str],
    provider: str,
    config: Optional[str],
    client_dispatcher: IClientDispatcher,
    activity_gateway: IActivityGateway,
):
    client = client_dispatcher.current_client

    sources = sources or []
    sources = get_relative_paths(base=client.path, paths=sources)
    paths = paths or []
    paths = get_relative_paths(base=client.path, paths=paths)

    activities = list(
        get_activities_until_paths(
            paths, sources, activity_gateway=activity_gateway, client_dispatcher=client_dispatcher
        )
    )

    if len(activities) == 0:
        raise errors.NothingToExecuteError()

    activities = sort_activities(activities)
    if dry_run:
        return activities, set(sources)

    graph = ExecutionGraph([a.plan_with_values for a in activities], virtual_links=True)
    # FIXME: drop
    provider = "toil"
    execute_workflow(dag=graph.workflow_graph, command_name="rerun", provider=provider, config=config)
