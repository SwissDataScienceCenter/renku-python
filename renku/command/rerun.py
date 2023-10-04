# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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

from pydantic import validate_arguments

from renku.command.command_builder.command import Command, inject
from renku.core import errors
from renku.core.interface.activity_gateway import IActivityGateway
from renku.core.util.os import get_relative_paths
from renku.core.workflow.activity import get_activities_until_paths, sort_activities
from renku.core.workflow.execute import execute_workflow_graph
from renku.core.workflow.model.concrete_execution_graph import ExecutionGraph
from renku.domain_model.project_context import project_context


def rerun_command(skip_metadata_update: bool):
    """Recreate files generated by a sequence of ``run`` commands."""
    command = Command().command(_rerun).require_migration().require_clean()
    if skip_metadata_update:
        command = command.with_database(write=False)
    else:
        command = command.with_database(write=True).with_commit()
    return command


@inject.autoparams()
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def _rerun(
    dry_run: bool,
    sources: List[str],
    paths: List[str],
    provider: str,
    config: Optional[str],
    activity_gateway: IActivityGateway,
):
    """Rerun a previously run workflow.

    Args:
        dry_run (bool): Whether or not to actually execute the workflow or just show
            what would be executed.
        sources (List[str]): Input files to start execution from.
        paths (List[str]): Output paths to recreate.
        provider (str): Name of the workflow provider to use for execution.
        config (str): Path to configuration for the workflow provider.
        activity_gateway (IActivityGateway): Injected activity gateway.
    """

    sources = sources or []
    sources = get_relative_paths(base=project_context.path, paths=sources)
    paths = paths or []
    paths = get_relative_paths(base=project_context.path, paths=paths)

    activities = list(get_activities_until_paths(paths, sources, activity_gateway=activity_gateway))

    if len(activities) == 0:
        raise errors.NothingToExecuteError()

    activities = sort_activities(activities)
    if dry_run:
        return activities, set(sources)

    graph = ExecutionGraph([a.plan_with_values for a in activities], virtual_links=True)
    execute_workflow_graph(dag=graph.workflow_graph, provider=provider, config=config)
