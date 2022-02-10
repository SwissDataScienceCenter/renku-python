# -*- coding: utf-8 -*-
#
# Copyright 2017-2021- Swiss Data Science Center (SDSC)
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
"""Plugin hooks for renku run customization."""
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Tuple

if TYPE_CHECKING:
    import networkx as nx

import pluggy

from renku.core import errors
from renku.core.models.workflow.provider import IWorkflowProvider

RENKU_ENV_PREFIX = "RENKU_ENV_"

hookspec = pluggy.HookspecMarker("renku")


@hookspec
def workflow_provider() -> Tuple[IWorkflowProvider, str]:
    """Plugin Hook to get providers for ``workflow execute`` call.

    :returns: A tuple of the provider itself and the workflow executor backends name.
    """
    raise NotImplementedError


@hookspec(firstresult=True)
def workflow_execute(dag: "nx.DiGraph", basedir: Path, config: Dict[str, Any]):
    """Plugin Hook for ``workflow execute`` call.

    Can be used to execute renku workflows with different workflow executors.

    :param workflow: a ``AbstractPlan`` object that describes the given workflow.
    :param config: a configuration for the provider.
    """
    raise NotImplementedError


def available_workflow_providers() -> List[str]:
    """Returns the currently available workflow execution providers."""
    from renku.core.plugins.pluginmanager import get_plugin_manager

    pm = get_plugin_manager()
    providers = pm.hook.workflow_provider()
    return [p[1] for p in providers]


def execute(dag: "nx.DiGraph", basedir: Path, config: Dict[str, Any], provider: str = "cwltool") -> List[str]:
    """Executes a given workflow using the selected provider.

    :param workflow: Workflow to be executed.
    :param basedir: The root directory of the renku project.
    :param config: Configuration values for the workflow provider.
    :param provider: The workflow executor engine to be used.
    :returns: List of paths that has been modified.
    """
    from renku.core.plugins.pluginmanager import get_plugin_manager

    pm = get_plugin_manager()
    providers = pm.hook.workflow_provider()
    provider = next(filter(lambda x: provider == x[1], providers), None)
    if not provider:
        raise errors.ParameterError(f"The specified workflow executor '{provider}' is not available.")

    providers.remove(provider)
    executor = pm.subset_hook_caller("workflow_execute", list(map(lambda x: x[0], providers)))

    return executor(dag=dag, basedir=basedir, config=config)
