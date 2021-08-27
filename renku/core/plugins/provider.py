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
from typing import Any, Dict, List, Tuple

import pluggy

from renku.core.models.workflow.plan import AbstractPlan
from renku.core.models.workflow.provider import IWorkflowProvider

hookspec = pluggy.HookspecMarker("renku")


@hookspec
def workflow_provider() -> Tuple[IWorkflowProvider, str]:
    """Plugin Hook for ``workflow execute`` call.

    :returns: The workflow executor backend's name.
    """
    pass


@hookspec(firstresult=True)
def workflow_execute(workflow: AbstractPlan, basedir: Path, config: Dict[str, Any]):
    """Plugin Hook for ``workflow execute`` call.

    Can be used to execute renku workflows with different workflow executors.

    :param workflow: a ``AbstractPlan`` object that describes the given workflow.
    :param config: a configuration for the provider.
    """
    pass


def available_workflow_providers() -> List[str]:
    """Returns the currently available workflow execution providers."""
    from renku.core.plugins.pluginmanager import get_plugin_manager

    pm = get_plugin_manager()
    providers = pm.hook.workflow_provider()
    return [p[1] for p in providers]
