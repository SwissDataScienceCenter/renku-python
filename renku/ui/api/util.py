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
r"""Renku API utilities."""

from functools import wraps
from typing import TYPE_CHECKING, Optional

from renku.domain_model.project_context import project_context

if TYPE_CHECKING:
    from renku.infrastructure.gateway.activity_gateway import ActivityGateway
    from renku.infrastructure.gateway.plan_gateway import PlanGateway


def ensure_project_context(fn):
    """Check existence of a project context.

    Args:
        fn: The function to wrap.

    Returns:
        The function with the current project injected.
    """

    def get_current_project():
        """Return current project context if any or a new project object.

        Returns:
            The current project context or None.
        """
        from renku.ui.api import Project

        return Project._project_contexts.top if Project._project_contexts.top else None

    @wraps(fn)
    def wrapper(*args, **kwargs):
        from renku.ui.api import Project

        project = get_current_project() or Project()
        return fn(*args, **kwargs, project=project)

    return wrapper


@ensure_project_context
def get_activity_gateway(project) -> Optional["ActivityGateway"]:
    """Return an instance of ActivityGateway when inside a Renku project."""
    from renku.infrastructure.gateway.activity_gateway import ActivityGateway

    try:
        _ = project_context.repository
    except ValueError:
        return None

    return ActivityGateway()


@ensure_project_context
def get_plan_gateway(project) -> Optional["PlanGateway"]:
    """Return an instance of PlanGateway when inside a Renku project."""
    from renku.infrastructure.gateway.plan_gateway import PlanGateway

    try:
        _ = project_context.repository
    except ValueError:
        return None

    return PlanGateway()
