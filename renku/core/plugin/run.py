# -*- coding: utf-8 -*-
#
# Copyright 2017-2022- Swiss Data Science Center (SDSC)
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
import pluggy

hookspec = pluggy.HookspecMarker("renku")


@hookspec
def plan_annotations(plan):
    """Plugin Hook to add ``Annotation`` entry list to a ``Plan``.

    Run when a Plan is created by ``renku run``.

    Args:
        plan: A ``Plan`` object to get annotations for.

    Returns:
        A list of ``renku.domain_model.provenance.annotation.Annotation``
        objects.

    """
    pass


@hookspec
def activity_annotations(activity):
    """Plugin Hook to add ``Annotation`` entry list to a ``Activity``.

    Run when creating an activity from a ``renku run``.

    Args:
        activity: An ``Activity`` object to get annotations for.

    Returns:
        A list of ``renku.domain_model.provenance.annotation.Annotation``
        objects.

    """
    pass


@hookspec
def pre_run(tool):
    """Plugin Hook that gets called at the start of a ``renku run`` call.

    Can be used to setup plugins that get executed during the run.

    Args:
        tool (``PlanFactory``): The plan factory that captured the run.
    """
    pass
