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
"""Checks needed to determine integrity of workflows."""
from datetime import timedelta
from typing import List, Optional, Tuple, cast

from renku.command.command_builder import inject
from renku.command.util import WARNING
from renku.core.interface.plan_gateway import IPlanGateway
from renku.core.util import communication
from renku.domain_model.project_context import project_context
from renku.domain_model.workflow.plan import AbstractPlan
from renku.infrastructure.gateway.activity_gateway import reindex_catalog


def check_activity_catalog(fix, force, **_) -> Tuple[bool, bool, Optional[str]]:
    """Check if the activity-catalog needs to be rebuilt.

    Args:
        fix: Whether to fix found issues.
        force: Whether to force rebuild the activity catalog.
        _: keyword arguments.

    Returns:
        Tuple of whether the activity-catalog needs to be rebuilt, if an automated fix is available and a string of
            found problems.
    """
    database = project_context.database
    activity_catalog = database["activity-catalog"]
    relations = database["_downstream_relations"]

    # NOTE: If len(activity_catalog) > 0 then either the project is fixed or it used a fixed Renku version but still has
    # broken metadata. ``force`` allows to rebuild the metadata in the latter case.
    if (len(relations) == 0 or len(activity_catalog) > 0) and not (force and fix):
        return True, False, None

    if not fix:
        problems = (
            WARNING + "The project's workflow metadata needs to be rebuilt (use 'renku doctor --fix' to rebuild it).\n"
        )

        return False, True, problems

    with communication.busy("Rebuilding workflow metadata ..."):
        reindex_catalog(database=database)

    communication.info("Workflow metadata was rebuilt")

    return True, False, None


@inject.autoparams("plan_gateway")
def check_plan_modification_date(fix, plan_gateway: IPlanGateway, **_) -> Tuple[bool, bool, Optional[str]]:
    """Check if all plans have modification date set for them.

    Args:
        fix(bool): Whether to fix found issues.
        plan_gateway(IPlanGateway): Injected PlanGateway.
        _: keyword arguments.

    Returns:
        Tuple[bool, Optional[str]]: Tuple of whether there are plans without modification date, if an automated fix is
            available and a string of their IDs
    """
    plans: List[AbstractPlan] = plan_gateway.get_all_plans()

    to_be_processed = []
    for plan in plans:
        if not hasattr(plan, "date_modified") or plan.date_modified is None:
            to_be_processed.append(plan)

    if not to_be_processed:
        return True, False, None
    if not fix:
        ids = [plan.id for plan in to_be_processed]
        message = (
            WARNING
            + "The following workflows have incorrect modification date (use 'renku doctor --fix' to fix them):\n\t"
            + "\n\t".join(ids)
        )
        return False, True, message

    fix_plan_dates(plans=to_be_processed, plan_gateway=plan_gateway)
    project_context.database.commit()
    communication.info("Workflow modification dates were fixed")

    return True, False, None


def fix_plan_dates(plans: List[AbstractPlan], plan_gateway):
    """Set modification date on a list of plans and fix their creation date."""
    processed = set()
    # NOTE: switch creation date for modification date
    for tail in plans:
        to_be_processed: List[AbstractPlan] = []
        if tail not in processed:
            processed.add(tail)
            to_be_processed.append(tail)
        creation_date = tail.date_created
        plan = tail

        while plan.is_derivation():
            plan = cast(AbstractPlan, plan_gateway.get_by_id(plan.derived_from))
            creation_date = plan.date_created
            if plan not in processed:
                processed.add(plan)
                to_be_processed.append(plan)

        while to_be_processed:
            plan = to_be_processed.pop()
            plan.unfreeze()
            plan.date_modified = plan.date_created
            plan.date_created = creation_date
            if plan.date_removed and plan.date_removed < plan.date_created:
                plan.date_removed = plan.date_created + timedelta(seconds=1)
            plan.freeze()


@inject.autoparams("plan_gateway")
def check_plan_id(fix, plan_gateway: IPlanGateway, **_) -> Tuple[bool, bool, Optional[str]]:
    """Check if all plans have correct IDs.

    Args:
        fix(bool): Whether to fix found issues.
        plan_gateway(IPlanGateway): Injected PlanGateway.
        _: keyword arguments.

    Returns:
        Tuple[bool, Optional[str]]: Tuple of whether there are plans with invalid IDs, if an automated fix is
            available and a string of their IDs
    """
    plans: List[AbstractPlan] = plan_gateway.get_all_plans()

    to_be_processed = []
    for plan in plans:
        if isinstance(plan.id, str) and plan.id.startswith("/plans//plans"):
            to_be_processed.append(plan)

    if not to_be_processed:
        return True, False, None
    if not fix:
        ids = [plan.id for plan in to_be_processed]
        message = (
            WARNING
            + "The following workflows have incorrect IDs (use 'renku doctor --fix' to fix them):\n\t"
            + "\n\t".join(ids)
        )
        return False, True, message

    for plan in to_be_processed:
        plan.unfreeze()
        plan.id = plan.id.replace("//plans/", "/")
        plan.freeze()
    project_context.database.commit()
    communication.info("Workflow IDs were fixed")

    return True, False, None
