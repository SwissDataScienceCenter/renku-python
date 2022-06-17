# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Checks needed to determine integrity of plans."""

import click

from renku.command.command_builder import inject
from renku.command.echo import WARNING
from renku.core.interface.plan_gateway import IPlanGateway
from renku.core.util import communication
from renku.core.workflow.plan import get_derivative_chain


@inject.autoparams("plan_gateway")
def check_deleted_plan_chain(client, fix, plan_gateway: IPlanGateway):
    """Check that all plans in a derivation chain are deleted if one of them is deleted."""
    wrong_plans = set()

    plans = plan_gateway.get_all_plans()
    for plan in plans:
        if plan.deleted:
            for derivative_plan in get_derivative_chain(plan):
                if not derivative_plan.deleted:
                    if fix:
                        communication.info(f"Fixing plan '{plan.name}'")
                        derivative_plan.delete(when=plan.invalidated_at)
                    else:
                        wrong_plans.add(plan.name)

    if not wrong_plans:
        return True, None

    problems = (
        WARNING
        + "There are plans that are not deleted correctly (use 'renku doctor --fix' to fix them):\n\n\t"
        + "\n\t".join(click.style(p, fg="yellow") for p in wrong_plans)
        + "\n"
    )

    return False, problems
