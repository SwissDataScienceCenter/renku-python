#
# Copyright 2021 Swiss Data Science Center (SDSC)
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
"""Renku CLI fixtures for execution management."""

from typing import TYPE_CHECKING, List, NamedTuple, Optional, Tuple, Union

import pytest

if TYPE_CHECKING:
    from renku.domain_model.provenance.activity import Activity


class Result(NamedTuple):
    """CLI execution result."""

    exit_code: int
    activities: Union[None, "Activity", List["Activity"]]


@pytest.fixture
def renku_cli(project, run, with_injection):
    """Return a callable Renku CLI.

    It returns the exit code and the resulting activity or list of activities.
    """
    from renku.command.command_builder.command import inject
    from renku.core.interface.activity_gateway import IActivityGateway
    from renku.domain_model.provenance.activity import Activity

    def renku_cli_(*args, **kwargs) -> Tuple[int, Union[None, Activity, List[Activity]]]:
        @inject.autoparams()
        def _get_activities(activity_gateway: IActivityGateway):
            return {a.id: a for a in activity_gateway.get_all_activities()}

        with with_injection(project):
            activities_before = _get_activities()

        str_args = [str(a) for a in args]

        exit_code = run(str_args, **kwargs)

        with with_injection(project):
            activities_after = _get_activities()

        new_activities = [a for id, a in activities_after.items() if id not in activities_before]

        result: Optional[Union[Activity, List[Activity]]] = new_activities

        if len(new_activities) == 0:
            result = None
        elif len(new_activities) == 1:
            result = new_activities[0]

        return Result(exit_code, result)

    return renku_cli_
