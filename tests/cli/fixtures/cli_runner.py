# -*- coding: utf-8 -*-
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

from collections import namedtuple
from typing import List, Tuple, Union

import pytest

Result = namedtuple("Result", "exit_code, activities")


@pytest.fixture
def renku_cli(client, run, client_database_injection_manager):
    """Return a callable Renku CLI.

    It returns the exit code and the resulting activity or list of activities.
    """
    from renku.core.management.command_builder.command import inject
    from renku.core.management.interface.activity_gateway import IActivityGateway
    from renku.core.models.provenance.activity import Activity

    def renku_cli_(*args, **kwargs) -> Tuple[int, Union[None, Activity, List[Activity]]]:
        @inject.autoparams()
        def _get_activities(activity_gateway: IActivityGateway):
            return {a.id: a for a in activity_gateway.get_latest_activity_per_plan().values()}

        with client_database_injection_manager(client):
            activities_before = _get_activities()

        args = [str(a) for a in args]

        exit_code = run(args, **kwargs)

        with client_database_injection_manager(client):
            activities_after = _get_activities()

        new_activities = [a for id, a in activities_after.items() if id not in activities_before]

        if len(new_activities) == 0:
            new_activities = None
        elif len(new_activities) == 1:
            new_activities = new_activities[0]

        return Result(exit_code, new_activities)

    return renku_cli_
