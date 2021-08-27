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

import pytest


@pytest.fixture
def renku_cli(client, run, client_database_injection_manager):
    """Return a callable Renku CLI.

    It returns the exit code and content of the resulting CWL tool.
    """
    from renku.core.management.command_builder.command import inject
    from renku.core.management.interface.activity_gateway import IActivityGateway

    def renku_cli_(*args, **kwargs):
        @inject.autoparams()
        def _get_activities(activity_gateway: IActivityGateway):
            return {a.id: a for a in activity_gateway.get_latest_activity_per_plan().values()}

        with client_database_injection_manager(client):
            activities_before = _get_activities()

        exit_code = run(args, **kwargs)

        with client_database_injection_manager(client):
            activities_after = _get_activities()

        new_activities = set(activities_after.keys()).difference(set(activities_before.keys()))

        assert len(new_activities) <= 1

        if new_activities:
            return exit_code, activities_after[new_activities.pop()]

        return exit_code, None

    return renku_cli_
