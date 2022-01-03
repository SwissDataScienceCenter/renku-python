# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 - Swiss Data Science Center (SDSC)
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
"""Log of renku commands."""

from renku.core.commands.view_model.log import LogViewModel
from renku.core.management.command_builder import Command, inject
from renku.core.management.interface.activity_gateway import IActivityGateway


def log_command():
    """Return a command for getting a log of renku commands."""
    return Command().command(_log)


@inject.autoparams("activity_gateway")
def _log(activity_gateway: IActivityGateway, workflows_only: bool = False):
    """Get a log of renku commands."""

    activities = activity_gateway.get_all_activities()
    log_entries = [LogViewModel.from_activity(a) for a in activities]

    return log_entries
