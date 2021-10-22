# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
"""Log view model."""

from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, List

from renku.core.commands.format.tabulate import tabulate

if TYPE_CHECKING:
    from renku.core.models.provenance.activity import Activity


def tabular(data, columns):
    """Tabular output."""
    return tabulate(data, columns, columns_mapping=LOG_COLUMNS, reverse=True)


def json(data, columns):
    """JSON output."""
    import json

    data = sorted(data, key=lambda x: x.date)
    return json.dumps([d.to_dict() for d in data], indent=2)


class LogType(str, Enum):
    """Enum of different types of Log entries."""

    ACTIVITY = "Run"
    DATASET = "Dataset"


class LogViewModel:
    """ViewModel for renku log entries."""

    def __init__(self, date: datetime, type: LogType, description: str, agents: List[str]):
        self.date = date
        self.type = type.value
        self.description = description
        self.agents = agents

    def to_dict(self):
        """Return a dict representation of this view model."""
        return {
            "date": self.date.isoformat(),
            "type": self.type,
            "description": self.description,
            "agents": self.agents,
        }

    @classmethod
    def from_activity(cls, activity: "Activity"):
        """Create a log entry from an activity."""
        return cls(
            date=activity.ended_at_time,
            type=LogType.ACTIVITY,
            description=" ".join(activity.plan_with_values.to_argv()),
            agents=[a.full_identity for a in activity.agents],
        )


LOG_COLUMNS = {
    "date": ("date", None),
    "type": ("type", None),
    "description": ("description", None),
    "actors": ("agents", "actors"),
}

LOG_FORMATS = {
    "tabular": tabular,
    "json": json,
}
