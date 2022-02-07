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

from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, List, Optional, Tuple

import inject

from renku.core.commands.format.tabulate import tabulate

if TYPE_CHECKING:
    from renku.core.models.dataset import Dataset
    from renku.core.models.provenance.activity import Activity


def tabular(data, columns) -> str:
    """Tabular output."""
    return tabulate(data, columns, columns_mapping=LOG_COLUMNS, reverse=True)


def json(data, columns) -> str:
    """JSON output."""
    import json

    data = sorted(data, key=lambda x: x.date)
    return json.dumps([d.to_dict() for d in data], indent=2)


class LogType(str, Enum):
    """Enum of different types of Log entries."""

    ACTIVITY = "Run"
    DATASET = "Dataset"


@dataclass
class ActivityDetailsViewModel:
    """View Model for Activity details."""

    start_time: str
    end_time: str
    renku_version: Optional[str] = None
    user: Optional[str] = None
    inputs: Optional[List[Tuple[str, str]]] = None
    outputs: Optional[List[Tuple[str, str]]] = None
    parameters: Optional[List[Tuple[str, str]]] = None


@dataclass
class DatasetChangeDetailsViewModel:
    """View model for detailed changes in a dataset."""

    created: bool = False
    imported: bool = False
    deleted: bool = False
    migrated: bool = False
    modified: bool = False
    files_added: Optional[List[str]] = None
    files_removed: Optional[List[str]] = None
    title_changed: Optional[str] = None
    description_changed: Optional[str] = None
    creators_added: Optional[List[str]] = None
    creators_removed: Optional[List[str]] = None
    keywords_added: Optional[List[str]] = None
    keywords_removed: Optional[List[str]] = None
    images_changed_to: Optional[List[str]] = None


class LogViewModel:
    """ViewModel for renku log entries."""

    def __init__(
        self,
        id: str,
        date: datetime,
        description: str,
        agents: List[str],
    ):
        self.id = id
        self.date = date
        self.description = description
        self.agents = agents

    def to_dict(self):
        """Return a dict representation of this view model."""
        raise NotImplementedError()

    @classmethod
    def from_activity(cls, activity: "Activity"):
        """Create a log entry from an activity."""
        from renku.core.models.provenance.agent import Person, SoftwareAgent

        plan = activity.plan_with_values

        details = ActivityDetailsViewModel(
            start_time=activity.started_at_time.isoformat(), end_time=activity.ended_at_time.isoformat()
        )

        user = next((a for a in activity.agents if isinstance(a, Person)), None)
        renku_user = next((a for a in activity.agents if isinstance(a, SoftwareAgent)), None)

        if user:
            details.user = user.full_identity

        if renku_user:
            details.renku_version = renku_user.name

        command = " ".join(activity.plan_with_values.to_argv())

        if plan.inputs:
            details.inputs = [(input.name, input.actual_value) for input in plan.inputs]

        if plan.outputs:
            details.outputs = [(output.name, output.actual_value) for output in plan.outputs]

        if plan.parameters:
            details.parameters = [(parameter.name, parameter.actual_value) for parameter in plan.parameters]

        return ActivityLogViewModel(
            id=activity.id,
            date=activity.ended_at_time,
            description=command,
            details=details,
            agents=[a.full_identity for a in activity.agents],
        )

    @classmethod
    def from_dataset(cls, dataset: "Dataset"):
        """Create a log entry from an activity."""
        from renku.core.management.interface.dataset_gateway import IDatasetGateway
        from renku.core.models.dataset import DatasetChangeType

        dataset_gateway = inject.instance(IDatasetGateway)

        if not dataset.change_type:
            return

        descriptions = [f"Dataset '{dataset.name}': "]
        details = DatasetChangeDetailsViewModel()

        if dataset.change_type & DatasetChangeType.CREATED:
            descriptions.append("created")
            details.created = True
        elif dataset.change_type & DatasetChangeType.IMPORTED:
            descriptions.append("imported")
            details.imported = True
        elif dataset.change_type & DatasetChangeType.INVALIDATED:
            descriptions.append("deleted")
            details.deleted = True
        elif dataset.change_type & DatasetChangeType.MIGRATED:
            descriptions.append("migrated")
            details.migrated = True

        previous_dataset = None

        if dataset.derived_from:
            previous_dataset = dataset_gateway.get_by_id(dataset.derived_from.url_id)

        current_files = {f for f in dataset.dataset_files if not f.date_removed}
        previous_files = set()

        if previous_dataset:
            previous_files = {f for f in previous_dataset.dataset_files if not f.date_removed}

        if dataset.change_type & DatasetChangeType.FILES_ADDED:
            if previous_files:
                new_files = current_files - previous_files
            else:
                new_files = current_files

            descriptions.append(f"{len(new_files)} file(s) added")
            details.files_added = [str(f.entity.path) for f in new_files]
            details.modified = True

        if dataset.change_type & DatasetChangeType.FILES_REMOVED and previous_files:
            removed_files = previous_files - current_files
            descriptions.append(f"{len(removed_files)} file(s) removed")
            details.files_removed = [str(f.entity.path) for f in removed_files]
            details.modified = True

        if dataset.change_type & DatasetChangeType.METADATA_CHANGED and previous_dataset or not previous_dataset:
            if (
                not dataset.change_type & DatasetChangeType.CREATED
                and not dataset.change_type & DatasetChangeType.IMPORTED
            ):
                descriptions.append("metadata modified")
                details.modified = True

            if not previous_dataset or dataset.title != previous_dataset.title:
                details.title_changed = dataset.title

            if not previous_dataset or dataset.description != previous_dataset.description:
                details.description_changed = dataset.description

            if not previous_dataset or sorted(dataset.creators, key=lambda x: x.id) != sorted(
                previous_dataset.creators, key=lambda x: x.id
            ):
                if previous_dataset:
                    added_creators = set(dataset.creators) - set(previous_dataset.creators)
                    removed_creators = set(previous_dataset.creators) - set(dataset.creators)
                else:
                    added_creators = set(dataset.creators)
                    removed_creators = None

                if added_creators:
                    details.creators_added = [c.full_identity for c in added_creators]

                if removed_creators:
                    details.creators_removed = [c.full_identity for c in removed_creators]

            if (not previous_dataset and dataset.keywords) or (
                previous_dataset and sorted(dataset.keywords) != sorted(previous_dataset.keywords)
            ):
                if previous_dataset:
                    added_keywords = set(dataset.keywords) - set(previous_dataset.keywords)
                    removed_keywords = set(previous_dataset.keywords) - set(dataset.keywords)
                else:
                    added_keywords = set(dataset.keywords)
                    removed_keywords = None

                if added_keywords:
                    details.keywords_added = [k for k in added_keywords]
                if removed_keywords:
                    details.keywords_removed = [k for k in removed_keywords]

            if (not previous_dataset and dataset.images) or (
                previous_dataset
                and sorted(dataset.images, key=lambda x: x.id) != sorted(previous_dataset.images, key=lambda x: x.id)
            ):
                details.images_changed_to = [i.content_url for i in dataset.images]

        return DatasetLogViewModel(
            id=dataset.name,
            date=dataset.date_modified,
            description=descriptions[0] + " ".join(descriptions[1:]),
            details=details,
            agents=[c.full_identity for c in dataset.creators],
        )


class DatasetLogViewModel(LogViewModel):
    """View model for a dataset log entry."""

    type = LogType.DATASET.value

    def __init__(
        self, id: str, date: datetime, description: str, details: DatasetChangeDetailsViewModel, agents: List[str]
    ):
        super().__init__(id, date, description, agents)
        self.details = details

    def to_dict(self):
        """Return a dict representation of this view model."""
        return {
            "date": self.date.isoformat(),
            "type": self.type,
            "description": self.description,
            "agents": self.agents,
            "details": asdict(self.details),
        }


class ActivityLogViewModel(LogViewModel):
    """View model for an activity log entry."""

    type = LogType.ACTIVITY.value

    def __init__(self, id: str, date: datetime, description: str, details: ActivityDetailsViewModel, agents: List[str]):
        super().__init__(id, date, description, agents)
        self.details = details

    def to_dict(self):
        """Return a dict representation of this view model."""
        return {
            "date": self.date.isoformat(),
            "type": self.type,
            "description": self.description,
            "agents": self.agents,
            "details": asdict(self.details),
        }


LOG_COLUMNS = {
    "id": ("id", None),
    "date": ("date", None),
    "type": ("type", None),
    "description": ("description", None),
    "actors": ("agents", "actors"),
}

LOG_FORMATS = {
    "tabular": tabular,
    "json": json,
    "detailed": None,
}
