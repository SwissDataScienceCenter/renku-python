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
"""Log view model."""

from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import inject

if TYPE_CHECKING:
    from renku.command.view_model.plan import PlanViewModel
    from renku.domain_model.dataset import Dataset
    from renku.domain_model.provenance.activity import Activity


def tabular(data, columns) -> str:
    """Tabular output.

    Args:
        data: Input data.
        columns: Columns to show.

    Returns:
        str: data in tabular form.
    """
    from renku.command.format.tabulate import tabulate

    return tabulate(data, columns, columns_mapping=LOG_COLUMNS, reverse=True)


def json(data, columns) -> str:
    """JSON output.

    Args:
        data: Input data.
        columns: Not used.

    Returns:
        str: Data in JSON format.
    """
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
    source: Optional[str] = None


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

    def to_dict(self) -> Dict[str, Any]:
        """Return a dict representation of this view model."""
        raise NotImplementedError()

    @classmethod
    def from_activity(cls, activity: "Activity") -> "ActivityLogViewModel":
        """Create a log entry from an activity.

        Args:
            activity("Activity"): Activity to create log entry from.

        Returns:
            Log entry for activity.
        """
        from renku.command.view_model.plan import PlanViewModel
        from renku.domain_model.provenance.agent import Person, SoftwareAgent

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
            date=activity.ended_at_time or activity.started_at_time or datetime.utcfromtimestamp(0),
            description=command,
            details=details,
            agents=[a.full_identity for a in activity.agents],
            plan=PlanViewModel.from_plan(activity.plan_with_values),
        )

    @classmethod
    def from_dataset(cls, dataset: "Dataset") -> "DatasetLogViewModel":
        """Create a log entry from an activity.

        Args:
            dataset("Dataset"): Dataset to create log entry for.

        Returns:
            Log entry for dataset.
        """
        from renku.core.interface.dataset_gateway import IDatasetGateway

        dataset_gateway = inject.instance(IDatasetGateway)

        descriptions = [f"Dataset '{dataset.name}': "]
        details = DatasetChangeDetailsViewModel()

        if not dataset.derived_from and not dataset.same_as:
            descriptions.append("created")
            details.created = True
        elif dataset.same_as:
            descriptions.append("imported")
            details.imported = True
            details.source = dataset.same_as.value
        elif dataset.derived_from and dataset.date_removed:
            descriptions.append("deleted")
            details.deleted = True

        previous_dataset: Optional[Dataset] = None

        if dataset.is_derivation():
            previous_dataset = dataset_gateway.get_by_id(dataset.derived_from.value)  # type: ignore

        current_files = {f for f in dataset.dataset_files if not f.date_removed}
        previous_files = set()

        if previous_dataset:
            previous_files = {f for f in previous_dataset.dataset_files if not f.date_removed}

        if (
            previous_files
            and {f.id for f in current_files}.difference({f.id for f in previous_files})
            or not previous_files
            and current_files
        ):
            # NOTE: Files added
            if previous_files:
                new_files = current_files - previous_files
            else:
                new_files = current_files

            descriptions.append(f"{len(new_files)} file(s) added")
            details.files_added = [str(f.entity.path) for f in new_files]
            details.modified = bool(previous_files)

        if previous_files and {f.id for f in previous_files}.difference({f.id for f in current_files}):
            # NOTE: Files removed
            removed_files = previous_files - current_files
            descriptions.append(f"{len(removed_files)} file(s) removed")
            details.files_removed = [str(f.entity.path) for f in removed_files]
            details.modified = True

        if not previous_dataset:
            # NOTE: Check metadata changes on create/import
            if dataset.title:
                details.title_changed = dataset.title

            if dataset.description:
                details.description_changed = dataset.description

            if dataset.creators:
                details.creators_added = [c.full_identity for c in dataset.creators]

            if dataset.keywords:
                details.keywords_added = [k for k in dataset.keywords]

            if dataset.images:
                details.images_changed_to = [i.content_url for i in dataset.images]
        elif not details.deleted:
            # NOTE: Check metadata changes to previous dataset
            modified = False
            if dataset.title != previous_dataset.title:
                details.title_changed = dataset.title
                modified = True
            if dataset.description != previous_dataset.description:
                details.description_changed = dataset.description
                modified = True

            current_creators = set(dataset.creators or [])
            previous_creators = set(previous_dataset.creators or [])

            if current_creators.difference(previous_creators):
                details.creators_added = [c.full_identity for c in current_creators.difference(previous_creators)]
                modified = True
            if previous_creators.difference(current_creators):
                details.creators_removed = [c.full_identity for c in previous_creators.difference(current_creators)]
                modified = True

            current_keywords = set(dataset.keywords)
            previous_keywords = set(previous_dataset.keywords)

            if current_keywords.difference(previous_keywords):
                details.keywords_added = list(current_keywords.difference(previous_keywords))
                modified = True
            if previous_keywords.difference(current_keywords):
                details.keywords_removed = list(previous_keywords.difference(current_keywords))
                modified = True

            current_images = set(dataset.images) if dataset.images else set()
            previous_images = set(previous_dataset.images) if previous_dataset.images else set()

            if current_images != previous_images:
                details.images_changed_to = [i.content_url for i in current_images]
                modified = True
            if modified:
                details.modified = True
                descriptions.append("metadata modified")

        return DatasetLogViewModel(
            id=dataset.name,
            date=dataset.date_removed
            if dataset.date_removed
            else (
                dataset.date_modified or dataset.date_created or dataset.date_published or datetime.utcfromtimestamp(0)
            ),
            description=descriptions[0] + ", ".join(descriptions[1:]),
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

    def to_dict(self) -> Dict[str, Any]:
        """Return a dict representation of this view model.

        Returns:
            Dict[str,Any]: Dictionary representation of view model.
        """
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

    def __init__(
        self,
        id: str,
        date: datetime,
        description: str,
        details: ActivityDetailsViewModel,
        agents: List[str],
        plan: "PlanViewModel",
    ):
        super().__init__(id, date, description, agents)
        self.details = details
        self.plan = plan

    def to_dict(self) -> Dict[str, Any]:
        """Return a dict representation of this view model.

        Returns:
            Dict[str,Any]: Dictionary representation of view model.
        """
        return {
            "date": self.date.isoformat(),
            "type": self.type,
            "description": self.description,
            "agents": self.agents,
            "details": asdict(self.details),
            "plan": {"id": self.plan.id, "name": self.plan.name},
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
