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
from typing import TYPE_CHECKING, Any, Dict, List

import inject
from click import style

from renku.core.commands.format.tabulate import tabulate

if TYPE_CHECKING:
    from renku.core.models.dataset import Dataset
    from renku.core.models.provenance.activity import Activity


def tabular(data, columns) -> str:
    """Tabular output."""
    return tabulate(data, columns, columns_mapping=LOG_COLUMNS, reverse=True)


def detailed(data, columns) -> str:
    """Detailed output (similar to git)."""
    entries = sorted(data, key=lambda x: x.date, reverse=True)
    descriptions = [d.detailed_description for d in entries]

    return "\n\n".join(descriptions)


def json(data, columns) -> str:
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

    def __init__(
        self,
        date: datetime,
        type: LogType,
        description: str,
        detailed_description: str,
        details: Dict[Any, Any],
        agents: List[str],
    ):
        self.date = date
        self.type = type.value
        self.description = description
        self.detailed_description = detailed_description
        self.details = details
        self.agents = agents

    def to_dict(self):
        """Return a dict representation of this view model."""
        return {
            "date": self.date.isoformat(),
            "type": self.type,
            "description": self.description,
            "agents": self.agents,
            "details": self.details,
        }

    @classmethod
    def from_activity(cls, activity: "Activity"):
        """Create a log entry from an activity."""
        from renku.core.models.provenance.agent import Person, SoftwareAgent

        plan = activity.plan_with_values

        details = {
            "id": activity.id,
            "start_time": activity.started_at_time,
            "end_time": activity.ended_at_time,
            "user": None,
            "renku_version": None,
            "inputs": None,
            "outputs": None,
            "parameters": None,
        }

        detailed_description = style(f"Activity Id: {activity.id}\n", fg="yellow") + (
            f"Start Time: {activity.started_at_time}\n" f"End Time: {activity.ended_at_time}\n"
        )

        user = next((a for a in activity.agents if isinstance(a, Person)), None)
        renku_user = next((a for a in activity.agents if isinstance(a, SoftwareAgent)), None)

        if user:
            detailed_description += f"User: {user.full_identity}"
            details["user"] = user.full_identity

        if renku_user:
            detailed_description += f"Renku Version: {renku_user.name}"
            details["renku_version"] = renku_user.name

        if plan.inputs:
            input_desc = "\n\t".join(f"{input.name}: {input.actual_value}" for input in plan.inputs)
            detailed_description += f"Inputs:\n\t{input_desc}\n"

            details["inputs"] = [{"name": input.name, "value": input.actual_value} for input in plan.inputs]

        if plan.outputs:
            output_desc = "\n\t".join(f"{output.name}: {output.actual_value}" for output in plan.outputs)
            detailed_description += f"Outputs:\n\t{output_desc}\n"

            details["outputs"] = [{"name": output.name, "value": output.actual_value} for output in plan.outputs]

        if plan.parameters:
            parameter_desc = "\n\t".join(f"{parameter.name}: {parameter.actual_value}" for parameter in plan.parameters)
            detailed_description += f"Parameters:\n\t{parameter_desc}\n"

            details["parameters"] = [
                {"name": parameter.name, "value": parameter.actual_value} for parameter in plan.parameters
            ]

        return cls(
            date=activity.ended_at_time,
            type=LogType.ACTIVITY,
            description=" ".join(activity.plan_with_values.to_argv()),
            detailed_description=detailed_description,
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

        descriptions = []
        detailed_descriptions = [f"Date: {dataset.date_modified}"]
        new_files_list = []
        removed_files_list = []

        if dataset.change_type & DatasetChangeType.CREATED:
            descriptions.append(f"Dataset '{dataset.name}' created.")
        elif dataset.change_type & DatasetChangeType.IMPORTED:
            descriptions.append(f"Dataset '{dataset.name}' imported from {dataset.same_as}.")
        elif dataset.change_type & DatasetChangeType.INVALIDATED:
            descriptions.append(f"Dataset '{dataset.name}' deleted.")
        elif dataset.change_type & DatasetChangeType.MIGRATED:
            descriptions.append(f"Dataset '{dataset.name}' migrated.")

        previous_dataset = None

        if dataset.derived_from:
            previous_dataset = dataset_gateway.get_by_id(dataset.derived_from.url_id)

        if dataset.change_type & DatasetChangeType.FILES_ADDED:
            if previous_dataset:
                new_files = set(dataset.dataset_files) - set(previous_dataset.dataset_files)
            else:
                new_files = dataset.dataset_files

            descriptions.append(f"Added {len(new_files)} file(s) to dataset '{dataset.name}'")
            new_files_list = [f.entity.path for f in new_files]
            detailed_descriptions.append("New Files:\n\t" + "\n\t+ ".join(new_files_list))

        if dataset.change_type & DatasetChangeType.FILES_REMOVED:
            removed_files = set(previous_dataset.dataset_files) - set(dataset.dataset_files)
            descriptions.append(f"Removed {len(removed_files)} file(s) from dataset '{dataset.name}'")
            removed_files_list = [f.entity.path for f in removed_files]
            detailed_descriptions.append("Removed Files:\n\t" + "\n\t- ".join(removed_files_list))

        if dataset.change_type & DatasetChangeType.METADATA_CHANGED and previous_dataset:
            descriptions.append(f"Modified metadata of dataset {dataset.name}")

            if dataset.title != previous_dataset.title:
                detailed_descriptions.append(f"Changed title to:\n{dataset.title}")

            if dataset.description != previous_dataset.description:
                detailed_descriptions.append(f"Changed description to:\n{dataset.description}")

            if sorted(dataset.creators, key=lambda x: x.id) != sorted(previous_dataset.creators, key=lambda x: x.id):
                added_creators = set(dataset.creators) - set(previous_dataset.creators)
                removed_creators = set(previous_dataset.creators) - set(dataset.creators)

                if added_creators:
                    detailed_descriptions.append(
                        "Added creators:\n\t" + "\n\t".join(c.full_identity for c in added_creators)
                    )

                if removed_creators:
                    detailed_descriptions.append(
                        "Removed creators:\n\t" + "\n\t".join(c.full_identity for c in removed_creators)
                    )

            if sorted(dataset.keywords) != sorted(previous_dataset.keywords):
                added_keywords = set(dataset.keywords) - set(previous_dataset.keywords)
                removed_keywords = set(previous_dataset.keywords) - set(dataset.keywords)

                if added_keywords:
                    detailed_descriptions.append("Added keywords:\n\t" + "\n\t".join(k for k in added_keywords))
                if removed_keywords:
                    detailed_descriptions.append("Removed keywords:\n\t" + "\n\t".join(k for k in removed_keywords))

            if sorted(dataset.images, key=lambda x: x.id) != sorted(previous_dataset.images, key=lambda x: x.id):
                detailed_descriptions.append(
                    "Changed images to: \n\t" + "\n\t".join(i.content_url for i in dataset.images)
                )

        detailed_description = style(f"Dataset: {dataset.name}\n", fg="yellow") + "\n\n".join(detailed_descriptions)

        return cls(
            date=dataset.date_modified,
            type=LogType.DATASET,
            description=" ".join(descriptions),
            detailed_description=detailed_description,
            details={
                "change_type": str(dataset.change_type),
                "files_added": new_files_list,
                "files_removed": removed_files_list,
            },
            agents=[c.full_identity for c in dataset.creators],
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
    "detailed": detailed,
}
