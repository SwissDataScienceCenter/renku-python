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

from typing import List

from renku.core.commands.view_model.log import LogViewModel
from renku.core.management.command_builder import Command, inject
from renku.core.management.interface.activity_gateway import IActivityGateway
from renku.core.management.interface.dataset_gateway import IDatasetGateway
from renku.core.models.dataset import Dataset


def log_command():
    """Return a command for getting a log of renku commands."""
    return Command().command(_log)


@inject.autoparams("activity_gateway", "dataset_gateway")
def _log(
    activity_gateway: IActivityGateway,
    dataset_gateway: IDatasetGateway,
    workflows_only: bool = False,
    datasets_only: bool = False,
) -> List[LogViewModel]:
    """Get a log of renku commands."""

    def _get_all_dataset_versions(dataset: Dataset):
        """Get all datasets provenance for a dataset."""

        current_dataset = dataset

        while current_dataset:
            yield current_dataset

            if current_dataset.is_derivation():
                current_dataset = dataset_gateway.get_by_id(current_dataset.derived_from.url_id)
            else:
                return

    log_entries = []

    if not datasets_only:
        activities = activity_gateway.get_all_activities()
        log_entries.extend(LogViewModel.from_activity(a) for a in activities)

    if not workflows_only:
        datasets = [
            d
            for root_dataset in dataset_gateway.get_provenance_tails()
            for d in _get_all_dataset_versions(root_dataset)
        ]
        log_entries.extend(LogViewModel.from_dataset(d) for d in datasets)

    return log_entries
