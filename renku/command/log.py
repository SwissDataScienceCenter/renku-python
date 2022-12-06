# -*- coding: utf-8 -*-
#
# Copyright 2018-2022 - Swiss Data Science Center (SDSC)
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

from pydantic import validate_arguments

from renku.command.command_builder import Command, inject
from renku.command.view_model.log import LogViewModel
from renku.core.interface.activity_gateway import IActivityGateway
from renku.core.interface.dataset_gateway import IDatasetGateway
from renku.domain_model.dataset import Dataset


def log_command():
    """Return a command for getting a log of renku commands."""
    return Command().command(_log)


@inject.autoparams("activity_gateway", "dataset_gateway")
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def _log(
    activity_gateway: IActivityGateway,
    dataset_gateway: IDatasetGateway,
    workflows_only: bool = False,
    datasets_only: bool = False,
) -> List[LogViewModel]:
    """Get a log of renku commands.

    Args:
        activity_gateway(IActivityGateway): Injected activity gateway.
        dataset_gateway(IDatasetGateway): Injected dataset gateway.
        workflows_only(bool, optional): Whether to show logs for workflows only (Default value = False).
        datasets_only(bool, optional): Whether to show logs for datasets only (Default value = False).

    Returns:
        List of log entries.
    """

    def _get_all_dataset_versions(dataset: Dataset):
        """Get all datasets provenance for a dataset.

        Args:
            dataset(Dataset): Head dataset to get versions for.

        Returns:
            Iterator of all versions of a dataset.
        """

        current_dataset = dataset

        while current_dataset:
            yield current_dataset

            if current_dataset.is_derivation():
                assert current_dataset.derived_from is not None
                current_dataset = dataset_gateway.get_by_id(current_dataset.derived_from.value)
            else:
                return

    log_entries: List[LogViewModel] = []

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
