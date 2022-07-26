# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Checks for external files."""
import click

from renku.command.command_builder import inject
from renku.command.echo import WARNING
from renku.core.interface.dataset_gateway import IDatasetGateway


@inject.autoparams("dataset_gateway")
def check_missing_external_files(client, dataset_gateway: IDatasetGateway, **kwargs):
    """Find external files that are missing.

    Args:
        client: ``LocalClient``.
        kwargs: keyword arguments.

    Returns:
        Tuple of whether no external files are missing and string of found problems.
    """
    missing = []

    for dataset in dataset_gateway.get_all_active_datasets():
        for file_ in dataset.files:
            if file_.is_external:
                target = (client.path / file_.entity.path).resolve()
                if not target.exists():
                    missing.append((file_.entity.path, str(target)))

    if not missing:
        return True, None

    problems = (
        "\n" + WARNING + "There are missing external files.\n"
        "  (make sure that external paths are accessible)"
        + "\n\n\t"
        + "\n\t".join(
            click.style(path, fg="yellow") + " -> " + click.style(target, fg="red") for path, target in missing
        )
        + "\n"
    )
    return False, problems
