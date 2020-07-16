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
"""Checks needed to determine integrity of datasets."""
import os
from collections import defaultdict

import click

from renku.core.management.repository import DEFAULT_DATA_DIR as DATA_DIR

from ..echo import WARNING


def _dataset_pre_0_3(client):
    """Return paths of dataset metadata for pre 0.3.4."""
    project_is_pre_0_3 = int(client.project.version) < 2
    if project_is_pre_0_3:
        return (client.path / DATA_DIR).rglob(client.METADATA)
    return []


def check_dataset_metadata(client):
    """Check location of dataset metadata."""
    # Find pre 0.3.4 metadata files.
    old_metadata = list(_dataset_pre_0_3(client))

    if not old_metadata:
        return True, None

    problems = (
        WARNING + 'There are metadata files in the old location.'
        '\n  (use "renku migrate" to move them)\n\n\t' + '\n\t'.join(
            click.style(str(path.relative_to(client.path)), fg='yellow')
            for path in old_metadata
        ) + '\n'
    )

    return False, problems


def check_missing_files(client):
    """Find missing files listed in datasets."""
    missing = defaultdict(list)

    for dataset in client.datasets.values():
        for file_ in dataset.files:
            path = client.path / file_.path
            file_exists = (
                path.exists() or (file_.external and os.path.lexists(path))
            )
            if not file_exists:
                missing[dataset.name].append(file_.path)

    if not missing:
        return True, None

    problems = (WARNING + 'There are missing files in datasets.')

    for dataset, files in missing.items():
        problems += (
            '\n\t' + click.style(dataset, fg='yellow') + ':\n\t  ' +
            '\n\t  '.join(click.style(path, fg='red') for path in files)
        )

    return False, problems
