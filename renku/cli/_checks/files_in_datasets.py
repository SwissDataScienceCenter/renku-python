# -*- coding: utf-8 -*-
#
# Copyright 2019 - Swiss Data Science Center (SDSC)
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
"""Check location of files in datasets."""

import os
from collections import defaultdict

import click

from .._echo import WARNING


def check_missing_files(client):
    """Find missing files listed in datasets."""
    missing = defaultdict(list)

    for path, dataset in client.datasets.items():
        for file in dataset.files:
            filepath = (path.parent / file)
            if not filepath.exists():
                missing[str(
                    path.parent.relative_to(client.renku_datasets_path)
                )].append(
                    os.path.normpath(str(filepath.relative_to(client.path)))
                )

    if not missing:
        return True

    click.secho(
        WARNING + 'There are missing files in datasets.'
        # '\n  (use "renku dataset clean <name>" to clean them)'
    )

    for dataset, files in missing.items():
        click.secho(
            '\n\t' + click.style(dataset, fg='yellow') + ':\n\t  ' +
            '\n\t  '.join(click.style(path, fg='red') for path in files)
        )

    return False
