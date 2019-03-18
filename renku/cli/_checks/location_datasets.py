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
"""Check location of dataset metadata files."""

import click

from .._echo import WARNING


def _dataset_metadata_pre_0_3_4(client):
    """Return paths of dataset metadata for pre 0.3.4."""
    return (client.path / 'data').rglob('metadata.yml')


def check_dataset_metadata(client):
    """Check location of dataset metadata."""
    # Find pre 0.3.4 metadata files.
    old_metadata = list(_dataset_metadata_pre_0_3_4(client))

    if not old_metadata:
        return True

    click.secho(
        WARNING + 'There are metadata files in the old location.'
        '\n  (use "renku migrate datasets" to move them)\n\n\t' + '\n\t'.join(
            click.style(str(path.relative_to(client.path)), fg='yellow')
            for path in old_metadata
        ) + '\n'
    )

    return False
