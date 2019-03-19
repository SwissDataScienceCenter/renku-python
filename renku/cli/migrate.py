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
"""Migrate files and metadata to the latest Renku version.

Datasets
~~~~~~~~

The location of dataset metadata files has been changed from the
``data/<name>/metadata.yml`` to ``.renku/datasets/<UUID>/metadata.yml``.
All file paths inside a metadata file are relative to itself and the
``renku migrate datasets`` command will take care of it.
"""

import os

import click
import yaml

from ._client import pass_local_client


@click.group()
def migrate():
    """Migrate to latest Renku version."""


@migrate.command()
@pass_local_client(clean=True, commit=True)
@click.pass_context
def datasets(ctx, client):
    """Migrate dataset metadata."""
    from renku.models._jsonld import asjsonld
    from renku.models.datasets import Dataset
    from renku.models.refs import LinkReference

    from ._checks.location_datasets import _dataset_metadata_pre_0_3_4

    with client.lock:
        for old_path in _dataset_metadata_pre_0_3_4(client):
            with old_path.open('r') as fp:
                dataset = Dataset.from_jsonld(yaml.safe_load(fp))

            name = str(old_path.parent.relative_to(client.path / 'data'))
            new_path = (
                client.renku_datasets_path / dataset.identifier.hex /
                client.METADATA
            )
            new_path.parent.mkdir(parents=True, exist_ok=True)

            dataset = dataset.rename_files(
                lambda key: os.path.relpath(
                    str(old_path.parent / key), start=str(new_path.parent)
                )
            )

            with new_path.open('w') as fp:
                yaml.dump(asjsonld(dataset), fp, default_flow_style=False)

            old_path.unlink()

            LinkReference.create(
                client=client, name='datasets/' + name
            ).set_reference(new_path)
