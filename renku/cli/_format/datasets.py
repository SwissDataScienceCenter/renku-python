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
"""Serializers for datasets."""

import os
from collections import OrderedDict

import click


def tabular(client, datasets):
    """Format datasets with a tabular output."""
    from renku.models._tabulate import tabulate

    click.echo(
        tabulate(
            datasets,
            headers=OrderedDict((
                ('short_id', 'id'),
                ('name', None),
                ('created', None),
                ('authors_csv', 'authors'),
            )),
        )
    )


def jsonld(client, datasets):
    """Format datasets as JSON-LD."""
    from renku.models._json import dumps
    from renku.models._jsonld import asjsonld

    data = [
        asjsonld(
            dataset,
            basedir=os.path.relpath(
                '.', start=str(dataset.__reference__.parent)
            )
        ) for dataset in datasets
    ]
    click.echo(dumps(data, indent=2))


FORMATS = {
    'tabular': tabular,
    'json-ld': jsonld,
}
"""Valid formatting options."""
