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
"""Serializers for dataset list files."""

from collections import OrderedDict

from renku.cli._echo import echo_via_pager


def tabular(client, records):
    """Format dataset files with a tabular output.

    :param client: LocalClient instance.
    :param records: Filtered collection.
    """
    from renku.models._tabulate import tabulate

    echo_via_pager(
        tabulate(
            records,
            headers=OrderedDict((
                ('added', None),
                ('authors_csv', 'authors'),
                ('dataset', None),
                ('full_path', 'path'),
            )),
        )
    )


def jsonld(client, records):
    """Format dataset files as JSON-LD.

    :param client: LocalClient instance.
    :param records: Filtered collection.
    """
    from renku.models._json import dumps
    from renku.models._jsonld import asjsonld

    data = [asjsonld(record) for record in records]
    echo_via_pager(dumps(data, indent=2))


FORMATS = {
    'tabular': tabular,
    'json-ld': jsonld,
}
"""Valid formatting options."""
