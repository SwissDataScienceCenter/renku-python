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

from renku.core.models.json import dumps
from renku.core.models.jsonld import asjsonld
from renku.core.models.tabulate import tabulate


def tabular(client, datasets):
    """Format datasets with a tabular output."""
    return tabulate(
        datasets,
        headers=OrderedDict((
            ('uid', 'id'),
            ('display_name', None),
            ('version', None),
            ('created', None),
            ('creators_csv', 'creators'),
        )),
        # workaround for tabulate issue 181
        # https://bitbucket.org/astanin/python-tabulate/issues/181/disable_numparse-fails-on-empty-input
        disable_numparse=[0, 2] if any(datasets) else False
    )


def jsonld(client, datasets):
    """Format datasets as JSON-LD."""
    data = [
        asjsonld(
            dataset,
            basedir=os.path.relpath(
                '.', start=str(dataset.__reference__.parent)
            )
        ) for dataset in datasets
    ]
    return dumps(data, indent=2)


DATASETS_FORMATS = {
    'tabular': tabular,
    'json-ld': jsonld,
}
"""Valid formatting options."""
