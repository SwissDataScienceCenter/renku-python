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
"""Serializers for dataset tags list."""

from collections import OrderedDict

from renku.core.models.tabulate import tabulate


def tabular(client, tags):
    """Format dataset tags with a tabular output.

    :param client: LocalClient instance.
    :param tags: Dataset tags.
    """
    return tabulate(
        tags,
        headers=OrderedDict((
            ('created', None),
            ('name', None),
            ('description', None),
            ('dataset', None),
            ('commit', None),
        )),
        # workaround for tabulate issue 181
        # https://bitbucket.org/astanin/python-tabulate/issues/181/disable_numparse-fails-on-empty-input
        disable_numparse=[1, 2, 4] if len(tags) > 0 else False
    )


def jsonld(client, tags):
    """Format dataset tags as JSON-LD.

    :param client: LocalClient instance.
    :param tags: Dataset tags.
    """
    from renku.core.models.json import dumps

    data = [tag.as_jsonld() for tag in tags]
    return dumps(data, indent=2)


DATASET_TAGS_FORMATS = {
    'tabular': tabular,
    'json-ld': jsonld,
}
"""Valid formatting options."""
