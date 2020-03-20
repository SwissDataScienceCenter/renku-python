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
"""Tabular format helper functions."""

from collections import OrderedDict

from renku.core import errors
from renku.core.models.tabulate import tabulate as tabulate_


def tabulate(collection, columns, columns_mapping, columns_alignments=None):
    """Format collection with a tabular output."""
    if not columns:
        raise errors.ParameterError('Columns cannot be empty.')

    columns = [c.lower().strip() for c in columns.split(',') if c]

    headers, alignments = _make_headers(
        columns, columns_mapping, columns_alignments
    )

    # Sort based on the first requested field
    attr = list(headers.keys())[0]
    try:
        collection = sorted(collection, key=lambda d: getattr(d, attr))
    except TypeError:
        pass

    alignments = alignments if collection else None  # To avoid a tabulate bug

    return tabulate_(
        collection,
        headers=headers,
        colalign=alignments,
        disable_numparse=True
    )


def _make_headers(columns, columns_mapping, columns_alignments):
    columns_alignments = columns_alignments or {}
    headers = OrderedDict()
    alignments = []
    for column in columns:
        if column not in columns_mapping:
            raise errors.ParameterError(
                'Invalid column name: "{}".\nPossible values: {}'.format(
                    column, ', '.join(columns_mapping)
                )
            )
        name, display_name = columns_mapping.get(column)
        headers[name] = display_name

        alignment = columns_alignments.get(column, 'left')
        alignments.append(alignment)

    return headers, alignments
