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
from operator import attrgetter

from renku.core import errors


def tabulate(collection, columns, columns_mapping, columns_alignments=None, sort=True, reverse=False) -> str:
    """Format collection with a tabular output.

    Args:
        collection: Collection to format.
        columns: Columns to show.
        columns_mapping: Mapping of collection fields to columns.
        columns_alignments: Column alignment (Default value = None).
        sort: Whether to sort by first column or not (Default value = True).
        reverse: Whether to sort in reverse (Default value = False).
    """
    from renku.core.util.tabulate import tabulate as to_table

    if not columns:
        raise errors.ParameterError("Columns cannot be empty.")

    columns = [c.lower().strip() for c in columns.split(",") if c]

    headers, alignments = _make_headers(columns, columns_mapping, columns_alignments)

    # Sort based on the first requested field
    if sort:
        try:
            attr = list(headers.keys())[0]
            getter = attrgetter(attr)
            collection = sorted(collection, key=getter, reverse=reverse)
        except TypeError:
            pass

    alignments = alignments if collection else None  # To avoid a tabulate bug

    return to_table(collection, headers=headers, colalign=alignments, disable_numparse=True)


def _make_headers(columns, columns_mapping, columns_alignments):
    columns_alignments = columns_alignments or {}
    headers = OrderedDict()
    alignments = []
    for column in columns:
        if column not in columns_mapping:
            raise errors.ParameterError(
                'Invalid column name: "{}".\nPossible values: {}'.format(column, ", ".join(columns_mapping))
            )
        name, display_name = columns_mapping.get(column)
        headers[name] = display_name

        alignment = columns_alignments.get(column, "left")
        alignments.append(alignment)

    return headers, alignments
