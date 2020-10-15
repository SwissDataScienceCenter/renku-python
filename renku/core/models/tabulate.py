# -*- coding: utf-8 -*-
#
# Copyright 2017-2020- Swiss Data Science Center (SDSC)
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
"""Print a collection as a table."""

from datetime import datetime
from operator import attrgetter

from tabulate import tabulate as tblte


def format_cell(cell, datetime_fmt=None):
    """Format a cell."""
    if datetime_fmt and isinstance(cell, datetime):
        if cell.tzinfo:
            cell = cell.astimezone()
        return cell.strftime(datetime_fmt)
    elif isinstance(cell, bool):
        return "*" if cell else ""
    elif isinstance(cell, int):
        return f"{cell:,}"
    return cell


def tabulate(collection, headers, datetime_fmt="%Y-%m-%d %H:%M:%S", **kwargs):
    """Pretty-print a collection."""
    if isinstance(headers, dict):
        attrs = headers.keys()
        # if mapping is not specified keep original
        names = [key if value is None else value for key, value in headers.items()]
    else:
        attrs = names = headers
    table = [
        (format_cell(cell, datetime_fmt=datetime_fmt) for cell in _to_list(attrgetter(*attrs)(c))) for c in collection
    ]
    return tblte(table, headers=[h.upper() for h in names], **kwargs)


def _to_list(value):
    if isinstance(value, (list, tuple)):
        return value
    return [value]
