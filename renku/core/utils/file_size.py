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
"""Helper utils for handling file size strings."""

import re

units = {
    'b': 1,
    'kb': 1000,
    'mb': 1000**2,
    'gb': 1000**3,
    'tb': 1000**4,
    'm': 1000**2,
    'g': 1000**3,
    't': 1000**4,
    'p': 1000**5,
    'e': 1000**6,
    'z': 1000**7,
    'y': 1000**8,
    'ki': 1024,
    'mi': 1024**2,
    'gi': 1024**3,
    'ti': 1024**4,
    'pi': 1024**5,
    'ei': 1024**6,
    'zi': 1024**7,
    'yi': 1024**8,
}


def parse_file_size(size_str):
    """Parse a human readable filesize to bytes."""
    res = re.search(r'([0-9.]+)([a-zA-Z]{1,2})', size_str)
    if not res or res.group(2).lower() not in units:
        raise ValueError(
            'Supplied file size does not contain a unit. '
            'Valid units are: {}'.format(', '.join(units.keys()))
        )

    value = float(res.group(1))
    unit = units[res.group(2).lower()]

    return int(value * unit)
