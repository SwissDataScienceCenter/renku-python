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
"""Renku datetime utilities."""
import datetime
import re

from dateutil.parser import parse as dateutil_parse_date

regex = (
    r'^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12]['
    r'0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|['
    r'+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$'
)
match_iso8601 = re.compile(regex).match


def validate_iso8601(str_val):
    """Check if datetime string is in ISO8601 format."""
    try:
        if match_iso8601(str_val) is not None:
            return True
    except re.error:
        pass
    return False


def parse_date(value):
    """Convert date to datetime."""
    if value is None:
        return
    if isinstance(value, datetime.datetime):
        date = value
    else:
        date = dateutil_parse_date(value)
    if not date.tzinfo:
        # set timezone to local timezone
        tz = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
        date = date.replace(tzinfo=tz)

    return date
