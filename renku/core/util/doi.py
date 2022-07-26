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
"""Helper utilities for handling DOIs."""

import re
import urllib

doi_regexp = re.compile(
    r"(doi:\s*|(?:(?:https|http)://)?(?:(?:dx|www)\.)?doi\.org/)?" + r"(10\.\d+(.\d+)*/.+)$", flags=re.I
)
# NOTE: See http://en.wikipedia.org/wiki/Digital_object_identifier


def is_doi(uri):
    """Check if URI is DOI."""
    return doi_regexp.match(uri)


def extract_doi(uri):
    """Return the DOI in a string if there is one."""
    match = doi_regexp.match(uri)

    if match:
        return match.group(2)


def get_doi_url(identifier) -> str:
    """Return DOI URL for a given id."""
    return urllib.parse.urljoin("https://doi.org", identifier)
