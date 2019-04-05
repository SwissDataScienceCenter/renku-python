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
"""DOI API integration."""
import re

import attr
import requests

doi_regexp = re.compile(
    r'(doi:\s*|(?:https?://)?(?:dx\.)?doi\.org/)?(10\.\d+(.\d+)*/.+)$',
    flags=re.I
)
"""See http://en.wikipedia.org/wiki/Digital_object_identifier."""


@attr.s
class DOIMetadata:
    """Response from doi.org for DOI metadata."""

    id = attr.ib(kw_only=True)
    type = attr.ib(kw_only=True)
    categories = attr.ib(kw_only=True)
    author = attr.ib(kw_only=True)
    issued = attr.ib(kw_only=True)
    title = attr.ib(kw_only=True)
    abstract = attr.ib(kw_only=True)
    language = attr.ib(kw_only=True)
    publisher = attr.ib(kw_only=True)
    DOI = attr.ib(kw_only=True)
    URL = attr.ib(kw_only=True)


class DOIProvider:
    """doi.org registry API provider."""

    headers = {'accept': 'text/x-bibliography; style=apa'}
    timeout = 3

    def __init__(self, uri):
        """Build provider for given URI."""
        self.uri = uri

    @staticmethod
    def is_doi(uri):
        """Check if uri is DOI."""
        return doi_regexp.match(uri)

    def query(self, doi):
        """Retrieve metadata for given doi."""
        if doi.startswith('http://'):
            url = doi
        else:
            url = 'http://dx.doi.org/' + doi

        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            raise LookupError('record not found')

        return response

    def as_obj(self):
        """Get result as ``DOIMetadata`` instance."""
        response = self.as_json()
        return DOIMetadata(**response)

    def as_json(self):
        """Get result as ``dict`` instance."""
        self.headers['accept'] = 'application/vnd.citationstyles.csl+json'
        response = self.query(self.uri)
        return response.json()
