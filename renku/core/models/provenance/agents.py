# -*- coding: utf-8 -*-
#
# Copyright 2018-2019- Swiss Data Science Center (SDSC)
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
"""Represent provenance agents."""

import re

from renku.core.models import jsonld as jsonld
from renku.version import __version__, version_url


@jsonld.s(
    type=[
        'prov:Person',
        'schema:Person',
    ],
    context={
        'schema': 'http://schema.org/',
        'prov': 'http://www.w3.org/ns/prov#',
    },
    frozen=True,
    slots=True,
)
class Person:
    """Represent a person."""

    name = jsonld.ib(context='rdfs:label')
    email = jsonld.ib(context='schema:email')

    _id = jsonld.ib(context='@id', init=False, kw_only=True)

    @_id.default
    def default_id(self):
        """Configure calculated ID."""
        return 'mailto:{0}'.format(self.email)

    @email.validator
    def check_email(self, attribute, value):
        """Check that the email is valid."""
        if not (isinstance(value, str) and re.match(r'[^@]+@[^@]+', value)):
            raise ValueError('Email address "{0}" is invalid.'.format(value))

    @classmethod
    def from_commit(cls, commit):
        """Create an instance from a Git commit."""
        return cls(
            name=commit.author.name,
            email=commit.author.email,
        )


@jsonld.s(
    type=[
        'prov:SoftwareAgent',
        'wfprov:WorkflowEngine',
    ],
    context={
        'prov': 'http://www.w3.org/ns/prov#',
        'wfprov': 'http://purl.org/wf4ever/wfprov#',
    },
    frozen=True,
    slots=True,
)
class SoftwareAgent:
    """Represent executed software."""

    label = jsonld.ib(context='rdfs:label', kw_only=True)
    was_started_by = jsonld.ib(
        context='prov:wasStartedBy',
        default=None,
        kw_only=True,
    )

    _id = jsonld.ib(context='@id', kw_only=True)

    @classmethod
    def from_commit(cls, commit):
        """Create an instance from a Git commit."""
        author = Person.from_commit(commit)
        if commit.author != commit.committer:
            return cls(
                label=commit.committer.name,
                id=commit.committer.email,
                was_started_by=author,
            )
        return author


# set up the default agent

renku_agent = SoftwareAgent(
    label='renku {0}'.format(__version__), id=version_url
)
