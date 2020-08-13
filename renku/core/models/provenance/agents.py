# -*- coding: utf-8 -*-
#
# Copyright 2018-2020- Swiss Data Science Center (SDSC)
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

import os
import pathlib
import re
import urllib
import uuid
from urllib.parse import quote

import attr
from attr.validators import instance_of
from calamus.schema import JsonLDSchema
from marshmallow import EXCLUDE

from renku.core.models.calamus import fields, prov, rdfs, schema, wfprov
from renku.core.models.git import get_user_info
from renku.version import __version__, version_url


@attr.s(slots=True,)
class Person:
    """Represent a person."""

    client = attr.ib(default=None, kw_only=True)

    name = attr.ib(kw_only=True, validator=instance_of(str))
    email = attr.ib(default=None, kw_only=True)
    label = attr.ib(kw_only=True)
    affiliation = attr.ib(default=None, kw_only=True,)
    alternate_name = attr.ib(default=None, kw_only=True,)
    _id = attr.ib(default=None, kw_only=True)

    def default_id(self):
        """Set the default id."""
        return generate_person_id(email=self.email, client=self.client)

    @email.validator
    def check_email(self, attribute, value):
        """Check that the email is valid."""
        if self.email and not (isinstance(value, str) and re.match(r"[^@]+@[^@]+\.[^@]+", value)):
            raise ValueError("Email address is invalid.")

    @label.default
    def default_label(self):
        """Set the default label."""
        return self.name

    @classmethod
    def from_commit(cls, commit):
        """Create an instance from a Git commit."""
        return cls(name=commit.author.name, email=commit.author.email,)

    @property
    def short_name(self):
        """Gives full name in short form."""
        names = self.name.split()
        if len(names) == 1:
            return self.name

        last_name = names[-1]
        initials = [name[0] for name in names]
        initials.pop()

        return "{0}.{1}".format(".".join(initials), last_name)

    @property
    def full_identity(self):
        """Return name, email, and affiliation."""
        email = f" <{self.email}>" if self.email else ""
        affiliation = f" [{self.affiliation}]" if self.affiliation else ""
        return f"{self.name}{email}{affiliation}"

    @classmethod
    def from_git(cls, git):
        """Create an instance from a Git repo."""
        name, email = get_user_info(git)
        return cls(name=name, email=email)

    @classmethod
    def from_string(cls, string):
        """Create an instance from a 'Name <email>' string."""
        regex_pattern = r"([^<>\[\]]*)" r"(?:<{1}\s*(\S+@\S+\.\S+){0,1}\s*>{1}){0,1}\s*" r"(?:\[{1}(.*)\]{1}){0,1}"
        name, email, affiliation = re.search(regex_pattern, string).groups()
        if name:
            name = name.strip()
        if affiliation:
            affiliation = affiliation.strip()
        affiliation = affiliation or None

        return cls(name=name, email=email, affiliation=affiliation)

    @classmethod
    def from_dict(cls, obj):
        """Create and instance from a dictionary."""
        return cls(**obj)

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return PersonSchema().load(data)

    def __attrs_post_init__(self):
        """Finish object initialization."""
        # handle the case where ids were improperly set
        if self._id == "mailto:None" or self._id is None:
            self._id = self.default_id()

        if self.label is None:
            self.label = self.default_label()


class PersonSchema(JsonLDSchema):
    """Person schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Person, schema.Person]
        model = Person
        unknown = EXCLUDE

    name = fields.String(schema.name)
    email = fields.String(schema.email, missing=None)
    label = fields.String(rdfs.label)
    affiliation = fields.String(schema.affiliation, missing=None)
    alternate_name = fields.String(schema.alternateName, missing=None)
    _id = fields.Id(init_name="id")


@attr.s(
    frozen=True, slots=True,
)
class SoftwareAgent:
    """Represent executed software."""

    label = attr.ib(kw_only=True)

    _id = attr.ib(kw_only=True)

    @classmethod
    def from_commit(cls, commit):
        """Create an instance from a Git commit."""
        author = Person.from_commit(commit)
        if commit.author != commit.committer:
            return cls(label=commit.committer.name, id=commit.committer.email)
        return author

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return SoftwareAgentSchema().load(data)

    def as_jsonld(self):
        """Create JSON-LD."""
        return SoftwareAgentSchema().dump(self)


# set up the default agent

renku_agent = SoftwareAgent(label="renku {0}".format(__version__), id=version_url)


def generate_person_id(email, client=None):
    """Generate Person default id."""
    if email:
        return "mailto:{email}".format(email=email)

    host = "localhost"
    if client:
        host = client.remote.get("host") or host
    host = os.environ.get("RENKU_DOMAIN") or host

    id_ = str(uuid.uuid4())

    return urllib.parse.urljoin(
        "https://{host}".format(host=host), pathlib.posixpath.join("/persons", quote(id_, safe=""))
    )


class SoftwareAgentSchema(JsonLDSchema):
    """SoftwareAgent schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.SoftwareAgent, wfprov.WorkflowEngine]
        model = SoftwareAgent
        unknown = EXCLUDE

    label = fields.String(rdfs.label)
    _id = fields.Id(init_name="id")
