# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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
import uuid
from urllib.parse import quote

from calamus.schema import JsonLDSchema
from marshmallow import EXCLUDE

from renku.core.models.calamus import StringList, fields, prov, rdfs, schema, wfprov
from renku.core.models.git import get_user_info
from renku.core.utils.urls import get_host
from renku.version import __version__, version_url


class Person:
    """Represent a person."""

    __slots__ = ("affiliation", "alternate_name", "email", "id", "label", "name")

    def __init__(
        self,
        *,
        affiliation: str = None,
        alternate_name: str = None,
        email: str = None,
        id: str = None,
        label: str = None,
        name: str,
    ):
        self.validate_email(email)

        if id == "mailto:None" or not id or id.startswith("_:"):
            full_identity = Person.get_full_identity(email, affiliation, name)
            id = Person.generate_id(email, full_identity, hostname=get_host(client=None))
        label = label or name

        self.affiliation: str = affiliation
        self.alternate_name: str = alternate_name
        self.email: str = email
        self.id: str = id
        self.label: str = label
        self.name: str = name

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, Person):
            return False
        return self.id == other.id and self.full_identity == other.full_identity

    def __hash__(self):
        return hash((self.id, self.full_identity))

    @staticmethod
    def generate_id(email, full_identity, hostname):
        """Generate identifier for Person."""
        if email:
            return f"mailto:{email}"

        id = full_identity or str(uuid.uuid4())
        id = quote(id, safe="")

        # TODO: Remove hostname part once migrating to new metadata
        return f"https://{hostname}/persons/{id}"

    @staticmethod
    def validate_email(email):
        """Check that the email is valid."""
        if not email:
            return
        if not isinstance(email, str) or not re.match(r"[^@]+@[^@]+\.[^@]+", email):
            raise ValueError("Email address is invalid.")

    @classmethod
    def from_commit(cls, commit):
        """Create an instance from a Git commit."""
        return cls(name=commit.author.name, email=commit.author.email)

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
        return self.get_full_identity(self.email, self.affiliation, self.name)

    @staticmethod
    def get_full_identity(email, affiliation, name):
        """Return name, email, and affiliation."""
        email = f" <{email}>" if email else ""
        affiliation = f" [{affiliation}]" if affiliation else ""
        return f"{name}{email}{affiliation}"

    @classmethod
    def from_git(cls, git):
        """Create an instance from a Git repo."""
        name, email = get_user_info(git)
        return cls(email=email, name=name)

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

        return cls(affiliation=affiliation, email=email, name=name)

    @classmethod
    def from_dict(cls, data):
        """Create and instance from a dictionary."""
        return cls(**data)

    @classmethod
    def from_jsonld(cls, data):
        """Create an instance from JSON-LD data."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            raise ValueError(data)

        return PersonSchema().load(data)


class PersonSchema(JsonLDSchema):
    """Person schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.Person, schema.Person]
        model = Person
        unknown = EXCLUDE

    affiliation = StringList(schema.affiliation, missing=None)
    alternate_name = StringList(schema.alternateName, missing=None)
    email = fields.String(schema.email, missing=None)
    id = fields.Id()
    label = StringList(rdfs.label, missing=None)
    name = StringList(schema.name, missing=None)


class SoftwareAgent:
    """Represent executed software."""

    __slots__ = ("id", "label")

    def __init__(self, *, id: str, label: str):
        self.id: str = id
        self.label: str = label

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, SoftwareAgent):
            return False
        return self.id == other.id and self.label == other.label

    def __hash__(self):
        return hash((self.id, self.label))

    @classmethod
    def from_commit(cls, commit):
        """Create an instance from a Git commit."""
        # FIXME: This method can return a Person object but SoftwareAgent is not its super class
        author = Person.from_commit(commit)
        if commit.author != commit.committer:
            return cls(label=commit.committer.name, id=commit.committer.email)
        return author


# set up the default agent

renku_agent = SoftwareAgent(label="renku {0}".format(__version__), id=version_url)


class SoftwareAgentSchema(JsonLDSchema):
    """SoftwareAgent schema."""

    class Meta:
        """Meta class."""

        rdf_type = [prov.SoftwareAgent, wfprov.WorkflowEngine]
        model = SoftwareAgent
        unknown = EXCLUDE

    label = fields.String(rdfs.label)
    id = fields.Id()
