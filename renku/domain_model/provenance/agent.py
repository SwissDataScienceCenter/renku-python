# -*- coding: utf-8 -*-
#
# Copyright 2018-2022- Swiss Data Science Center (SDSC)
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
from typing import Optional, cast
from urllib.parse import quote

from renku.infrastructure.immutable import Slots
from renku.version import __version__, version_url


class Agent(Slots):
    """Represent executed software."""

    __slots__ = ("id", "name")

    id: str
    name: str

    def __init__(self, *, id: str, name: str, **kwargs):
        super().__init__(id=id, name=name, **kwargs)

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, SoftwareAgent):
            return False
        return self.id == other.id and self.name == other.name

    def __hash__(self):
        return hash((self.id, self.name))

    @property
    def full_identity(self):
        """Return the identity of this Agent."""
        return f"{self.name} <{self.id}>"


class SoftwareAgent(Agent):
    """Represent executed software."""


# set up the default agent
RENKU_AGENT = SoftwareAgent(id=version_url, name=f"renku {__version__}")


class Person(Agent):
    """Represent a person."""

    __slots__ = ("affiliation", "alternate_name", "email")

    affiliation: Optional[str]
    alternate_name: str
    email: str

    def __init__(
        self,
        *,
        affiliation: Optional[str] = None,
        alternate_name: Optional[str] = None,
        email: Optional[str] = None,
        id: Optional[str] = None,
        name: str,
    ):
        self._validate_email(email)

        if id is None or id == "mailto:None" or id.startswith("_:"):
            full_identity = Person.get_full_identity(email, affiliation, name)
            id = Person.generate_id(email, full_identity)

        affiliation = affiliation or None
        alternate_name = alternate_name or None

        super().__init__(
            affiliation=affiliation, alternate_name=alternate_name, email=email, id=cast(str, id), name=name
        )

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, Person):
            return False
        return self.id == other.id and self.full_identity == other.full_identity

    def __hash__(self):
        return hash((self.id, self.full_identity))

    @classmethod
    def from_string(cls, string):
        """Create an instance from a 'Name <email>' string."""
        regex_pattern = r"([^<>\[\]]*)" r"(?:<{1}\s*(\S+@\S+\.\S+){0,1}\s*>{1}){0,1}\s*" r"(?:\[{1}(.*)\]{1}){0,1}"
        match = re.search(regex_pattern, string)
        if match is None:
            raise ValueError(f"Couldn't parse person from string: {string}")
        name, email, affiliation = match.groups()
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

    @staticmethod
    def generate_id(email, full_identity):
        """Generate identifier for Person."""
        # TODO: Do not use email as id
        if email:
            return f"mailto:{email}"

        id = full_identity or str(uuid.uuid4().hex)
        id = quote(id, safe="")

        return f"/persons/{id}"

    @staticmethod
    def _validate_email(email):
        """Check that the email is valid."""
        if not email:
            return
        if not isinstance(email, str) or not re.match(r"[^@]+@[^@]+\.[^@]+", email):
            raise ValueError("Email address is invalid.")

    @staticmethod
    def get_full_identity(email, affiliation, name):
        """Return name, email, and affiliation."""
        email = f" <{email}>" if email else ""
        affiliation = f" [{affiliation}]" if affiliation else ""
        return f"{name}{email}{affiliation}"

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
