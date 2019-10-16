# -*- coding: utf-8 -*-
#
# Copyright 2017-2019 - Swiss Data Science Center (SDSC)
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
"""Model objects representing a creator."""
import configparser
import re
import uuid

import attr
from attr.validators import instance_of

from renku.core import errors

from . import jsonld as jsonld


@jsonld.s(
    type='schema:Person',
    context={'schema': 'http://schema.org/'},
    slots=True,
)
class Creator(object):
    """Represent the creator of a resource."""

    client = attr.ib(default=None, kw_only=True)

    affiliation = jsonld.ib(
        default=None, kw_only=True, context='schema:affiliation'
    )

    email = jsonld.ib(default=None, kw_only=True, context='schema:email')

    alternate_name = jsonld.ib(
        default=None, kw_only=True, context='schema:alternateName'
    )

    name = jsonld.ib(
        default=None,
        kw_only=True,
        validator=instance_of(str),
        context='schema:name'
    )

    _id = jsonld.ib(kw_only=True, context='@id')

    @property
    def short_name(self):
        """Gives full name in short form."""
        names = self.name.split()
        if len(names) == 1:
            return self.name

        last_name = names[-1]
        initials = [name[0] for name in names]
        initials.pop()

        return '{0}.{1}'.format('.'.join(initials), last_name)

    @email.validator
    def check_email(self, attribute, value):
        """Check that the email is valid."""
        if self.email and not (
            isinstance(value, str) and re.match(r'[^@]+@[^@]+\.[^@]+', value)
        ):
            raise ValueError('Email address is invalid.')

    @classmethod
    def from_git(cls, git):
        """Create an instance from a Git repo."""
        git_config = git.config_reader()
        try:
            name = git_config.get_value('user', 'name', None)
            email = git_config.get_value('user', 'email', None)
        except (
            configparser.NoOptionError, configparser.NoSectionError
        ):  # pragma: no cover
            raise errors.ConfigurationError(
                'The user name and email are not configured. '
                'Please use the "git config" command to configure them.\n\n'
                '\tgit config --global --add user.name "John Doe"\n'
                '\tgit config --global --add user.email '
                '"john.doe@example.com"\n'
            )

        # Check the git configuration.
        if name is None:  # pragma: no cover
            raise errors.MissingUsername()
        if email is None:  # pragma: no cover
            raise errors.MissingEmail()

        return cls(name=name, email=email)

    @classmethod
    def from_commit(cls, commit):
        """Create an instance from a Git commit."""
        return cls(name=commit.author.name, email=commit.author.email)

    @_id.default
    def default_id(self):
        """Set the default id."""
        if self.email:
            return 'mailto:{email}'.format(email=self.email)
        return '_:{}'.format(str(uuid.uuid4()))

    def __attrs_post_init__(self):
        """Finish object initialization."""
        # handle the case where ids were improperly set
        if self._id == 'mailto:None':
            self._id = self.default_id()


@attr.s
class CreatorsMixin:
    """Mixin for handling creators container."""

    creator = jsonld.container.list(
        Creator, kw_only=True, context='schema:creator'
    )

    @property
    def creators_csv(self):
        """Comma-separated list of creators associated with dataset."""
        return ','.join(creator.name for creator in self.creator)
