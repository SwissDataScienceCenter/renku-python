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
"""Represent provenance entities."""

import os
import pathlib
import urllib
import weakref

import attr

from renku.core.models import jsonld as jsonld
from renku.core.models.projects import Project


def _str_or_none(data):
    """Return str representation or None."""
    return str(data) if data is not None else data


@attr.s(cmp=False)
class CommitMixin:
    """Represent a commit mixin."""

    commit = attr.ib(default=None, kw_only=True)
    client = attr.ib(default=None, kw_only=True)
    path = jsonld.ib(
        context='prov:atLocation',
        default=None,
        kw_only=True,
        converter=_str_or_none
    )

    _id = jsonld.ib(default=None, context='@id', kw_only=True)
    _label = jsonld.ib(context='rdfs:label', kw_only=True)
    _project = jsonld.ib(
        context='schema:isPartOf', type=Project, kw_only=True, default=None
    )

    @property
    def submodules(self):
        """Proxy to client submodules."""
        return self.client.submodules

    def default_id(self):
        """Configure calculated ID."""
        if self.commit:
            hexsha = self.commit.hexsha
        else:
            hexsha = 'UNCOMMITTED'

        # Determine the hostname for the resource URIs.
        # If RENKU_DOMAIN is set, it overrides the host from remote.
        # Default is localhost.
        host = 'localhost'
        if self.client:
            host = self.client.remote.get('host') or host
        host = os.environ.get('RENKU_DOMAIN') or host

        # always set the id by the identifier
        return urllib.parse.urljoin(
            'https://{host}'.format(host=host),
            pathlib.posixpath.join(
                '/blob/{hexsha}/{path}'.format(hexsha=hexsha, path=self.path)
            )
        )

    @_label.default
    def default_label(self):
        """Generate a default label."""
        if self.commit:
            hexsha = self.commit.hexsha
        else:
            hexsha = 'UNCOMMITTED'
        if self.path:
            return '{self.path}@{hexsha}'.format(hexsha=hexsha, self=self)
        return '{hexsha}'.format(hexsha=hexsha, self=self)

    def __attrs_post_init__(self):
        """Post-init hook."""
        if self.path:
            path = pathlib.Path(self.path)
            if path.is_absolute():
                self.path = str(path.relative_to(self.client.path))

        # always force "project" to be the current project
        if self.client:
            self._project = self.client.project

        if not self._id:
            self._id = self.default_id()


@jsonld.s(
    type=[
        'prov:Entity',
        'wfprov:Artifact',
    ],
    context={
        'schema': 'http://schema.org/',
        'prov': 'http://www.w3.org/ns/prov#',
        'wfprov': 'http://purl.org/wf4ever/wfprov#',
    },
    cmp=False,
)
class Entity(CommitMixin):
    """Represent a data value or item."""

    _parent = attr.ib(
        default=None,
        kw_only=True,
        converter=lambda value: weakref.ref(value)
        if value is not None else None,
    )

    @classmethod
    def from_revision(
        cls, client, path, revision='HEAD', parent=None, **kwargs
    ):
        """Return dependency from given path and revision."""
        client, commit, path = client.resolve_in_submodules(
            client.find_previous_commit(path, revision=revision),
            path,
        )

        path_ = client.path / path
        if path != '.' and path_.is_dir():
            entity = Collection(
                client=client,
                commit=commit,
                path=path,
                members=[],
                parent=parent,
            )

            for member in path_.iterdir():
                if member.name == '.gitkeep':
                    continue

                try:
                    entity.members.append(
                        cls.from_revision(
                            client,
                            str(member.relative_to(client.path)),
                            commit,
                            parent=entity,
                            **kwargs
                        )
                    )
                except KeyError:
                    pass

        else:
            entity = cls(
                client=client,
                commit=commit,
                path=str(path),
                parent=parent,
                **kwargs
            )

        return entity

    @property
    def parent(self):  # pragma: no cover
        """Return the parent object."""
        return self._parent() if self._parent is not None else None

    @property
    def entities(self):
        """Yield itself."""
        yield self


@jsonld.s(
    type=[
        'prov:Collection',
    ],
    context={
        'prov': 'http://www.w3.org/ns/prov#',
    },
    cmp=False,
)
class Collection(Entity):
    """Represent a directory with files."""

    members = jsonld.ib(context='prov:hadMember', kw_only=True)

    @members.default
    def default_members(self):
        """Generate default members as entities from current path."""
        dir_path = self.client.path / self.path
        assert dir_path.is_dir()

        members = []
        for path in dir_path.iterdir():
            if path.name == '.gitkeep':
                continue  # ignore empty directories in Git repository
            cls = Collection if path.is_dir() else Entity
            members.append(
                cls(
                    commit=self.commit,
                    client=self.client,
                    path=str(path.relative_to(self.client.path)),
                    parent=self,
                )
            )
        return members

    @property
    def entities(self):
        """Recursively return all files."""
        for member in self.members:
            yield from member.entities
        yield self
