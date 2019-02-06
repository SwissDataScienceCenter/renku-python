# -*- coding: utf-8 -*-
#
# Copyright 2018 - Swiss Data Science Center (SDSC)
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

import weakref

import attr

from renku.models import _jsonld as jsonld


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

    _id = jsonld.ib(context='@id', kw_only=True)
    _label = jsonld.ib(context='rdfs:label', kw_only=True)
    _project = jsonld.ib(context='dcterms:isPartOf', kw_only=True)

    @property
    def submodules(self):
        """Proxy to client submodules."""
        return self.client.submodules

    @_id.default
    def default_id(self):
        """Configure calculated ID."""
        return 'blob/{self.commit.hexsha}/{self.path}'.format(self=self)

    @_label.default
    def default_label(self):
        """Generate a default label."""
        if self.path:
            return '{self.path}@{self.commit.hexsha}'.format(self=self)
        return '{self.commit.hexsha}'.format(self=self)

    @_project.default
    def default_project(self):
        """Generate a default location."""
        return self.client.project


@jsonld.s(
    type=[
        'prov:Entity',
        'wfprov:Artifact',
    ],
    context={
        'dcterms': 'http://purl.org/dc/terms/',
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
    def from_revision(cls, client, path, revision='HEAD', parent=None):
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
                            parent=entity
                        )
                    )
                except KeyError:
                    pass

        else:
            entity = Entity(
                client=client,
                commit=commit,
                path=str(path),
                parent=parent,
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


@jsonld.s(
    type=[
        'wfdesc:Process',
        'prov:Entity',
        'prov:Plan',
    ],
    context={
        'wfdesc': 'http://purl.org/wf4ever/wfdesc#',
        'prov': 'http://www.w3.org/ns/prov#',
    },
    cmp=False,
)
class Process(CommitMixin):
    """Represent a process."""

    _activity = jsonld.ib(
        context='prov:activity',
        kw_only=True,
        converter=weakref.ref,
    )

    @property
    def activity(self):
        """Return the activity object."""
        return self._activity()


@jsonld.s(
    type=[
        'wfdesc:Workflow',
        'prov:Entity',
        'prov:Plan',
    ],
    context={
        'wfdesc': 'http://purl.org/wf4ever/wfdesc#',
        'prov': 'http://www.w3.org/ns/prov#',
    },
    cmp=False,
)
class Workflow(Process):
    """Represent workflow with subprocesses."""

    subprocesses = jsonld.ib(context='wfdesc:hasSubProcess', kw_only=True)

    @subprocesses.default
    def default_subprocesses(self):
        """Load subprocesses."""
        return [
            subprocess.association.plan
            for subprocess in self.activity.subprocesses.values()
        ]
