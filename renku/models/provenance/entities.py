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


@attr.s(cmp=False)
class CommitMixin:
    """Represent a commit mixin."""

    commit = attr.ib(default=None, kw_only=True)
    client = attr.ib(default=None, kw_only=True)
    submodules = attr.ib(default=attr.Factory(list), kw_only=True)
    path = attr.ib(default=None, kw_only=True)

    _id = jsonld.ib(context='@id', kw_only=True)
    _label = jsonld.ib(context='rdfs:label', kw_only=True)
    _location = jsonld.ib(context='prov:atLocation', kw_only=True)

    @_id.default
    def default_id(self):
        """Configure calculated ID."""
        return 'blob/{self.commit.hexsha}/{self.path}'.format(self=self)

    @_label.default
    def default_label(self):
        """Generate a default label."""
        return '{self.path}@{self.commit.hexsha}'.format(self=self)

    @_location.default
    def default_location(self):
        """Generate a default location."""
        return self.client.project


@jsonld.s(
    type=[
        'prov:Entity',
        'wfprov:Artifact',
    ],
    context={
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

    @property
    def parent(self):  # pragma: no cover
        """Return the parent object."""
        return self._parent() if self._parent is not None else None

    @property
    def parents(self):
        """Return list of parents."""
        return [self.parent]


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
        for path in dir_path.rglob('*'):
            if path.name == '.gitignore':
                continue
            members.append(
                Entity(
                    commit=self.commit,
                    client=self.client,
                    submodules=self.submodules,
                    path=str(path.relative_to(self.client.path)),
                    parent=self,
                )
            )
        return members


@jsonld.s(
    type=[
        'wfdesc:Process',
        'prov:Entity',
        'prov:Plan',
    ],
    context={
        'wfdesc': 'http://purl.org/wf4ever/wfdesc#',
        'prov': 'http://www.w3.org/ns/prov#',
    }
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
    }
)
class Workflow(Process):
    """Represent workflow with subprocesses."""

    subprocesses = jsonld.ib(context='wfdesc:hasSubProcess', kw_only=True)

    @subprocesses.default
    def default_subprocesses(self):
        """Load subprocesses."""
        return [
            subprocess.association.plan
            for _, subprocess in self.activity.subprocesses.values()
        ]
