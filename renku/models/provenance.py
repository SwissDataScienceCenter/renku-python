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
"""Graph builder."""

import os
import re
import weakref
from datetime import datetime

import attr
import yaml

from renku.api import LocalClient
from renku.models import _jsonld as jsonld
from renku.models._datastructures import DirectoryTree
from renku.models.cwl._ascwl import CWLClass


@jsonld.s(
    type=[
        'prov:Location',
        'foaf:Project',
    ],
    context={
        'foaf': 'http://xmlns.com/foaf/0.1/',
        'prov': 'http://purl.org/dc/terms/',
    },
    frozen=True,
    slots=True,
)
class Project(object):
    """Represent a project."""

    _id = jsonld.ib(context='@id', kw_only=True)


@attr.s(cmp=False)
class CommitMixin:
    """Represent a commit mixin."""

    commit = attr.ib(kw_only=True)
    client = attr.ib(kw_only=True)
    submodules = attr.ib(default=attr.Factory(list), kw_only=True)
    path = attr.ib(default=None, kw_only=True)

    _id = jsonld.ib(context='@id', init=False, kw_only=True)
    _label = jsonld.ib(context='rdfs:label', init=False, kw_only=True)
    _location = jsonld.ib(context='prov:atLocation', init=False, kw_only=True)

    @_id.default
    def default_id(self):
        """Configure calculated ID."""
        return '{self.commit.hexsha}/{self.path}'.format(self=self)

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
        'prov:Person',
        'foaf:Person',
    ],
    context={
        'foaf': 'http://xmlns.com/foaf/0.1/',
        'prov': 'http://purl.org/dc/terms/',
        'scoro': 'http://purl.org/spar/scoro/',
    },
    frozen=True,
    slots=True,
)
class Person(object):
    """Represent a person."""

    name = jsonld.ib(context='foaf:name')
    email = jsonld.ib(context='foaf:mbox')

    _id = jsonld.ib(context='@id', init=False)

    @_id.default
    def default_id(self):
        """Configure calculated ID."""
        return 'mailto:{self.email}'.format(self=self)

    @email.validator
    def check_email(self, attribute, value):
        """Check that the email is valid."""
        if not (
            isinstance(value, str) and re.match(r"[^@]+@[^@]+\.[^@]+", value)
        ):
            raise ValueError('Email address is invalid.')

    @classmethod
    def from_commit(cls, commit):
        """Create an instance from a Git commit."""
        return cls(
            name=commit.author.name,
            email=commit.author.email,
        )


@jsonld.s(
    type='prov:Association',
    context={
        'prov': 'http://www.w3.org/ns/prov#',
    },
)
class Association:
    """Assign responsibility to an agent for an activity."""

    plan = jsonld.ib(context='prov:hadPlan')
    agent = jsonld.ib(context='prov:agent', default=None)

    @classmethod
    def from_activity(cls, activity):
        """Create an instance from the activity."""
        return cls(
            plan=activity.__association_cls__(
                commit=activity.commit,
                client=activity.client,
                submodules=activity.submodules,
                path=activity.path,
                activity=activity,
            ),
            agent=Person.from_commit(activity.commit),
        )


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


class EntityProxyMixin:
    """Implement proxy to entity attribute."""

    def __getattribute__(self, name):
        """Proxy entity attributes."""
        entity = object.__getattribute__(self, 'entity')
        if name not in {'id', 'entity', '__class__'} and hasattr(entity, name):
            return getattr(self.entity, name)
        return object.__getattribute__(self, name)


@jsonld.s(
    type='prov:Usage',
    context={
        'prov': 'http://www.w3.org/ns/prov#',
    },
    cmp=False,
)
class Usage(EntityProxyMixin):
    """Represent a dependent path."""

    entity = jsonld.ib(context='prov:entity', kw_only=True)
    id = jsonld.ib(context='prov:hadRole', default=None, kw_only=True)

    @classmethod
    def from_revision(cls, client, path, revision='HEAD', **kwargs):
        """Return dependency from given path and revision."""
        id_ = kwargs.pop('id', None)
        return cls(
            entity=Entity(
                client=client,
                commit=client.find_previous_commit(path, revision=revision),
                path=path,
                **kwargs
            ),
            id=id_,
        )

    @property
    def parents(self):
        """Return parent nodes."""
        # TODO connect files to an input directory
        return []  # pragma: no cover


@jsonld.s(
    type='prov:Generation',
    context={
        'prov': 'http://www.w3.org/ns/prov#',
    },
    cmp=False,
)
class Generation(EntityProxyMixin):
    """Represent an act of generating a file."""

    entity = jsonld.ib(
        context={
            '@reverse': 'prov:qualifiedGeneration',
        },
    )
    id = jsonld.ib(context='prov:hadRole', default=None)

    _activity = attr.ib(
        default=None,
        kw_only=True,
        converter=lambda value: weakref.ref(value)
        if value is not None else None,
    )

    @property
    def activity(self):
        """Return the activity object."""
        return self._activity() if self._activity is not None else None

    @property
    def parents(self):
        """Return list of parents."""
        return [self.activity]


@jsonld.s(
    type='prov:Activity',
    context={
        'prov': 'http://www.w3.org/ns/prov#',
        'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
    },
    cmp=False,
)
class Activity(CommitMixin):
    """Represent an activity in the repository."""

    part_of = attr.ib(default=None, kw_only=True)

    process = attr.ib(default=None, kw_only=True)
    outputs = attr.ib(kw_only=True)

    generated = jsonld.ib(
        context={
            '@reverse': 'prov:activity',
        }, kw_only=True, hash=False
    )

    started_at_time = jsonld.ib(
        context={
            '@id': 'prov:startedAtTime',
            '@type': 'http://www.w3.org/2001/XMLSchema#dateTime',
        },
        kw_only=True,
    )

    ended_at_time = jsonld.ib(
        context={
            '@id': 'prov:endedAtTime',
            '@type': 'http://www.w3.org/2001/XMLSchema#dateTime',
        },
        kw_only=True,
    )

    _id = jsonld.ib(context='@id', kw_only=True)
    _label = jsonld.ib(context='rdfs:label', init=False, kw_only=True)

    @generated.default
    def default_generated(self):
        """Calculate default values."""
        return [
            Generation(
                activity=self,
                entity=Entity(
                    commit=self.commit,
                    client=self.client,
                    submodules=self.submodules,
                    path=path,
                    parent=self,
                ),
                id=id_,
            ) for path, id_ in self.outputs.items()
        ]

    @property
    def paths(self):
        """Return all paths in the commit."""
        return set(self.commit.stats.files.keys())

    @_id.default
    def default_id(self):
        """Configure calculated ID."""
        return '{self.commit.hexsha}#'.format(self=self)

    @_label.default
    def default_label(self):
        """Generate a default label."""
        return self.commit.message.split('\n')[0]

    @outputs.default
    def default_outputs(self):
        """Guess default outputs from a commit."""
        return {path: None for path in self.commit.stats.files.keys()}

    @started_at_time.default
    def default_started_at_time(self):
        """Configure calculated properties."""
        return datetime.fromtimestamp(self.commit.authored_date).isoformat()

    @ended_at_time.default
    def default_ended_at_time(self):
        """Configure calculated properties."""
        return datetime.fromtimestamp(self.commit.committed_date).isoformat()

    @property
    def nodes(self):
        """Return topologically sorted nodes."""
        return self.generated

    @property
    def parents(self):
        """Return a list of parents."""
        return []

    @staticmethod
    def from_git_commit(commit, client, path=None, submodules=None):
        """Populate information from the given Git commit."""
        return from_git_commit(
            commit, client, path=None, submodules=submodules
        )


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
    type='wfprov:ProcessRun',
    context={
        'wfprov': 'http://purl.org/wf4ever/wfprov#',
    },
    cmp=False,
)
class ProcessRun(Activity):
    """A process run is a particular execution of a Process description."""

    __association_cls__ = Process

    inputs = attr.ib(kw_only=True)
    outputs = attr.ib(kw_only=True)

    generated = jsonld.ib(
        context={
            '@reverse': 'prov:activity',
        },
        kw_only=True,
        hash=False,
    )

    association = jsonld.ib(
        context='prov:qualifiedAssociation',
        init=False,
        kw_only=True,
    )

    qualified_usage = jsonld.ib(context='prov:qualifiedUsage', kw_only=True)

    @generated.default
    def default_generated(self):
        """Calculate default values."""
        return [
            Generation(
                activity=self,
                entity=Entity(
                    commit=self.commit,
                    client=self.client,
                    submodules=self.submodules,
                    path=path,
                    parent=self,
                ),
                id=id_,
            ) for path, id_ in self.outputs.items()
        ]

    def __attrs_post_init__(self):
        """Calculate properties."""
        self.association = Association.from_activity(self)

    @inputs.default
    def default_inputs(self):
        """Guess default inputs from a process."""
        basedir = os.path.dirname(self.path)
        commit = self.commit
        client = self.client
        process = self.process
        hierarchy = self.submodules

        inputs = {}
        revision = '{0}^'.format(commit)

        try:
            from git import Submodule

            submodules = [
                submodule for submodule in Submodule.
                iter_items(client.git, parent_commit=commit)
            ]
        except (RuntimeError, ValueError):
            # There are no submodules assiciated with the given commit.
            submodules = []

        subclients = {
            submodule: LocalClient(
                path=(client.path / submodule.path).resolve(),
                parent=client,
            )
            for submodule in submodules
        }

        def resolve_submodules(file_, **kwargs):
            original_path = client.path / file_
            if original_path.is_symlink(
            ) or file_.startswith('.renku/vendors'):
                original_path = original_path.resolve()
                for submodule, subclient in subclients.items():
                    try:
                        subpath = original_path.relative_to(subclient.path)
                        return Usage.from_revision(
                            client=subclient,
                            path=str(subpath),
                            revision=submodule.hexsha,
                            submodules=hierarchy + [submodule.name],
                            **kwargs
                        )
                    except ValueError:
                        pass

        for input_id, input_path in process.iter_input_files(basedir):
            try:
                dependency = resolve_submodules(input_path, id=input_id)
                if dependency is None:
                    dependency = Usage.from_revision(
                        client=client,
                        path=input_path,
                        id=input_id,
                        revision=revision,
                    )
                inputs[input_path] = dependency
            except KeyError:
                continue

        return inputs

    @qualified_usage.default
    def default_qualified_usage(self):
        """Generate list of used artifacts."""
        return list(self.inputs.values())

    @outputs.default
    def default_outputs(self):
        """Guess default outputs from a process."""
        basedir = os.path.dirname(self.path)
        tree = DirectoryTree.from_list((
            path for path in super(ProcessRun, self).default_outputs()
            if not self.client.is_cwl(path)
        ))
        outputs = {}

        for output_id, output_path in self.process.iter_output_files(
            basedir, commit=self.commit
        ):
            outputs[output_path] = output_id

            # Expand directory entries.
            for subpath in tree.get(output_path, []):
                outputs.setdefault(
                    os.path.join(output_path, subpath), output_id
                )

        return outputs

    @property
    def nodes(self):
        """Return topologically sorted nodes."""
        yield from super(ProcessRun, self).nodes
        yield self
        for node in self.inputs.values():
            if (node.client.path / node.path).is_dir():
                yield node

    @property
    def parents(self):
        """Return a list of parents."""
        return self.qualified_usage


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


@jsonld.s(
    type='wfprov:WorkflowRun',
    context={
        'wfprov': 'http://purl.org/wf4ever/wfprov#',
    },
    cmp=False,
)
class WorkflowRun(ProcessRun):
    """A workflow run typically contains several subprocesses."""

    __association_cls__ = Workflow

    # @reverse wfprov:wasPartOfWorkflowRun

    children = attr.ib(init=False, kw_only=True)

    _processes = jsonld.ib(
        context={
            '@reverse': 'wfprov:wasPartOfWorkflowRun',
        },
        default=attr.Factory(list),
        kw_only=True,
    )
    subprocesses = attr.ib(init=False, kw_only=True)

    @children.default
    def default_children(self):
        """Load children from process."""
        return self.process._tools

    @subprocesses.default
    def default_subprocesses(self):
        """Load subprocesses."""
        basedir = os.path.dirname(self.path)
        revision = '{0}^'.format(self.commit)

        ins = {
            dependency.id: dependency
            for path, dependency in self.inputs.items()
        }
        entities = {
            generation.entity.path: generation.entity
            for generation in self.generated
        }
        outputs_ = {id_: path_ for path_, id_ in self.outputs.items()}
        outs = {
            output.outputSource: outputs_[output.id]
            for output in self.process.outputs
        }

        subprocesses = {}

        for step in reversed(self.process.topological_steps):
            path = os.path.join(basedir, step.run)
            process = self.children[step.id]

            inputs = {}
            for alias, source in step.in_.items():
                if source in ins:
                    dependency = ins[source]
                    inputs[dependency.path] = attr.evolve(
                        dependency,
                        id=alias  #
                    )
                elif source in outs:
                    input_path = outs[source]
                    inputs[input_path] = Usage(
                        entity=entities[input_path],
                        id=alias,
                    )
                else:
                    # TODO check that it is not Path or Directory
                    pass

            outputs = {}
            generated = []
            for source in step.out:
                output_source = step.id + '/' + source
                output_path = outs.get(output_source)
                if output_path:
                    outputs[output_path] = source
                    generated.append(
                        Generation(
                            activity=None,
                            entity=entities[output_path],
                            id=source,
                        )
                    )

            subprocess = process.create_run(
                commit=self.client.find_previous_commit(
                    path, revision=revision
                ),
                client=self.client,
                part_of=self,
                process=process,
                path=path,
                inputs=inputs,
                outputs=outputs,
                generated=generated,
                id=self._id + 'steps/' + step.id,
                submodules=self.submodules,
            )
            # FIXME refactor
            for generation in generated:
                generation._activity = weakref.ref(subprocess)

            subprocesses[path] = (step, subprocess)
            self._processes.append(subprocess)

        return subprocesses

    @property
    def nodes(self):
        """Yield all graph nodes."""
        for subprocess in reversed(self._processes):
            yield from subprocess.nodes


def from_git_commit(commit, client, path=None, submodules=None):
    """Populate information from the given Git commit."""
    cls = Activity
    process = None
    hierarchy = list(submodules) if submodules else []

    if path is None:
        for file_ in commit.stats.files.keys():
            # 1.a Find process (CommandLineTool or Workflow);
            if client.is_cwl(file_):
                if path is not None:
                    raise ValueError(file_)  # duplicate
                path = file_
                continue

    if path:
        data = (commit.tree / path).data_stream.read()
        process = CWLClass.from_cwl(yaml.load(data))

        return process.create_run(
            commit=commit,
            client=client,
            process=process,
            path=path,
            submodules=hierarchy,
        )

    return cls(commit=commit, client=client, submodules=hierarchy)
