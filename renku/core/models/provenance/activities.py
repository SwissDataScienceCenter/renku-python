# -*- coding: utf-8 -*-
#
# Copyright 2018-2020 - Swiss Data Science Center (SDSC)
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
"""Represent a Git commit."""

import os
import urllib
import uuid
from collections import OrderedDict
from pathlib import Path, posixpath

import attr
from git import NULL_TREE

from renku.core.models import jsonld
from renku.core.models.cwl import WORKFLOW_STEP_RUN_TYPES
from renku.core.models.cwl.annotation import Annotation
from renku.core.models.cwl.ascwl import CWLClass
from renku.core.models.cwl.types import PATH_OBJECTS
from renku.core.models.entities import Collection, CommitMixin, Entity
from renku.core.models.provenance.processes import Process, Workflow
from renku.core.models.refs import LinkReference

from .agents import Person, renku_agent
from .qualified import Association, Generation, Usage


def _nodes(output, parent=None):
    """Yield nodes from entities."""
    # NOTE refactor so all outputs behave the same
    entity = getattr(output, 'entity', output)

    if isinstance(entity, Collection):
        for member in entity.members:
            if parent is not None:
                member = attr.evolve(member, parent=parent)
            yield from _nodes(member)
        yield output
    else:
        yield output


@jsonld.s(
    type='prov:Activity',
    context={
        'prov': 'http://www.w3.org/ns/prov#',
        'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
        'schema': 'http://schema.org/'
    },
    cmp=False,
)
class Activity(CommitMixin):
    """Represent an activity in the repository."""

    _id = jsonld.ib(default=None, context='@id', kw_only=True)
    _message = jsonld.ib(context='rdfs:comment', kw_only=True)
    _was_informed_by = jsonld.ib(
        context='prov:wasInformedBy',
        kw_only=True,
    )

    part_of = attr.ib(default=None, kw_only=True)

    process = attr.ib(default=None, kw_only=True)
    outputs = attr.ib(kw_only=True)

    _collections = attr.ib(
        default=attr.Factory(OrderedDict), init=False, kw_only=True
    )
    generated = jsonld.container.list(
        Generation, context={
            '@reverse': 'prov:activity',
        }, kw_only=True
    )

    invalidated = jsonld.container.list(
        Entity, context={
            '@reverse': 'prov:wasInvalidatedBy',
        }, kw_only=True
    )

    influenced = jsonld.ib(
        context='prov:influenced',
        kw_only=True,
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

    agent = jsonld.ib(
        context='prov:agent',
        kw_only=True,
        default=renku_agent,
        type='renku.core.models.provenance.agents.SoftwareAgent'
    )
    person_agent = jsonld.ib(
        context='prov:agent',
        kw_only=True,
        type='renku.core.models.provenance.agents.Person'
    )

    def _get_activity_entity(self, path, deleted=False):
        """Gets the entity associated with this Activity and path."""
        client, commit, path = self.client.resolve_in_submodules(
            self.commit,
            path,
        )
        output_path = client.path / path
        parents = list(output_path.relative_to(client.path).parents)

        collection = None
        members = []
        for parent in reversed(parents[:-1]):
            if str(parent) in self._collections:
                collection = self._collections[str(parent)]
            else:
                collection = Collection(
                    client=client,
                    commit=commit,
                    path=str(parent),
                    members=[],
                    parent=collection,
                )
                members.append(collection)
                self._collections[str(parent)] = collection

            members = collection.members

        entity_cls = Entity
        if (self.client.path / path).is_dir():
            entity_cls = Collection

        # TODO: use a factory method to generate the entity
        if str(path).startswith(
            os.path.join(client.renku_home, client.DATASETS)
        ) and not deleted:
            entity = client.load_dataset_from_path(path, commit=commit)
        else:
            entity = entity_cls(
                commit=commit,
                client=client,
                path=str(path),
                parent=collection,
            )

        if collection:
            collection.members.append(entity)

        return entity

    def default_invalidated(self):
        """Entities invalidated by this Action."""
        results = []
        for path in self.removed_paths:
            entity = self._get_activity_entity(path, deleted=True)

            results.append(entity)
        return results

    def default_generated(self):
        """Entities generated by this Action."""
        results = []
        for path, role in self.outputs.items():
            entity = self._get_activity_entity(path)

            results.append(
                Generation(
                    activity=self,
                    entity=entity,
                    role=role,
                )
            )
        return results

    @influenced.default
    def default_influenced(self):
        """Calculate default values."""
        return list(self._collections.values())

    @property
    def parents(self):
        """Return parent commits."""
        return list(self.commit.parents)

    @property
    def removed_paths(self):
        """Return all paths removed in the commit."""
        index = set()

        for file_ in self.commit.diff(self.commit.parents or NULL_TREE):
            # only process deleted files (note they appear as ADDED)
            # in this backwards diff
            if file_.change_type != 'A':
                continue
            path_ = Path(file_.a_path)

            index.add(str(path_))

        return index

    @property
    def paths(self):
        """Return all paths in the commit."""
        index = set()

        for file_ in self.commit.diff(self.commit.parents or NULL_TREE):
            # ignore deleted files (note they appear as ADDED)
            # in this backwards diff
            if file_.change_type == 'A':
                continue
            path_ = Path(file_.a_path)

            is_dataset = self.client.DATASETS in str(path_)
            not_refs = LinkReference.REFS not in str(path_)
            does_not_exists = not (
                path_.exists() or
                (path_.is_symlink() and os.path.lexists(path_))
            )

            if all([is_dataset, not_refs, does_not_exists]):
                uid = uuid.UUID(path_.parent.name)
                path_ = (
                    Path(self.client.renku_home) / self.client.DATASETS /
                    str(uid) / self.client.METADATA
                )

            index.add(str(path_))

        return index

    @classmethod
    def generate_id(cls, commit):
        """Calculate action ID."""
        host = 'localhost'
        if hasattr(cls, 'client'):
            host = cls.client.remote.get('host') or host
        host = os.environ.get('RENKU_DOMAIN') or 'localhost'

        # always set the id by the identifier
        return urllib.parse.urljoin(
            'https://{host}'.format(host=host),
            posixpath.join(
                '/activities', 'commit/{commit.hexsha}'.format(commit=commit)
            )
        )

    def default_id(self):
        """Configure calculated ID."""
        return self.generate_id(self.commit)

    @_message.default
    def default_message(self):
        """Generate a default message."""
        return self.commit.message

    @_was_informed_by.default
    def default_was_informed_by(self):
        """List parent actions."""
        return [{
            '@id': self.generate_id(parent),
        } for parent in self.commit.parents]

    @outputs.default
    def default_outputs(self):
        """Guess default outputs from a commit."""
        return {path: None for path in self.paths}

    @started_at_time.default
    def default_started_at_time(self):
        """Configure calculated properties."""
        return self.commit.authored_datetime.isoformat()

    @ended_at_time.default
    def default_ended_at_time(self):
        """Configure calculated properties."""
        return self.commit.committed_datetime.isoformat()

    @person_agent.default
    def default_person_agent(self):
        """Set person agent to be the author of the commit."""
        if self.commit:
            return Person.from_commit(self.commit)
        return None

    @property
    def nodes(self):
        """Return topologically sorted nodes."""
        collections = OrderedDict()

        def _parents(node):
            if node.parent:
                yield from _parents(node.parent)
                yield node.parent

        for output in self.generated:
            for parent in _parents(output.entity):
                collections[parent.path] = parent

            yield from _nodes(output)

        for removed in self.invalidated:
            for parent in _parents(removed):
                collections[parent.path] = parent

            yield from _nodes(removed)

        yield from reversed(collections.values())

    def __attrs_post_init__(self):
        """Sets ``generated`` and ``invalidated`` default values if needed."""
        super().__attrs_post_init__()
        if not self._id:
            self._id = self.default_id()
        if not self.outputs:
            self.outputs = self.default_outputs()
        if not self.generated:
            self.generated = self.default_generated()
        if not self.invalidated:
            self.invalidated = self.default_invalidated()


@jsonld.s(
    type='wfprov:ProcessRun',
    context={
        'wfprov': 'http://purl.org/wf4ever/wfprov#',
        'oa': 'http://www.w3.org/ns/oa#',
    },
    cmp=False,
)
class ProcessRun(Activity):
    """A process run is a particular execution of a Process description."""

    __association_cls__ = Process

    inputs = attr.ib(default=None, kw_only=True)
    outputs = attr.ib(default=None, kw_only=True)

    generated = jsonld.container.list(
        Generation,
        context={
            '@reverse': 'prov:activity',
        },
        kw_only=True,
        default=None
    )

    association = jsonld.ib(
        context='prov:qualifiedAssociation',
        default=None,
        kw_only=True,
        type=Association
    )

    annotations = jsonld.container.list(
        context={
            '@reverse': 'oa:hasTarget',
        }, kw_only=True, type=Annotation
    )

    qualified_usage = jsonld.ib(
        default=None, context='prov:qualifiedUsage', kw_only=True, type=Usage
    )

    def __attrs_post_init__(self):
        """Calculate properties."""
        super().__attrs_post_init__()

        if not self.inputs:
            self.inputs = self.default_inputs()

        if not self.qualified_usage:
            self.qualified_usage = self.default_qualified_usage()

        if self.association is None:
            self.association = Association.from_activity(self)

        if not self.annotations:
            if (
                hasattr(self.process, 'annotations') and
                self.process.annotations
            ):
                self.annotations = self.process.annotations

            self.annotations.extend(self.plugin_annotations())

        if self.path is None:
            # FIXME only works for linking directory to file
            existing_outputs = set(self.outputs.values())
            for output_id, output_path in self.iter_output_files():
                if output_id not in existing_outputs:
                    self.outputs[os.path.join(
                        next(
                            path for path, usage in self.inputs.items()
                            if usage.role == 'input_directory'
                        ), output_path
                    )] = output_id
                    break

    def plugin_annotations(self):
        """Adds ``Annotation``s from plugins to a ``ProcessRun``."""
        from renku.core.plugins.pluginmanager import get_plugin_manager
        pm = get_plugin_manager()

        results = pm.hook.process_run_annotations(run=self)
        return [a for r in results for a in r]

    def default_inputs(self):
        """Guess default inputs from a process."""
        inputs = {}
        basedir = os.path.dirname(self.path)

        commit = self.commit
        client = self.client
        process = self.process

        revision = '{0}^'.format(commit)

        for input_id, input_path in process.iter_input_files(basedir):
            try:
                usage_id = self._id + '/inputs/' + input_id
                dependency = Usage.from_revision(
                    client=client,
                    path=input_path,
                    role=input_id,
                    revision=revision,
                    id=usage_id,
                )
                inputs[input_path] = dependency
            except KeyError:
                continue

        return inputs

    def default_qualified_usage(self):
        """Generate list of used artifacts."""
        return list(self.inputs.values())

    def iter_output_files(self, commit=None):
        """Yield tuples with output id and path."""
        process = self.process

        for output in process.outputs:
            if output.type in {'stdout', 'stderr'}:
                stream = getattr(process, output.type)
                if stream:
                    yield output.id, stream
            elif output.type in PATH_OBJECTS:
                glob = output.outputBinding.glob
                # TODO better support for Expression
                if glob.startswith('$(inputs.'):
                    input_id = glob[len('$(inputs.'):-1]
                    for input_ in process.inputs:
                        if input_.id == input_id:
                            yield output.id, input_.default
                            break  # out from process.inputs
                else:
                    yield output.id, glob

    def default_outputs(self):
        """Guess default outputs from a process."""
        if self.path is None:
            return {}
        return {
            output_path: output_id
            for output_id, output_path in self.iter_output_files()
        }

    @property
    def parents(self):
        """Return parent commits."""
        return [
            member.commit for usage in self.qualified_usage
            for member in usage.entity.entities
        ] + super().parents

    @property
    def nodes(self):
        """Return topologically sorted nodes."""
        # Outputs go first
        yield from super().nodes

        # Activity itself
        yield self.association.plan


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

    children = attr.ib(kw_only=True)

    _processes = jsonld.ib(
        context={
            '@reverse': 'wfprov:wasPartOfWorkflowRun',
        },
        default=attr.Factory(list),
        kw_only=True,
        type=Process
    )
    subprocesses = attr.ib(default=None, kw_only=True)

    outputs = attr.ib(default=None, kw_only=True)

    generated = jsonld.container.list(
        Generation, context={
            '@reverse': 'prov:activity',
        }, kw_only=True
    )

    @children.default
    def default_children(self):
        """Load children from process."""
        basedir = os.path.dirname(self.path) if self.path is not None else None

        def _load(step):
            """Load step definition."""
            if isinstance(step.run, WORKFLOW_STEP_RUN_TYPES):
                return step.run

            if self.commit:
                import yaml
                data = (self.commit.tree / basedir /
                        step.run).data_stream.read()
                return CWLClass.from_cwl(yaml.safe_load(data))

            return CWLClass.from_yaml(step.run)

        return {step.id: _load(step) for step in self.process.steps}

    def default_subprocesses(self):
        """Load subprocesses."""
        basedir = os.path.dirname(self.path)
        revision = '{0}^'.format(self.commit)

        ins = {
            dependency.role: dependency
            for path, dependency in self.inputs.items()
            if isinstance(dependency, Usage)
        }

        entities = {}
        outs = {}
        subprocesses = {}

        for step in reversed(self.process.topological_steps):
            if isinstance(step.run, WORKFLOW_STEP_RUN_TYPES):
                path = None
                process = step.run
            else:
                path = os.path.join(basedir, step.run)
                process = self.children[step.id]

            subprocess_id = self._id + '/steps/' + step.id

            inputs = {}
            for alias, source in step.in_.items():
                usage_id = subprocess_id + '/inputs/' + alias
                if source in ins:
                    dependency = ins[source]
                    inputs[dependency.path] = attr.evolve(
                        dependency,
                        role=alias,
                        id=usage_id,
                    )
                elif source in outs:
                    input_path = outs[source]
                    inputs[input_path] = Usage(
                        entity=entities[input_path],
                        role=alias,
                        id=usage_id,
                    )
                else:
                    # TODO check that it is not Path or Directory
                    pass

            subprocess_entity_commit = self.client.find_previous_commit(
                path, revision=revision
            )
            subprocess = process.create_run(
                commit=self.commit,
                client=self.client,
                part_of=self,
                process=process,
                path=path,
                inputs=inputs,
                id=subprocess_id,
            )

            subprocess.association = Association.from_activity(
                subprocess,
                commit=subprocess_entity_commit,
            )

            for output_path, source in subprocess.outputs.items():
                outs.setdefault(step.id + '/' + source, output_path)

            for generation in subprocess.generated:
                entity = generation.entity
                entities[entity.path] = entity

                if isinstance(entity, Collection):
                    entities.update(
                        **{member.path: member
                           for member in entity.members}
                    )

            subprocesses[step.id] = subprocess
            self._processes.append(subprocess)

        return subprocesses

    def iter_output_files(self, commit=None):
        """Yield tuples with output id and path."""
        commit = commit or self.commit

        tools = self.default_children()
        setattr(self, 'children', tools)

        for output in self.process.outputs:
            if output.type not in PATH_OBJECTS:
                continue

            if output.outputSource:
                step_id, _, source = output.outputSource.partition('/')
                subprocess = self.subprocesses[step_id]
                for glob, output_id in subprocess.outputs.items():
                    if output.id == output_id:
                        yield output.id, glob
                        break
            elif output.outputBinding:
                glob = output.outputBinding.glob
                # TODO better support for Expression
                if glob.startswith('$(inputs.'):
                    input_id = glob[len('$(inputs.'):-1]
                    for input_ in self.inputs:
                        if input_.id == input_id:
                            yield output.id, input_.default
                else:
                    yield output.id, glob

    def default_generated(self):
        """Calculate default values."""
        results = []
        for output in self.process.outputs:
            step_id, _, source = output.outputSource.partition('/')
            assert step_id in self.children

            for generated in self.subprocesses[step_id].generated:
                if generated.role == source:
                    results.append(
                        attr.evolve(
                            generated,
                            role=output.id,
                            activity=self,
                        )
                    )
                    break
            else:
                raise KeyError(output)

        return results

    @property
    def nodes(self):
        """Yield all graph nodes."""
        for subprocess in reversed(self._processes):
            if subprocess.path is None:
                # skip nodes connecting directory to file
                continue
            yield from subprocess.nodes

    def __attrs_post_init__(self):
        """Attrs post initializations."""
        if not self._id:
            self._id = self.default_id()
        if not self.inputs:
            self.inputs = self.default_inputs()
        if not self.subprocesses:
            self.subprocesses = self.default_subprocesses()
        if not self.generated:
            self.generated = self.default_generated()

        super().__attrs_post_init__()
