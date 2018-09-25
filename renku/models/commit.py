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
from collections import deque

import attr
import yaml

from renku.api import LocalClient
from renku.models._datastructures import DirectoryTree
from renku.models.cwl._ascwl import CWLClass


@attr.s
class Dependency(object):
    """Represent a dependent path."""

    commit = attr.ib()
    client = attr.ib()

    path = attr.ib(default=None)
    id = attr.ib(default=None)

    submodules = attr.ib(default=attr.Factory(list))

    @classmethod
    def from_revision(cls, client, path, revision='HEAD', **kwargs):
        """Return dependency from given path and revision."""
        return cls(
            client=client,
            commit=client.find_previous_commit(path, revision=revision),
            path=path,
            **kwargs
        )


@attr.s
class Action(object):
    """Represent an action in the repository."""

    commit = attr.ib()
    client = attr.ib()

    process = attr.ib(default=None)
    process_path = attr.ib(default=None)
    inputs = attr.ib(default=attr.Factory(dict))
    outputs = attr.ib(default=attr.Factory(dict))

    parent = attr.ib(default=None)
    children = attr.ib(default=attr.Factory(list))

    submodules = attr.ib(default=attr.Factory(list))

    def change_key(self, dependency, path=None):
        """Rename the path part in the key."""
        path = path or dependency.path
        return str(dependency.commit), path

    def iter_nodes(self, expand_workflow=True):
        """Yield all graph nodes."""
        for path, _ in self.outputs.items():
            data = {
                'commit': self.commit,
                'path': path,
                'submodule': self.submodules,
                'client': self.client,
            }
            yield (str(self.commit), path), data

        if self.process_path:
            if expand_workflow and self.children:
                steps = {step.id: step for step in self.process.steps}
                basedir = os.path.dirname(self.process_path)
                default_data = {
                    'client': self.client,
                    'workflow': self.process,
                    'submodule': self.submodules,
                }
                revision = '{0}^'.format(self.commit)

                for step_id, subprocess in self.children.items():
                    step = steps[step_id]
                    path = os.path.join(basedir, step.run)
                    # The workflow path must be relative to the current tool
                    # since we might be inside a submodule.
                    workflow_path = os.path.relpath(
                        self.process_path, start=os.path.dirname(path)
                    )

                    data = {
                        'path': path,
                        'commit':
                            self.client.find_previous_commit(
                                path, revision=revision
                            ),
                        'tool': subprocess,
                        'workflow_path':
                            '{workflow_path}#steps/{step.id}'.format(
                                workflow_path=workflow_path, step=step
                            ),
                    }
                    data.update(**default_data)
                    yield (str(self.commit), path), data
            else:
                data = {
                    'client': self.client,
                    'commit': self.commit,
                    'path': self.process_path,
                    'tool': self.process,
                    'submodule': self.submodules,
                }
                # TODO submodule
                yield (str(self.commit), self.process_path), data

        for path, dependency in self.inputs.items():
            data = {
                'commit': dependency.commit,
                'path': dependency.path,
                'submodule': dependency.submodules,
                'client': dependency.client,
            }
            yield self.change_key(dependency, path=path), data

    def iter_edges(self, expand_workflow=True):
        """Yield all graph edges."""
        if expand_workflow and self.children:
            commit = self.commit
            workflow = self.process
            path = self.process_path
            basedir = os.path.dirname(self.process_path)

            # Keep track of node identifiers for steps, inputs and outputs:
            step_map = {}
            input_map = {
                dep.id: (str(dep.commit), path)
                for path, dep in self.inputs.items()
            }
            output_map = {}

            for step in workflow.steps:
                step_tool = self.children[step.id]
                tool_path = os.path.join(basedir, step.run)
                tool_key = (str(commit), tool_path)
                step_map[step.id] = tool_key

                for input_id, input_path in step_tool.iter_input_files(
                    basedir
                ):
                    if input_path in commit.stats.files:
                        #: Check intermediate committed files
                        input_key = (str(commit), input_path)
                        output_map[step.id + '/' + input_id] = input_key
                        #: Edge from an input to the tool.
                        yield input_key, tool_key, {'id': input_id}
                    else:
                        #: Global workflow input
                        source = step.in_[input_id]
                        yield input_map[source], tool_key, {'id': input_id}

                # Find ALL siblings that MUST be generated in the same commit.
                step_outputs = workflow._step_outputs.get(step.id)
                if step_outputs is not None:
                    step_outputs = step_outputs.items()
                else:
                    step_outputs = step_tool.iter_output_files(
                        basedir, commit=commit
                    )

                for output_id, output_path in step_outputs:
                    node_key = (str(commit), output_path)
                    yield tool_key, node_key, {'id': output_id}

            for step in workflow.steps:
                for alias, source in step.in_.items():
                    name = step.id + '/' + alias

                    if name in output_map and '/' in source:
                        other_step, id_ = source.split('/')
                        other_key = step_map[other_step]
                        yield other_key, output_map[name], {'id': id_}

        elif self.process:
            tool_key = (str(self.commit), self.process_path)
            for path, dependency in self.inputs.items():
                input_key = self.change_key(dependency, path=path)
                #: Edge from an input to the tool.
                yield input_key, tool_key, {'id': dependency.id}

            for path, output_id in self.outputs.items():
                node_key = (str(self.commit), path)
                #: Edge from the tool to an output.
                yield tool_key, node_key, {'id': output_id}

    @classmethod
    def from_git_commit(cls, commit, client, submodules=None):
        """Populate information from the given Git commit."""
        process = None
        process_path = None
        inputs = {}
        outputs = {}
        children = {}
        hierarchy = list(submodules) or []

        tree = DirectoryTree()

        for file_ in commit.stats.files.keys():
            # 1.a Find process (CommandLineTool or Workflow);
            if client.is_cwl(file_):
                if process_path is not None:
                    raise ValueError(file_)  # duplicate
                process_path = file_
                continue

            # Build tree index.
            tree.add(file_)

        if process_path:
            basedir = os.path.dirname(process_path)
            try:
                data = (commit.tree / process_path).data_stream.read()
                process = CWLClass.from_cwl(yaml.load(data))
            except KeyError:
                pass
        else:
            outputs = {path: None for path in tree}

        # 2. Map all outputs;
        if process:
            for output_id, output_path in process.iter_output_files(
                basedir, commit=commit
            ):
                outputs[output_path] = output_id

                # Expand directory entries.
                for subpath in tree.get(output_path, []):
                    outputs.setdefault(
                        os.path.join(output_path, subpath), output_id
                    )

            if process.__class__.__name__ == 'Workflow':
                children = process._tools

        # 3. Identify input files (filepath: (input_id, commit))
        if process and process_path:
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
                            return Dependency.from_revision(
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
                        dependency = Dependency.from_revision(
                            client=client,
                            path=input_path,
                            id=input_id,
                            revision=revision,
                        )
                    inputs[input_path] = dependency
                except KeyError:
                    continue

        return cls(
            commit=commit,
            client=client,
            process=process,
            process_path=process_path,
            inputs=inputs,
            outputs=outputs,
            children=children,
            submodules=hierarchy,
        )

    @classmethod
    def build_graph(cls, client, revision='HEAD', lookup=None, graph=None):
        """Build a graph for the whole repository."""
        graph = graph or {}
        if lookup is None:
            lookup = deque(
                Dependency(client=client, commit=commit)
                for commit in client.git.iter_commits(rev=revision)
            )
        else:
            lookup = deque(lookup)

        while lookup:
            dependency = lookup.popleft()
            if dependency.commit in graph:
                continue

            action = graph[dependency.commit] = cls.from_git_commit(
                dependency.commit,
                client=dependency.client,
                submodules=dependency.submodules,
            )
            lookup.extendleft(action.inputs.values())

        return graph

    @classmethod
    def dependencies(cls, client, revision='HEAD', can_be_cwl=False):
        """Build a status."""
        from renku.cli._graph import _safe_path

        if revision == 'HEAD':
            index = client.git.index
        else:
            from git import IndexFile
            index = IndexFile.from_tree(client.git, revision)

        return [
            Dependency.from_revision(
                client=client,
                path=path,
                revision=revision,
            ) for path, _ in index.entries.keys()
            if _safe_path(path, can_be_cwl=can_be_cwl)
        ]
