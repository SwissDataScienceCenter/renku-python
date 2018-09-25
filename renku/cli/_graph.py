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
from collections import OrderedDict, defaultdict, deque

import attr

from renku import errors
from renku._compat import Path
from renku.models.cwl.parameter import InputParameter, WorkflowOutputParameter
from renku.models.cwl.types import PATH_TYPES
from renku.models.cwl.workflow import Workflow
from renku.models.provenance import Activity, ProcessRun, Usage


def _safe_path(filepath, can_be_cwl=False):
    """Check if the path should be used in output."""
    # Should not be in ignore paths.
    if filepath in {'.gitignore', '.gitattributes'}:
        return False

    # Ignore everything in .renku ...
    if filepath.startswith('.renku'):
        # ... unless it can be a CWL.
        if can_be_cwl and filepath.endswith('.cwl'):
            return True
        return False

    return True


@attr.s(cmp=False)
class Graph(object):
    """Represent the provenance graph."""

    client = attr.ib()
    commits = attr.ib(default=attr.Factory(dict))

    _sorted_commits = attr.ib(default=attr.Factory(list))
    _nodes = attr.ib()
    _latest_commits = attr.ib(default=attr.Factory(dict))
    _need_update = attr.ib(default=None)

    cwl_prefix = attr.ib(init=False)

    def __attrs_post_init__(self):
        """Derive basic informations."""
        self.cwl_prefix = self.client.cwl_prefix

    @_nodes.default
    def default_nodes(self):
        """Build node index."""
        _nodes = OrderedDict()
        _need_update = self._need_update = {}
        self._latest_commits = _latest_commits = {}

        def _update_node(node, need_update, store_node=True):
            path = node.path
            latest = _latest_commits.get(path)

            if latest is None:
                latest = Usage.from_revision(
                    node.client,
                    path=node.path,
                    # revision='{0}'.format(key[0]),
                ).commit
                _latest_commits[path] = latest

            data = {}
            _need_update[node._id] = list(need_update)
            if latest and latest != node.commit:
                data['latest'] = latest
                _need_update[node._id].append(node)

            if store_node:
                _nodes[node] = data

            return _need_update[node._id]

        for commit in reversed(self._sorted_commits):
            activity = self.commits[commit]

            need_update = []
            for path, dependency in getattr(activity, 'inputs', {}).items():
                need_update.extend(
                    _need_update.get(
                        dependency._id,
                        _update_node(dependency, [], store_node=False),
                    )
                )

            for node in reversed(list(activity.nodes)):
                _update_node(node, need_update)

        return _nodes

    def need_update(self, node):
        """Return out-dated nodes."""
        try:
            return self._need_update[node._id]
        except KeyError:
            node = self.commits[node.commit]
            return self._need_update[node._id]

    @property
    def nodes(self):
        """Return topologically sorted nodes."""
        return reversed(self._nodes.keys())

    def normalize_path(self, path):
        """Normalize path relative to the Git workdir."""
        start = self.client.path.resolve()
        path = Path(path).resolve()
        return os.path.relpath(str(path), start=str(start))

    def _format_path(self, path):
        """Return a relative path based on the client configuration."""
        return os.path.relpath(str(self.client.path / path))

    def dependencies(self, revision='HEAD', can_be_cwl=False, paths=None):
        """Return dependencies from a revision or paths."""
        if paths:
            return {
                Usage.from_revision(self.client, path=path, revision=revision)
                for path in paths if _safe_path(path, can_be_cwl=can_be_cwl)
            }

        if revision == 'HEAD':
            index = self.client.git.index
        else:
            from git import IndexFile
            index = IndexFile.from_tree(self.client.git, revision)

        return {
            Usage.from_revision(
                client=self.client,
                path=path,
                revision=revision,
            )
            for path, _ in index.entries.keys()
            if _safe_path(path, can_be_cwl=can_be_cwl)
        }

    def process_dependencies(self, dependencies):
        """Process given dependencies."""
        visited = set()
        queue = deque(dependencies)

        while queue:
            processing = queue.popleft()

            if processing.commit in visited:
                continue

            # Mark as visited:
            visited.add(processing.commit)

            # Do the node processing here:
            activity = Activity.from_git_commit(
                processing.commit,
                client=processing.client,
                submodules=processing.submodules,
            )

            self.commits[activity.commit] = activity

            # Iterate over parents.
            for input_ in getattr(activity, 'inputs', {}).values():
                if input_.commit not in visited:
                    queue.append(input_)

        from renku.models._sort import topological
        self._sorted_commits = list(
            topological({
                activity.commit: [
                    input_.commit
                    for input_ in getattr(activity, 'inputs', {}).values()
                ]
                for activity in self.commits.values()
            })
        )
        self._nodes = self.default_nodes()

    def build(
        self, revision='HEAD', paths=None, dependencies=None, can_be_cwl=False
    ):
        """Build graph from paths and/or revision."""
        if dependencies is None:
            dependencies = self.dependencies(
                revision=revision, can_be_cwl=can_be_cwl, paths=paths
            )

        self.process_dependencies(dependencies)

        return {dependency for dependency in dependencies}

    @property
    def output_paths(self):
        """Return all output paths."""
        paths = set()
        for activity in self.commits.values():
            if activity.path:
                paths |= set(activity.outputs.keys())
        return paths

    def build_status(self, revision='HEAD', can_be_cwl=False):
        """Return files from the revision grouped by their status."""
        status = {
            'up-to-date': {},
            'outdated': {},
            'multiple-versions': {},
            'deleted': {},
        }

        dependencies = self.dependencies(
            revision=revision, can_be_cwl=can_be_cwl
        )
        current_files = self.build(dependencies=dependencies)

        # First find all up-to-date nodes.
        up_to_date = {
            node.path: node.commit
            for node, data in self._nodes.items()
            if not self.need_update(node)
        }

        for node in current_files:
            if node.path in up_to_date:  # trick the workflow step
                # FIXME use the latest commit
                status['up-to-date'][node.path] = up_to_date[node.path]
            else:
                try:
                    need_update = self.need_update(node)
                    status['outdated'][node.path] = [need_update]
                except KeyError:
                    pass

        # Merge all versions of used inputs in outdated file.
        multiple_versions = defaultdict(set)

        for need_updates in status['outdated'].values():
            for need_update in need_updates:
                for node in need_update:
                    multiple_versions[node.path].add(node)

        for node in current_files:
            if node.path in multiple_versions:
                multiple_versions[node.path].add(node)

        status['multiple-versions'] = {
            key: value
            for key, value in multiple_versions.items() if len(value) > 1
        }

        # Build a list of used files that have been deleted.
        current_paths = {node.path for node in current_files}
        status['deleted'] = {
            node.path: node
            for node in self.nodes
            if _safe_path(node.path, can_be_cwl=can_be_cwl) and
            node.path not in current_paths and
            not ((self.client.path / node.path).exists() or
                 (self.client.path / node.path).is_dir())
        }
        return status

    def siblings(self, node):
        """Return siblings for a given node.

        The key is part of the result set, hence to check if the node has
        siblings you should check the lenght is greater than 1.
        """
        from renku.models.provenance import Generation, Usage

        parent = None

        if isinstance(node, Generation):
            parent = node.activity
        elif isinstance(node, Usage):
            parent = self.commits[node.commit]

        if parent is None or not isinstance(parent, ProcessRun):
            raise errors.InvalidOutputPath(
                'The file "{0}" was not created by a renku command. \n\n'
                'Check the file history using: git log --follow "{0}"'.format(
                    node.path
                )
            )

        return set(parent.generated)

    def ascwl(self, input_paths=None, output_paths=None, outputs=None):
        """Serialize graph to CWL workflow.

        :param global_step_outputs: Make all step outputs global.
        """
        if output_paths is None:
            output_paths = {
                node.path
                for node in outputs if _safe_path(node.path)
            }

        workflow = Workflow()

        processes = set()
        stack = []

        output_keys = {(node.commit, node.path) for node in outputs}
        nodes = {(node.commit, node.path): node for node in self.nodes}

        for node in self.nodes:
            if (node.commit, node.path) not in output_keys:
                continue

            process_run = None
            if isinstance(node, ProcessRun):
                process_run = node
            elif isinstance(node.activity, ProcessRun):
                process_run = node.activity

            if process_run and process_run not in processes:
                stack.append(process_run)
                processes.add(process_run)

        while stack:
            action = stack.pop()

            if not hasattr(action, 'inputs'):
                continue

            for path, dependency in action.inputs.items():
                # Do not follow defined input paths.
                if input_paths and path in input_paths:
                    continue

                try:
                    process_run = nodes[(dependency.commit,
                                         dependency.path)].activity
                except AttributeError:
                    continue

                # Skip existing commits
                if isinstance(process_run, ProcessRun) and \
                        process_run not in processes:
                    stack.append(process_run)
                    processes.add(process_run)

        steps = {
            tool: 'step_{0}'.format(tool_index)
            for tool_index, tool in enumerate(processes, 1)
        }

        def _source_name(commit, path):
            """Find source name for a node."""
            try:
                process_run = nodes[(commit, path)].activity
                output_id = process_run.outputs[path]
                return '{0}/{1}'.format(steps[process_run], output_id)
            except (KeyError, AttributeError):
                pass

        def _relative_default(client, default):
            """Evolve ``File`` or ``Directory`` path."""
            if isinstance(default, PATH_TYPES):
                path = (client.workflow_path / default.path).resolve()
                return attr.evolve(default, path=path)
            return default

        input_index = 1

        for action, step_id in steps.items():
            tool = action.process

            ins = {}
            for path, dependency in action.inputs.items():
                alias = _source_name(dependency.commit, path)
                if alias:
                    ins[dependency.id] = alias

            outs = list(set(action.outputs.values()))

            for input_ in tool.inputs:
                input_mapping = ins.get(input_.id)
                if input_mapping is None:
                    input_id = 'input_{0}'.format(input_index)
                    workflow.inputs.append(
                        InputParameter(
                            id=input_id,
                            type=input_.type,
                            default=_relative_default(
                                self.client, input_.default
                            ),
                        )
                    )
                    input_index += 1
                    ins[input_.id] = input_id

            workflow.add_step(
                run=self.client.path / action.path,
                id=step_id,
                in_=ins,
                out=outs,
            )

        for index, node in enumerate(
            (node for node in outputs if node.path in output_paths)
        ):
            commit, path = node.commit, node.path
            id_ = 'output_{0}'.format(index)
            process_run = nodes[(commit, path)].activity
            output_id = process_run.outputs[path]
            type_ = next(
                output for output in process_run.process.outputs
                if output.id == output_id
            ).type
            type_ = type_ if type_ == 'Directory' else 'File'
            output_source = _source_name(commit, path)

            workflow.outputs.append(
                WorkflowOutputParameter(
                    id=id_,
                    type=type_,
                    outputSource=output_source,
                )
            )

        return workflow
