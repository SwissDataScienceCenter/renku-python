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
from functools import lru_cache

import attr

from renku import errors
from renku._compat import Path
from renku.models.cwl.parameter import InputParameter, WorkflowOutputParameter
from renku.models.cwl.types import PATH_TYPES
from renku.models.cwl.workflow import Workflow
from renku.models.provenance import Activity, Dependency


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

    cwl_prefix = attr.ib(init=False)

    def __attrs_post_init__(self):
        """Derive basic informations."""
        self.cwl_prefix = self.client.cwl_prefix

    @_nodes.default
    def default_nodes(self):
        """Build node index."""
        _nodes = OrderedDict()
        self._latest_commits = _latest_commits = {}
        for commit in reversed(self._sorted_commits):
            activity = self.commits[commit]
            activity_nodes = reversed(list(activity.nodes))

            need_update = []
            for path, dependency in getattr(activity, 'inputs', {}).items():
                try:
                    need_update.extend(
                        _nodes[(dependency.commit,
                                dependency.path)]['_need_update']
                    )
                except KeyError:
                    # Process a directory input
                    for (c, p), data in _nodes.items():
                        if c == commit and p.startswith(path):
                            need_update.extend(data['_need_update'])

            for key, data in activity_nodes:
                path = key[1]

                latest = _latest_commits.get(path)
                if latest is None:
                    latest = Dependency.from_revision(
                        activity.client,
                        path=data['path'],
                        # revision='{0}'.format(key[0]),
                    ).commit
                    _latest_commits[path] = latest

                data['_need_update'] = list(need_update)
                if latest and latest != data['commit']:
                    data['latest'] = latest
                    data['_need_update'].append(key)

                _nodes[key] = data

        return _nodes

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

    def _is_cwl(self, path):
        """Check if the path is a valid CWL file."""
        return path.startswith(self.cwl_prefix) and path.endswith('.cwl')

    @lru_cache(maxsize=1024)
    def find_cwl(self, commit):
        """Return a CWL."""
        cwl = None
        for file_ in commit.stats.files.keys():
            if self.client.is_cwl(file_):
                if cwl is not None:
                    raise ValueError(file_)  # duplicate
                cwl = file_
        return cwl

    def dependencies(self, revision='HEAD', can_be_cwl=False, paths=None):
        """Return dependencies from a revision or paths."""
        if paths:
            return [
                Dependency.from_revision(
                    self.client, path=path, revision=revision
                ) for path in paths
            ]

        if revision == 'HEAD':
            index = self.client.git.index
        else:
            from git import IndexFile
            index = IndexFile.from_tree(self.client.git, revision)

        return [
            Dependency.from_revision(
                client=self.client,
                path=path,
                revision=revision,
            ) for path, _ in index.entries.keys()
            if _safe_path(path, can_be_cwl=can_be_cwl)
        ]

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

        current_files = {(dependency.commit, dependency.path)
                         for dependency in dependencies if dependency.path}

        self.process_dependencies(dependencies)

        return current_files

    @property
    def output_paths(self):
        """Return all output paths."""
        paths = set()
        for activity in self.commits.values():
            if activity.process_path:
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
        current_files = set(self.build(dependencies=dependencies))

        # First find all up-to-date nodes.
        up_to_date = {
            filepath: commit
            for (commit, filepath), data in self._nodes.items()
            if not data.get('_need_update')
        }

        for commit, filepath in current_files:
            if filepath in up_to_date:  # trick the workflow step
                # FIXME use the latest commit
                status['up-to-date'][filepath] = up_to_date[filepath]
            else:
                try:
                    need_update = self._nodes[(commit,
                                               filepath)]['_need_update']
                    status['outdated'][filepath] = [need_update]
                except KeyError:
                    pass

        # Merge all versions of used inputs in outdated file.
        multiple_versions = defaultdict(set)

        for need_updates in status['outdated'].values():
            for need_update in need_updates:
                for commit, filepath in need_update:
                    multiple_versions[filepath].add((commit, filepath))

        for commit, filepath in current_files:
            if filepath in multiple_versions:
                multiple_versions[filepath].add((commit, filepath))

        status['multiple-versions'] = {
            key: value
            for key, value in multiple_versions.items() if len(value) > 1
        }

        # Build a list of used files that have been deleted.
        current_paths = {filepath for _, filepath in current_files}
        status['deleted'] = {
            filepath: (commit, filepath)
            for commit, filepath in self.nodes
            if _safe_path(filepath, can_be_cwl=can_be_cwl) and filepath not in
            current_paths and not ((self.client.path / filepath).exists() or
                                   (self.client.path / filepath).is_dir())
        }
        return status

    def siblings(self, key):
        """Return siblings for a given key.

        The key is part of the result set, hence to check if the node has
        siblings you should check the lenght is greater than 1.
        """
        commit, path = key
        activity = self.commits[commit]

        # TODO refactor to .renku/workflows/name#step_id
        tools = set()
        if activity.process_path:
            tools.add(activity.process_path)

            if hasattr(activity, 'children'):
                tools |= {
                    os.path.join(
                        os.path.dirname(activity.process_path),
                        step.run,
                    )
                    for step in activity.process.steps
                }

        if not activity.process_path or not (
            path in tools or path in activity.outputs
        ):
            raise errors.InvalidOutputPath(
                'The file "{0}" was not created by a renku command. \n\n'
                'Check the file history using: git log --follow "{0}"'.format(
                    key[1]
                )
            )

        elif path in tools:
            return {key}

        elif hasattr(activity, 'children'):
            output_id = activity.outputs[path]
            steps = {
                output.id: output.outputSource.split('/')[0]
                for output in activity.process.outputs
            }
            step_id = steps[output_id]

            return {(key[0], output_path)
                    for output_path, output_id in activity.outputs.items()
                    if steps[output_id] == step_id}

        return {(key[0], path) for path in activity.outputs.keys()}

    def ascwl(self, input_paths=None, output_paths=None, outputs=None):
        """Serialize graph to CWL workflow.

        :param global_step_outputs: Make all step outputs global.
        """
        if output_paths is None:
            output_paths = {path for _, path in outputs if _safe_path(path)}

        workflow = Workflow()

        def find_process_run(commit, path):
            """Return a process run."""
            activity = self.commits[commit]
            parents = activity.pred(path)

            if not parents:
                return None

            assert len(parents) == 1

            if hasattr(activity, 'subprocesses'):
                return activity.subprocesses[parents[0][1]][1]
            return activity

        processes = set()
        stack = []

        for commit, path in outputs:
            process_run = find_process_run(commit, path)
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
                    process_run = find_process_run(dependency.commit, path)
                except Exception:
                    # import ipdb; ipdb.set_trace()
                    process_run = None

                # Skip existing commits
                if process_run and process_run not in processes:
                    stack.append(process_run)
                    processes.add(process_run)

        steps = {
            tool: 'step_{0}'.format(tool_index)
            for tool_index, tool in enumerate(processes, 1)
        }

        def _source_name(commit, path):
            """Find source name for a node."""
            try:
                process_run = find_process_run(commit, path)
                if not process_run:
                    return
                output_id = process_run.outputs[path]
                return '{0}/{1}'.format(steps[process_run], output_id)
            except KeyError:
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
                run=self.client.path / action.process_path,
                id=step_id,
                in_=ins,
                out=outs,
            )

        for index, key in enumerate(
            (key for key in outputs if key[1] in output_paths)
        ):
            commit, path = key
            id_ = 'output_{0}'.format(index)
            process_run = find_process_run(commit, path)
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
