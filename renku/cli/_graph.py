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
from renku.models._git import Range
from renku.models.cwl.command_line_tool import CommandLineTool
from renku.models.cwl.parameter import InputParameter, WorkflowOutputParameter
from renku.models.cwl.types import PATH_TYPES
from renku.models.cwl.workflow import Workflow
from renku.models.provenance import Activity, Generation, ProcessRun, Usage
from renku.models.provenance.entities import Collection, Entity, Process

LINK_CWL = CommandLineTool(
    baseCommand=['true'],
    requirements=[
        {
            'class': 'InlineJavascriptRequirement',
        },
        {
            'class': 'InitialWorkDirRequirement',
            'listing': '$(inputs.input_directory.listing)',
        },
    ],
    inputs={
        'input_directory': 'Directory',
        'filename': 'string',
    },
    outputs={
        'output_file': {
            'type': 'File',
            'outputBinding': {
                'glob': '$(inputs.filename)',
                # .slice(inputs.input_directory.basename.length)
            },
        },
    },
)


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
    activities = attr.ib(default=attr.Factory(dict))
    generated = attr.ib(default=attr.Factory(dict))

    _sorted_commits = attr.ib(default=attr.Factory(list))
    _latest_commits = attr.ib(default=attr.Factory(dict))
    _nodes = attr.ib()
    _need_update = attr.ib(default=attr.Factory(dict))

    cwl_prefix = attr.ib(init=False)

    def __attrs_post_init__(self):
        """Derive basic informations."""
        self.cwl_prefix = self.client.cwl_prefix

    @_nodes.default
    def default_nodes(self):
        """Build node index."""
        self.generated = {}
        nodes = OrderedDict()

        for commit in reversed(self._sorted_commits):
            try:
                activity = self.activities[commit]

                # for node in reversed(list(activity.nodes)):
                #     key = (node.commit, node.path)
                #     if key in nodes:
                #         del nodes[key]
                #     nodes[key] = node

                nodes.update(((node.commit, node.path), node)
                             for node in reversed(list(activity.nodes)))

                if isinstance(activity, ProcessRun):
                    self.generated.update({
                        generation.entity._id: generation
                        for generation in activity.generated
                    })

            except KeyError:
                pass

        return nodes

    def need_update(self, node):
        """Return out-dated nodes."""
        if node is None:
            return

        skip = True
        if isinstance(node, ProcessRun):
            node = node.association.plan
            skip = False

        if node._id in self._need_update:
            return self._need_update[node._id]

        latest = self.latest(node)
        if latest:
            return self._need_update.setdefault(node._id, [node])

        need_update_ = []

        for parent in self.parents(node):
            # Skip Collections if it is not an input
            if skip and isinstance(parent, Collection):
                continue

            parent_updates = self.need_update(parent)
            if parent_updates:
                need_update_.extend(parent_updates)

        return self._need_update.setdefault(node._id, need_update_)

    def parents(self, node):
        """Return parents for a given node."""
        import warnings

        def _from_entity(entity, check_parents=True):
            """Find parent from entity."""
            try:
                return [self.generated[entity._id].activity]
            except KeyError:
                id_ = Path(entity._id)
                while check_parents and id_ != id_.parent:
                    try:
                        # TODO include selection step here
                        return [self.generated[str(id_)]]
                    except KeyError:
                        id_ = id_.parent
                return []

        if isinstance(node, Generation):
            result = [node.parent] if node.parent is not None else []
            if node.activity and isinstance(node.activity, ProcessRun):
                return result + [node.activity.association.plan]
            return result
        elif isinstance(node, Usage):
            return _from_entity(node.entity)
        elif isinstance(node, Entity):
            # Link files and directories and generations.
            return ([node.parent]
                    if node.parent is not None else []) + _from_entity(
                        node, check_parents=False
                    )
        elif isinstance(node, Process):
            # warnings.warn('Called on run {0}'.format(node), stacklevel=2)
            return self.parents(node.activity)
        elif isinstance(node, ProcessRun):
            return node.qualified_usage
        elif isinstance(node, Activity):
            warnings.warn('Called parents on {0}'.format(node), stacklevel=2)
            return []

        raise NotImplementedError(node)

    def latest(self, node):
        """Return a latest commit where the node was modified."""
        if node.path and node.path not in self._latest_commits:
            try:
                latest = Usage.from_revision(
                    node.client,
                    path=node.path,
                    # TODO support range queries
                    # revision='{0}'.format(node.commit.hexsha),
                ).commit
            except KeyError:
                latest = None

            self._latest_commits[node.path] = latest
        else:
            latest = self._latest_commits.get(node.path)

        if latest and latest != node.commit:
            return latest

    @property
    def nodes(self):
        """Return topologically sorted nodes."""
        return reversed(self._nodes.values())

    def normalize_path(self, path):
        """Normalize path relative to the Git workdir."""
        start = self.client.path.resolve()
        path = Path(path).resolve()
        return os.path.relpath(str(path), start=str(start))

    def _format_path(self, path):
        """Return a relative path based on the client configuration."""
        return os.path.relpath(str(self.client.path / path))

    def dependencies(self, revision='HEAD', paths=None):
        """Return dependencies from a revision or paths."""
        result = []

        if not paths:
            if revision == 'HEAD':
                index = self.client.repo.index
            else:
                from git import IndexFile
                index = IndexFile.from_tree(self.client.repo, revision)

            paths = (path for path, _ in index.entries.keys())

        for path in paths:
            try:
                result.append(
                    Usage.from_revision(
                        self.client,
                        path=self.normalize_path(path),
                        revision=revision,
                    )
                )
            except KeyError:
                continue

        return result

    def process_dependencies(self, dependencies, visited=None):
        """Process given dependencies."""
        for dependency in dependencies:
            # We can't simply reuse information from submodules
            if dependency.client != self.client:
                continue
            self._latest_commits[dependency.path] = dependency.commit

        visited = visited or set()
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
            )

            if activity is None:
                continue

            self.activities[activity.commit] = activity

            # Iterate over parents.
            if isinstance(activity, ProcessRun):
                for entity in activity.qualified_usage:
                    for member in entity.entities:
                        if member.commit not in visited:
                            queue.append(member)

        from renku.models._sort import topological
        self._sorted_commits = topological({
            commit: activity.parents
            for commit, activity in self.activities.items()
        })
        self._nodes = self.default_nodes()

    def build(
        self, revision='HEAD', paths=None, dependencies=None, can_be_cwl=False
    ):
        """Build graph from paths and/or revision."""
        interval = Range.rev_parse(self.client.repo, revision)

        if dependencies is None:
            dependencies = self.dependencies(revision=revision, paths=paths)

        ignore = {
            commit
            for commit in self.client.repo.iter_commits(interval.start)
        } if interval.start else set()

        self.process_dependencies(dependencies, visited=ignore)

        return {
            self._nodes.get((dependency.commit, dependency.path), dependency)
            for dependency in dependencies
            if _safe_path(dependency.path, can_be_cwl=can_be_cwl)
        }

    @property
    def output_paths(self):
        """Return all output paths."""
        paths = set()
        for activity in self.activities.values():
            if isinstance(activity, ProcessRun):
                paths |= {path for path in activity.outputs.keys() if path}
        return paths

    def build_status(self, revision='HEAD', can_be_cwl=False):
        """Return files from the revision grouped by their status."""
        status = {
            'up-to-date': {},
            'outdated': {},
            'multiple-versions': {},
            'deleted': {},
        }

        dependencies = self.dependencies(revision=revision)
        current_files = self.build(
            dependencies=dependencies,
            can_be_cwl=can_be_cwl,
        )

        # TODO check only outputs
        paths = {}
        for commit in reversed(self._sorted_commits):
            activity = self.activities.get(commit)

            if isinstance(activity, ProcessRun):
                nodes = activity.nodes if can_be_cwl else activity.generated

                for node in nodes:
                    paths[node.path] = node

        # First find all up-to-date nodes.
        for node in paths.values():
            # for node in current_files:
            need_update = [
                dependency for dependency in self.need_update(node)
                if dependency.path != node.path
            ]

            if need_update:
                status['outdated'][node.path] = need_update
            else:
                status['up-to-date'][node.path] = node.commit

        # Merge all versions of used inputs in outdated file.
        multiple_versions = defaultdict(set)

        for need_update in status['outdated'].values():
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
        parent = None

        if isinstance(node, Entity):
            parent_siblings = self.siblings(node.parent) - {node.parent}
            return set(node.parent.members) | parent_siblings
        elif isinstance(node, Generation):
            parent = node.activity
        elif isinstance(node, Usage):
            parent = self.activities[node.commit]
        elif isinstance(node, Process):
            return {node}

        if parent is None or not isinstance(parent, ProcessRun):
            raise errors.InvalidOutputPath(
                'The file "{0}" was not created by a renku command. \n\n'
                'Check the file history using: git log --follow "{0}"'.format(
                    node.path
                )
            )

        return set(parent.generated)

    def ascwl(
        self,
        input_paths=None,
        output_paths=None,
        outputs=None,
        use_latest=True,
    ):
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

        def connect_file_to_directory(node):
            """Return step connecting file to a directory."""
            process = attr.evolve(
                LINK_CWL,
                inputs={
                    'input_directory': 'Directory',
                    'filename': {
                        'type': 'string',
                        'default':
                            str(Path(node.path).relative_to(node.parent.path)),
                    },
                }
            )
            process_run = ProcessRun(
                commit=node.commit,
                client=node.client,
                path=None,
                process=process,
                inputs={
                    node.parent.path:
                        Usage(
                            entity=node.parent,
                            role='input_directory',
                        ),
                },
                outputs={
                    node.path: 'output_file',
                },
            )

            for generated in process_run.generated:
                nodes[(generated.commit, generated.path)] = generated

            return process_run

        for node in self.nodes:
            if (node.commit, node.path) not in output_keys:
                continue

            process_run = None
            if isinstance(node, Entity) and not hasattr(node, 'activity'):
                process_run = connect_file_to_directory(node)

                stack.append(process_run)
                processes.add(process_run)

            else:
                assert hasattr(node, 'activity'), node
                assert isinstance(node.activity, ProcessRun)

                plan = node.activity.association.plan
                latest = self.latest(plan)
                if use_latest and latest:
                    plan = nodes[(latest, plan.path)]

                process_run = plan.activity

                if process_run not in processes:
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

                node = nodes.get((dependency.commit, dependency.path),
                                 dependency)

                if isinstance(node, Generation):
                    process_run = node.activity
                elif isinstance(node, Collection) and node.parent:
                    raise NotImplementedError('Can not connect subdirectory')
                elif isinstance(node, Entity) and node.parent:
                    process_run = connect_file_to_directory(node)
                else:
                    process_run = None

                # Skip existing commits
                if process_run and isinstance(process_run, ProcessRun):
                    plan = process_run.association.plan
                    latest = self.latest(plan)
                    if process_run.path and use_latest and latest:
                        plan = nodes[(latest, plan.path)]

                    process_run = plan.activity

                    if process_run not in processes:
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
                    ins[dependency.role] = alias

            outs = list(set(action.outputs.values()))

            for generated in action.generated:
                if generated.entity.path not in output_paths:
                    output_paths.add(generated.entity.path)
                    outputs.add(generated.entity)

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
                run=self.client.path / action.path if action.path else tool,
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

            if process_run.process is None or process_run.path is None:
                continue

            output_id = process_run.outputs[path]
            type_ = next(
                output for output in process_run.process.outputs
                if output.id == output_id
            ).type
            type_ = type_ if type_ == 'Directory' else 'File'
            output_source = _source_name(commit, path)

            if output_source is None:
                continue

            workflow.outputs.append(
                WorkflowOutputParameter(
                    id=id_,
                    type=type_,
                    outputSource=output_source,
                )
            )

        return workflow
