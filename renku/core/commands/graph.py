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
"""Graph builder."""

import os
from collections import OrderedDict, defaultdict, deque
from pathlib import Path

import attr
from git import NULL_TREE

from renku.core import errors
from renku.core.commands.client import pass_local_client
from renku.core.models.entities import Collection, Entity
from renku.core.models.git import Range
from renku.core.models.provenance.activities import Activity, ProcessRun, Usage, WorkflowRun
from renku.core.models.provenance.qualified import Generation
from renku.core.models.workflow.run import Run
from renku.core.utils.scm import git_unicode_unescape


def _safe_path(filepath, can_be_cwl=False):
    """Check if the path should be used in output."""
    if isinstance(filepath, Path):
        filepath = str(filepath)

    # Should not be in ignore paths.
    if filepath in {".gitignore", ".gitattributes"}:
        return False

    # Ignore everything in .renku ...
    if filepath.startswith(".renku"):
        # ... unless it can be a CWL.
        if can_be_cwl and filepath.startswith(".renku/workflow/"):
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
    _workflows = attr.ib(default=attr.Factory(dict))

    cwl_prefix = attr.ib(init=False)

    def __attrs_post_init__(self):
        """Derive basic information."""
        self.cwl_prefix = self.client.cwl_prefix

    @_nodes.default
    def default_nodes(self):
        """Build node index."""
        self.generated = {}
        nodes = OrderedDict()

        for commit in reversed(self._sorted_commits):
            try:
                activity = self.activities[commit]
                nodes.update(((node.commit, node.path), node) for node in reversed(list(activity.nodes)))

                if isinstance(activity, ProcessRun):
                    self.generated.update({generation.entity._id: generation for generation in activity.generated})

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
                return result + self.parents(node.activity.association.plan)
            return result
        elif isinstance(node, Usage):
            return _from_entity(node.entity)
        elif isinstance(node, Entity):
            # Link files and directories and generations.
            return ([node.parent] if node.parent is not None else []) + _from_entity(node, check_parents=False)
        elif isinstance(node, Run):
            # warnings.warn('Called on run {0}'.format(node), stacklevel=2)
            activity = node.activity
            return self.parents(activity) if activity else []
        elif isinstance(node, ProcessRun):
            return node.qualified_usage
        elif isinstance(node, Activity):
            warnings.warn("Called parents on {0}".format(node), stacklevel=2)
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
        try:
            p = Path(path).resolve()
            p.relative_to(self.client.path)
        except ValueError:  # External file
            path = Path(os.path.abspath(path))
        else:
            path = p
        return os.path.relpath(str(path), start=str(start))

    def _format_path(self, path):
        """Return a relative path based on the client configuration."""
        return os.path.relpath(str(self.client.path / path))

    def dependencies(self, revision="HEAD", paths=None):
        """Return dependencies from a revision or paths."""
        result = []

        if paths:
            paths = (self.normalize_path(path) for path in paths)
        else:
            if revision == "HEAD":
                index = self.client.repo.index
            else:
                from git import IndexFile

                index = IndexFile.from_tree(self.client.repo, revision)

            paths = (path for path, _ in index.entries.keys())

        for path in paths:
            try:
                result.append(Usage.from_revision(self.client, path=path, revision=revision,))
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
        usage_paths = []

        while queue:
            processing = queue.popleft()

            if processing.commit in visited:
                continue

            # Mark as visited:
            visited.add(processing.commit)

            activity = processing.client.process_commit(processing.commit)
            if activity is None:
                continue

            self.activities[activity.commit] = activity

            # Iterate over parents.
            if isinstance(activity, ProcessRun):
                if isinstance(activity, WorkflowRun):
                    self._workflows[activity.path] = activity
                for entity in activity.qualified_usage:
                    for member in entity.entities:
                        parent_activities = self.client.activities_for_paths(paths=[member.path], revision="HEAD")
                        for a in parent_activities:
                            if a.commit and a.commit not in visited:
                                self.activities[a.commit] = a
                        if member.commit not in visited:
                            queue.append(member)
                        usage_paths.append(member.path)
                for entity in activity.generated:
                    for member in entity.entities:
                        if all(member.path != d.path for d in dependencies) and any(
                            u.startswith(member.path) for u in usage_paths
                        ):
                            dependencies = [
                                d
                                for d in dependencies
                                if not (self.client.path / member.path).is_dir() or not d.path.startswith(member.path)
                            ]
                            dependencies.append(member)

        from renku.core.models.sort import topological

        commit_nodes = {commit: activity.parents for commit, activity in self.activities.items()}

        # add dependencies between processes
        for activity in self.activities.values():
            if not isinstance(activity, ProcessRun):
                continue
            for usage in activity.qualified_usage:
                for other_activity in self.activities.values():
                    if other_activity == activity:
                        continue
                    if any(
                        g.path == usage.path and g.commit.hexsha == usage.commit.hexsha
                        for g in other_activity.generated
                    ):
                        parents = commit_nodes[activity.commit]
                        if other_activity.commit in parents:
                            continue
                        parents.append(other_activity.commit)

        self._sorted_commits = topological(commit_nodes)
        self._nodes = self.default_nodes()

        return dependencies

    def build(self, revision="HEAD", paths=None, dependencies=None, can_be_cwl=False):
        """Build graph from paths and/or revision."""
        interval = Range.rev_parse(self.client.repo, revision)

        if dependencies is None:
            dependencies = self.dependencies(revision=revision, paths=paths)

        ignore = {commit for commit in self.client.repo.iter_commits(interval.start)} if interval.start else set()
        dependencies = self.process_dependencies(dependencies, visited=ignore)

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
            if isinstance(activity, ProcessRun) and activity.association and activity.association.plan:
                paths |= {o.produces.path for o in activity.association.plan.outputs if o.produces.path}
        return paths

    def build_status(self, revision="HEAD", can_be_cwl=False):
        """Return files from the revision grouped by their status."""
        status = {
            "up-to-date": {},
            "outdated": {},
            "multiple-versions": {},
            "deleted": {},
        }

        dependencies = self.dependencies(revision=revision)
        current_files = self.build(dependencies=dependencies, can_be_cwl=can_be_cwl,)

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
            need_update = [dependency for dependency in self.need_update(node) if dependency.path != node.path]

            if need_update:
                status["outdated"][node.path] = need_update
            else:
                status["up-to-date"][node.path] = node.commit

        # Merge all versions of used inputs in outdated file.
        multiple_versions = defaultdict(set)

        for need_update in status["outdated"].values():
            for node in need_update:
                multiple_versions[node.path].add(node)

        for node in current_files:
            if node.path in multiple_versions:
                multiple_versions[node.path].add(node)

        status["multiple-versions"] = {key: value for key, value in multiple_versions.items() if len(value) > 1}

        # Build a list of used files that have been deleted.
        current_paths = {node.path for node in current_files}
        status["deleted"] = {
            node.path: node
            for node in self.nodes
            if _safe_path(node.path, can_be_cwl=can_be_cwl)
            and node.path not in current_paths
            and not ((self.client.path / node.path).exists() or (self.client.path / node.path).is_dir())
        }
        return status

    def siblings(self, node):
        """Return siblings for a given node.

        The key is part of the result set, hence to check if the node has
        siblings you should check the length is greater than 1.
        """
        parent = None

        if isinstance(node, Entity):
            if not node.parent:
                return {node}
            parent_siblings = self.siblings(node.parent) - {node.parent}
            return set(node.parent.members) | parent_siblings
        elif isinstance(node, Generation):
            parent = node.activity
        elif isinstance(node, Usage):
            parent = self.activities[node.commit]
        elif isinstance(node, Run):
            return {node}

        if parent is None or not isinstance(parent, ProcessRun):
            raise errors.InvalidOutputPath(
                'The file "{0}" was not created by a renku command. \n\n'
                'Check the file history using: git log --follow "{0}"'.format(node.path)
            )

        return set(parent.generated)

    def as_workflow(
        self, input_paths=None, output_paths=None, outputs=None, use_latest=True,
    ):
        """Serialize graph to renku ``Run`` workflow."""
        processes = set()
        stack = []

        output_keys = {(node.commit, node.path) for node in outputs}
        nodes = {(node.commit, node.path): node for node in self.nodes}

        for node in self.nodes:
            if (node.commit, node.path) not in output_keys:
                continue

            if not hasattr(node, "activity"):
                continue

            assert isinstance(node.activity, ProcessRun)

            plan = node.activity.association.plan
            process_run = plan.activity

            if input_paths and any(g.path in input_paths for g in process_run.generated):
                continue

            if process_run not in processes:
                stack.append(process_run)
                processes.add(process_run)

        while stack:
            action = stack.pop()

            if not hasattr(action, "association") or not hasattr(action.association.plan, "inputs"):
                continue

            for inp in action.association.plan.inputs:
                path = inp.consumes.path
                dependency = inp.consumes
                # Do not follow defined input paths.
                if input_paths and path in input_paths:
                    continue

                node = nodes.get((dependency.commit, dependency.path), dependency)

                if isinstance(node, Generation):
                    process_run = node.activity
                elif isinstance(node, Collection) and node.parent:
                    raise NotImplementedError("Can not connect subdirectory")
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

        if len(processes) == 1:
            process_run = list(processes)[0]
            if not isinstance(process_run, WorkflowRun):
                return process_run.association.plan

        parent_process = Run(client=self.client)

        for step in processes:
            # Loop through runs and add them as sub processes to parent.
            parent_process.add_subprocess(step.association.plan)

        return self._find_identical_parent_run(run=parent_process, outputs=outputs)

    def _find_identical_parent_run(self, run, outputs):
        from marshmallow.exceptions import ValidationError

        def workflow_has_identical_subprocesses(workflow_, subprocesses_ids_):
            wf_subprocesses_ids = [step.process._id for step in workflow_.association.plan.subprocesses]
            return wf_subprocesses_ids == subprocesses_ids_

        subprocesses_ids = [step.process._id for step in run.subprocesses]
        for workflow in self._workflows.values():
            if workflow_has_identical_subprocesses(workflow, subprocesses_ids):
                return workflow.association.plan

        # Search all workflow files that generate the same outputs to find a similar parent run
        workflow_files = set()
        for output in outputs:
            activities = self.client.path_activity_cache.get(output.path, {}).values()
            workflow_files |= set([file for activity in activities for file in activity])
        for file_ in workflow_files:
            try:
                workflow = WorkflowRun.from_yaml(path=file_, client=self.client)
            except ValidationError:  # Not a WorkflowRun
                pass
            else:
                if workflow_has_identical_subprocesses(workflow, subprocesses_ids):
                    return workflow.association.plan

        return run


@pass_local_client(requires_migration=True)
def build_graph(client, revision, no_output, paths):
    """Build graph structure."""
    graph = Graph(client)
    if not paths:
        start, is_range, stop = revision.partition("..")
        if not is_range:
            stop = start
        elif not stop:
            stop = "HEAD"

        commit = client.repo.rev_parse(stop)
        paths = (
            str(client.path / git_unicode_unescape(item.a_path))
            for item in commit.diff(commit.parents or NULL_TREE)
            # if not item.deleted_file
        )

    # NOTE shall we warn when "not no_output and not paths"?
    graph.build(paths=paths, revision=revision, can_be_cwl=no_output)
    return graph
