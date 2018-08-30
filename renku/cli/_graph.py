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
from collections import defaultdict
from functools import lru_cache

import attr

from renku import errors
from renku._compat import Path
from renku.models.cwl.parameter import InputParameter, WorkflowOutputParameter
from renku.models.cwl.types import PATH_TYPES
from renku.models.cwl.workflow import Workflow


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
    G = attr.ib()
    commits = attr.ib(default=attr.Factory(dict))

    cwl_prefix = attr.ib(init=False)

    def __attrs_post_init__(self):
        """Derive basic informations."""
        self.cwl_prefix = self.client.cwl_prefix

    @G.default
    def _default_graph(self):
        """Return directional graph."""
        import networkx as nx
        return nx.DiGraph()

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

    def build(
        self, revision='HEAD', paths=None, dependencies=None, can_be_cwl=False
    ):
        """Build graph from paths and/or revision."""
        from renku.models.commit import Action, Dependency

        if paths is None and dependencies is None:
            dependencies = Action.dependencies(
                self.client, revision=revision, can_be_cwl=can_be_cwl
            )
        elif dependencies is None:
            dependencies = [
                Dependency.from_revision(
                    self.client, path=path, revision=revision
                ) for path in paths
            ]

        current_files = {(dependency.commit.hexsha, dependency.path)
                         for dependency in dependencies if dependency.path}
        latest_commits = {path: commit for commit, path in current_files}

        self.commits = Action.build_graph(
            self.client, lookup=dependencies, graph=self.commits
        )

        for action in self.commits.values():
            for key, data in action.iter_nodes():
                path = key[1]
                commit = str(data['commit'])

                if path in latest_commits:
                    latest = latest_commits[path]
                else:
                    latest_dependency = Dependency.from_revision(
                        data['client'],
                        path=data['path'],
                        # revision='{0}'.format(key[0]),
                    )
                    latest = latest_dependency.commit.hexsha
                    # current_files.add((
                    #     latest_dependency.commit.hexsha,
                    #     latest_dependency.path
                    # ))

                if latest and latest != commit:
                    data['latest'] = latest
                    latest_commits.setdefault(path, latest)
                    # current_files.add((latest, path))

                self.G.add_node(key, **data)

            for source, target, data in action.iter_edges():
                self.G.add_edge(source, target, **data)

        return current_files

    @property
    def _output_keys(self):
        """Return a list of the output keys."""
        return [n for n, d in self.G.out_degree() if d == 0]

    def _need_update(self):
        """Yield all files that need to be updated."""
        import networkx as nx
        visited = set()

        for key in nx.topological_sort(self.G):
            assert key not in visited
            need_update = []
            node = self.G.nodes[key]
            latest = node.get('latest')

            if not latest:
                for data in node.get('contraction', {}).values():
                    latest = data.get('latest')
                    if latest:
                        node['latest'] = latest
                        break

            if latest:
                need_update.append(key)

            for parent, _ in self.G.in_edges(key):
                assert parent in visited
                need_update.extend(self.G.nodes[parent]['_need_update'])

            if not latest and need_update:
                need_update.append(key)

            self.G.nodes[key]['_need_update'] = need_update
            visited.add(key)

    def build_status(self, revision='HEAD', can_be_cwl=False):
        """Return files from the revision grouped by their status."""
        status = {
            'up-to-date': {},
            'outdated': {},
            'multiple-versions': {},
            'deleted': {},
        }

        from renku.models.commit import Action
        dependencies = Action.dependencies(
            self.client, revision=revision, can_be_cwl=can_be_cwl
        )
        current_files = set(self.build(dependencies=dependencies))

        # Prepare status info for each file.
        self._need_update()

        # First find all up-to-date nodes.
        up_to_date = {
            filepath: commit
            for (commit,
                 filepath), need_update in self.G.nodes.data('_need_update')
            if not need_update
        }

        for commit, filepath in current_files:
            if filepath in up_to_date:  # trick the workflow step
                # FIXME use the latest commit
                status['up-to-date'][filepath] = up_to_date[filepath]
            else:
                try:
                    need_update = self.G.nodes[(commit,
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
            for commit, filepath in self.G.nodes
            if _safe_path(filepath, can_be_cwl=can_be_cwl) and filepath not in
            current_paths and not ((self.client.path / filepath).exists() or
                                   (self.client.path / filepath).is_dir())
        }
        return status

    @property
    def output_files(self):
        """Return a list of nodes representing output files."""
        for key in self._output_keys:
            node = self.G.nodes[key]
            if 'tool' not in node:
                yield key, node

    @property
    def _tool_nodes(self):
        """Yield topologically sorted tools."""
        import networkx as nx

        for key in nx.topological_sort(self.G):
            node = self.G.nodes[key]
            tool = node.get('tool')
            if tool is not None:
                yield key, node

    def siblings(self, key):
        """Return siblings for a given key.

        The key is part of the result set, hence to check if the node has
        siblings you should check the lenght is greater than 1.
        """
        parents = list(self.G.predecessors(key))
        if not parents:
            raise errors.InvalidOutputPath(
                'The file "{0}" was not created by a renku command. \n\n'
                'Check the file history using: git log --follow "{0}"'.format(
                    key[1]
                )
            )
        return {
            sibling
            for parent in parents for sibling in self.G.successors(parent)
        }

    def ascwl(self, global_step_outputs=False):
        """Serialize graph to CWL workflow.

        :param global_step_outputs: Make all step outputs global.
        """
        workflow = Workflow()

        input_index = 1
        steps = {}

        def _source_name(key):
            """Find source name for a node."""
            if self.G.in_degree(key) == 0:
                return None

            assert self.G.in_degree(key) == 1

            tool_key, node = list(self.G.pred[key].items())[0]
            return '{0}/{1}'.format(steps[tool_key], node['id'])

        def _relative_default(client, default):
            """Evolve ``File`` or ``Directory`` path."""
            if isinstance(default, PATH_TYPES):
                path = (client.workflow_path / default.path).resolve()
                return attr.evolve(default, path=path)
            return default

        for tool_index, (key, node) in enumerate(self._tool_nodes, 1):
            _, path = key
            tool = node['tool']
            step_id = 'step_{0}'.format(tool_index)
            steps[key] = step_id

            ins = {
                edge_id: _source_name(target_id)
                for target_id, _, edge_id in self.G.in_edges(key, data='id')
            }
            outs = [
                edge_id for _, _, edge_id in self.G.out_edges(key, data='id')
            ]

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
                run=self.client.path / path,
                id=step_id,
                in_=ins,
                out=outs,
            )

        if global_step_outputs:
            output_keys = (key for _, key in self.G.out_edges(steps.keys()))
        else:
            output_keys = self._output_keys

        for index, key in enumerate(output_keys):
            output_id = 'output_{0}'.format(index)
            # FIXME use the type of step output
            type_ = 'Directory' if (self.client.path /
                                    key[0]).is_dir() else 'File'
            workflow.outputs.append(
                WorkflowOutputParameter(
                    id=output_id,
                    type=type_,
                    outputSource=_source_name(key),
                )
            )

        return workflow
