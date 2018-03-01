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
from itertools import groupby
from operator import itemgetter

import attr
import networkx as nx
import yaml
from git import IndexFile, Submodule

from renga._compat import Path
from renga.models.cwl.command_line_tool import CommandLineTool
from renga.models.cwl.workflow import Workflow

from ._repo import Repo


@attr.s
class Graph(object):
    """Represent the provenance graph."""

    repo = attr.ib()
    G = attr.ib(default=attr.Factory(nx.DiGraph))

    repo_path = attr.ib(init=False)
    cwl_prefix = attr.ib(init=False)

    def __attrs_post_init__(self):
        """Derive basic informations."""
        self.repo_path = self.repo.path
        self.cwl_prefix = str(
            self.repo.workflow_path.relative_to(self.repo_path))

    def add_node(self, commit, path, **kwargs):
        """Add a node representing a file."""
        key = str(commit), str(path)
        if key not in self.G.node:
            latest = self.find_latest(commit, path)
            self.G.add_node(
                key, commit=commit, path=path, latest=latest, **kwargs)
        return key

    def find_cwl(self, commit):
        """Return a CWL."""
        files = [
            file_ for file_ in commit.stats.files.keys()
            if file_.startswith(self.cwl_prefix) and file_.endswith('.cwl')
        ]

        if len(files) == 1:
            return files[0]

    def find_latest_cwl(self):
        """Return the latest CWL in the repository."""
        for commit in self.repo.git.iter_commits(paths=self.cwl_prefix):
            cwl = self.find_cwl(commit)
            if cwl:
                return cwl

    def find_latest(self, start, path):
        """Return the latest commit for path."""
        commits = list(
            self.repo.git.iter_commits('{0}..'.format(start), paths=path))
        if commits:
            return commits[-1]

    def iter_file_inputs(self, tool, basedir):
        """Yield path of tool file inputs."""
        if tool.stdin:
            if tool.stdin[0] != '$':  # pragma: no cover
                raise NotImplemented(tool.stdin)
        for input_ in tool.inputs:
            if input_.type == 'File' and input_.default:
                yield (
                    os.path.normpath(basedir / input_.default.path),
                    input_.id
                )

    def add_tool(self, commit, path):
        """Add a tool and its dependencies to the graph."""
        data = (commit.tree / path).data_stream.read()
        tool = CommandLineTool.from_cwl(yaml.load(data))
        tool_key = self.add_node(commit, path, tool=tool)

        for input_path, input_id in self.iter_file_inputs(
                tool, os.path.dirname(path)):
            input_key = self.add_file(
                input_path, revision='{0}^'.format(commit))
            #: Edge from an input to the tool.
            self.G.add_edge(input_key, tool_key, id=input_id)

        return tool_key

    def add_file(self, path, revision='HEAD'):
        """Add a file node to the graph."""
        file_commits = list(self.repo.git.iter_commits(revision, paths=path))

        for commit in file_commits:
            cwl = self.find_cwl(commit)
            if cwl is not None:
                file_key = self.add_node(commit, path)
                tool_key = self.add_tool(commit, cwl)
                #: Edge from a tool to the output.
                tool = self.G.nodes[tool_key]['tool']
                output_id = tool.get_output_id(path)
                self.G.add_edge(tool_key, file_key, id=output_id)
                return file_key

        if file_commits:
            #: Does not have a parent CWL.
            root_node = self.add_node(file_commits[0], path)
            parent_commit, parent_path = root_node

            #: Capture information about the submodule in a submodule.
            root_submodule = self.G.nodes[root_node].get('submodule', [])

            #: Resolve Renga based submodules.
            original_path = Path(parent_path)
            if original_path.is_symlink() or str(original_path).startswith(
                    '.renga/vendors'):
                original_path = original_path.resolve()

                for submodule in Submodule.iter_items(
                        self.repo.git, parent_commit=parent_commit):
                    try:
                        subpath = original_path.relative_to(
                            Path(submodule.path).resolve())
                        subgraph = Graph(repo=Repo(git_home=submodule.path))
                        subnode = subgraph.add_file(
                            str(subpath), revision=submodule.hexsha)

                        #: Extend node metadata.
                        for _, data in subgraph.G.nodes(data=True):
                            data['submodule'] = root_submodule + [
                                submodule.name
                            ]

                        #: Merge file node with it's symlinked version.
                        self.G = nx.contracted_nodes(
                            nx.compose(self.G, subgraph.G),
                            root_node,
                            subnode,
                        )  # TODO optionally it can be changed to an edge.
                        break
                    except ValueError:
                        continue

            return root_node

    @property
    def _output_keys(self):
        """Return a list of the output keys."""
        return [n for n, d in self.G.out_degree() if d == 0]

    def _need_update(self):
        """Yield all files that need to be updated."""
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

    def build_status(self, revision='HEAD'):
        """Return files from the revision grouped by their status."""
        index = self.repo.git.index if revision == 'HEAD' \
            else IndexFile.from_tree(repo.git, revision)

        current_files = set()

        for filepath, _ in index.entries.keys():
            if not filepath.startswith('.renga') and \
                    filepath not in {'.gitignore', '.gitattributes'}:
                self.add_file(filepath, revision=revision)
                current_files.add(filepath)

        # Prepare status info for each file.
        self._need_update()

        graph_files = sorted(
            ((commit, filepath) for (commit, filepath) in self.G
             if filepath in current_files),
            key=itemgetter(1))

        status = {'up-to-date': {}, 'outdated': {}, 'multiple-versions': {}}

        for filepath, keys in groupby(graph_files, itemgetter(1)):
            keys = list(keys)

            if len(keys) > 1:
                status['multiple-versions'][filepath] = keys

            if any(len(self.G.nodes[key]['_need_update']) > 1 for key in keys):
                updates = list(
                    self.G.nodes[key]['_need_update'] for key in keys)
                status['outdated'][filepath] = updates
            elif len(keys) == 1:
                status['up-to-date'][filepath] = keys[0][0]

        return status

    @property
    def output_files(self):
        """Return a list of nodes representing output files."""
        for key in self._output_keys:
            node = self.G.nodes[key]
            if 'tool' not in node:
                yield key, node

    def _source_name(self, key):
        """Find source name for a node."""
        if self.G.in_degree(key) == 0:
            return None

        assert self.G.in_degree(key) == 1

        tool_key, attr = list(self.G.pred[key].items())[0]
        step = self.G.nodes[tool_key]['step']['id']
        return '{0}/{1}'.format(step, attr['id'])

    @property
    def _tool_nodes(self):
        """Yield topologically sorted tools."""
        for key in nx.topological_sort(self.G):
            node = self.G.nodes[key]
            tool = node.get('tool')
            if tool is not None:
                yield key, node

    def ascwl(self):
        """Serialize graph to CWL workflow."""
        workflow = Workflow()

        input_index = 1

        for tool_index, (key, node) in enumerate(self._tool_nodes, 1):
            _, path = key
            tool = node['tool']
            step_id = 'step_{0}'.format(tool_index)
            node['step'] = {'id': step_id}

            ins = {
                edge_id: self._source_name(target_id)
                for target_id, _, edge_id in self.G.in_edges(key, data='id')
            }
            outs = [
                edge_id for _, _, edge_id in self.G.out_edges(key, data='id')
            ]

            for input_ in tool.inputs:
                input_mapping = ins.get(input_.id)
                if input_mapping is None:
                    input_id = 'input_{0}'.format(input_index)
                    workflow.inputs.append({
                        'id': input_id,
                        'type': input_.type,
                        'default': input_.default,
                    })
                    input_index += 1
                    ins[input_.id] = input_id

            workflow.add_step(
                run=Path(path),
                id=step_id,
                in_=ins,
                out=outs,
            )

        for index, key in enumerate(self._output_keys):
            output_id = 'output_{0}'.format(index)
            workflow.outputs.append({
                'id': output_id,
                'type': 'File',
                'outputSource': self._source_name(key),
            })

        return workflow
