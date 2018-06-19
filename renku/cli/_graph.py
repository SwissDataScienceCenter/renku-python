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

from renku._compat import Path
from renku.api import LocalClient
from renku.models.cwl.command_line_tool import CommandLineTool
from renku.models.cwl.parameter import InputParameter, WorkflowOutputParameter
from renku.models.cwl.types import File
from renku.models.cwl.workflow import Workflow


def _safe_path(filepath, can_be_workflow=False):
    """Check if the path should be used in output."""
    return not filepath.startswith('.renku') and \
        filepath not in {'.gitignore', '.gitattributes'}


@attr.s
class Graph(object):
    """Represent the provenance graph."""

    client = attr.ib()
    G = attr.ib(default=attr.Factory(nx.DiGraph))

    cwl_prefix = attr.ib(init=False)

    def __attrs_post_init__(self):
        """Derive basic informations."""
        self.cwl_prefix = str(
            self.client.workflow_path.resolve().relative_to(self.client.path)
        )

    def normalize_path(self, path):
        """Normalize path relative to the Git workdir."""
        start = self.client.path.resolve()
        path = Path(path).resolve()
        return os.path.relpath(path, start=start)

    def _format_path(self, path):
        """Return a relative path based on the client configuration."""
        return os.path.relpath(self.client.path / path)

    def add_node(self, commit, path, **kwargs):
        """Add a node representing a file."""
        key = str(commit), str(path)
        if key not in self.G.node:
            latest = self.find_latest(commit, path)
            self.G.add_node(
                key, commit=commit, path=path, latest=latest, **kwargs
            )
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
        for commit in self.client.git.iter_commits(paths=self.cwl_prefix):
            cwl = self.find_cwl(commit)
            if cwl:
                return cwl

    def find_latest(self, start, path):
        """Return the latest commit for path."""
        commits = list(
            self.client.git.iter_commits('{0}..'.format(start), paths=path)
        )
        if commits:
            return commits[-1]

    def iter_input_files(self, tool, basedir):
        """Yield tuples with input id and path."""
        stdin = getattr(tool, 'stdin', None)
        if stdin and stdin[0] != '$':  # pragma: no cover
            raise NotImplemented(tool.stdin)
        for input_ in tool.inputs:
            if input_.type == 'File' and input_.default:
                yield (
                    input_.id, os.path.normpath(basedir / input_.default.path)
                )

    def iter_output_files(self, tool):
        """Yield tuples with output id and path."""
        for output in tool.outputs:
            if output.type in {'stdout', 'stderr'}:
                stream = getattr(tool, output.type)
                if stream:
                    yield output.id, stream
            elif output.type == 'File':
                glob = output.outputBinding.glob
                # TODO better support for Expression
                if glob.startswith('$(inputs.'):
                    input_id = glob[len('$(inputs.'):-1]
                    for input_ in tool.inputs:
                        if input_.id == input_id:
                            yield output.id, input_.default
                            break  # out from tool.inputs
                else:
                    yield output.id, glob

    def add_workflow(self, commit, path, cwl=None, file_key=None):
        """Add a workflow and its dependencies to the graph."""
        if cwl is None:
            data = (commit.tree / path).data_stream.read()
            cwl = yaml.load(data)

        workflow = Workflow.from_cwl(cwl)
        basedir = os.path.dirname(path)

        # Keep track of node identifiers for steps, inputs and outputs:
        step_map = {}
        input_map = {}
        output_map = {}

        #: First find workflow inputs, but don't connect them yet.
        for input_id, input_path in self.iter_input_files(workflow, basedir):
            input_key = self.add_file(
                input_path, revision='{0}^'.format(commit)
            )
            input_map[input_id] = input_key

        for step in workflow.steps:
            tool_key = self.add_tool(
                commit,
                os.path.join(basedir, step.run),
                file_key=file_key,
                is_step=True,
            )

            step_tool = self.G.nodes[tool_key]['tool']

            for input_id, input_path in self.iter_input_files(
                step_tool, basedir
            ):
                if input_path in commit.stats.files:
                    #: Check intermediate committed files
                    input_key = self.add_node(commit, input_path)
                    #: Edge from an input to the tool.
                    self.G.add_edge(input_key, tool_key, id=input_id)
                else:
                    #: Global workflow input
                    source = step.in_[input_id]
                    self.G.add_edge(input_map[source], tool_key, id=input_id)

            # Find ALL siblings that MUST be generated in the same commit.
            for output_id, output_path in self.iter_output_files(step_tool):
                self.G.add_edge(
                    tool_key, (str(commit), output_path), id=output_id
                )

            output_map.update({
                step.id + '/' + name: target
                for target, _, name in self.G.in_edges(tool_key, data='id')
            })
            step_map[step.id] = tool_key

            self.G.nodes[tool_key]['workflow'] = workflow
            self.G.nodes[tool_key]['workflow_path'
                                   ] = path + '#steps/' + step.id

        for step in workflow.steps:
            for alias, source in step.in_.items():
                name = step.id + '/' + alias

                if name in output_map and '/' in source:
                    other_step, id_ = source.split('/')
                    other_key = step_map[other_step]
                    self.G.add_edge(other_key, output_map[name], id=id_)

        return workflow

    def add_tool(
        self, commit, path, file_key=None, expand_workflow=True, is_step=False
    ):
        """Add a tool and its dependencies to the graph."""
        data = (commit.tree / path).data_stream.read()
        cwl = yaml.load(data)

        try:
            tool = CommandLineTool.from_cwl(cwl)
        except TypeError:
            if expand_workflow:
                return self.add_workflow(
                    commit, path, file_key=file_key, cwl=cwl
                )
            tool = Workflow.from_cwl(cwl)

        tool_key = self.add_node(commit, path, tool=tool)

        if is_step:
            return tool_key

        for input_id, input_path in self.iter_input_files(
            tool, os.path.dirname(path)
        ):
            input_key = self.add_file(
                input_path, revision='{0}^'.format(commit)
            )
            #: Edge from an input to the tool.
            self.G.add_edge(input_key, tool_key, id=input_id)

        # Find ALL siblings that MUST be generated in the same commit.
        for output_id, path in self.iter_output_files(tool):
            self.G.add_edge(tool_key, (str(commit), path), id=output_id)

        return tool_key

    def add_file(self, path, revision='HEAD'):
        """Add a file node to the graph."""
        file_commits = list(self.client.git.iter_commits(revision, paths=path))

        if not file_commits:
            raise KeyError(
                'Could not find a file {0} in range {1}'.format(
                    path, revision
                )
            )

        commit = file_commits[0]

        cwl = self.find_cwl(commit)
        if cwl is not None:
            file_key = self.add_node(commit, path)
            self.add_tool(commit, cwl, file_key=file_key)
            return file_key
        else:
            #: Does not have a parent CWL.
            root_node = self.add_node(commit, path)
            parent_commit, parent_path = root_node

            #: Capture information about the submodule in a submodule.
            root_submodule = self.G.nodes[root_node].get('submodule', [])

            #: Resolve Renku based submodules.
            original_path = Path(parent_path)
            if original_path.is_symlink(
            ) or str(original_path).startswith('.renku/vendors'):
                original_path = original_path.resolve()

                for submodule in Submodule.iter_items(
                    self.client.git, parent_commit=parent_commit
                ):
                    try:
                        subpath = original_path.relative_to(
                            Path(submodule.path).resolve()
                        )
                        subgraph = Graph(
                            client=LocalClient(path=submodule.path)
                        )
                        subnode = subgraph.add_file(
                            str(subpath), revision=submodule.hexsha
                        )

                        #: Extend node metadata.
                        for _, data in subgraph.G.nodes(data=True):
                            data['submodule'
                                 ] = root_submodule + [submodule.name]

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

    def build_status(self, revision='HEAD'):
        """Return files from the revision grouped by their status."""
        index = self.client.git.index if revision == 'HEAD' \
            else IndexFile.from_tree(self.client.git, revision)

        current_files = set()

        for filepath, _ in index.entries.keys():
            if _safe_path(filepath):
                self.add_file(filepath, revision=revision)
                current_files.add(filepath)

        # Prepare status info for each file.
        self._need_update()

        graph_files = sorted(((commit, filepath)
                              for (commit, filepath) in self.G
                              if filepath in current_files),
                             key=itemgetter(1))

        status = {'up-to-date': {}, 'outdated': {}, 'multiple-versions': {}}

        for filepath, keys in groupby(graph_files, itemgetter(1)):
            keys = list(keys)

            if len(keys) > 1:
                status['multiple-versions'][filepath] = keys

            nodes = [self.G.nodes[key] for key in keys]

            # Any latest version of a file needs an update.
            is_outdated = any(
                len(node.get('_need_update', [])) > 1
                for node in nodes if node.get('latest') is None
            )

            if is_outdated:
                updates = list(
                    self.G.nodes[key]['_need_update'] for key in keys
                )
                status['outdated'][filepath] = updates
            else:
                status['up-to-date'][filepath] = keys[0][0]

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
        return {
            sibling
            for parent in self.G.predecessors(key)
            for sibling in self.G.successors(parent)
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
            """Evolve ``File`` path."""
            if isinstance(default, File):
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

        output_keys = (key for _, key in self.G.out_edges(steps.keys())
                       ) if global_step_outputs else self._output_keys

        for index, key in enumerate(output_keys):
            output_id = 'output_{0}'.format(index)
            workflow.outputs.append(
                WorkflowOutputParameter(
                    id=output_id,
                    type='File',
                    outputSource=_source_name(key),
                )
            )

        return workflow
