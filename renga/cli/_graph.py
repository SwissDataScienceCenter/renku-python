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

import attr
import networkx as nx
import yaml

from renga._compat import Path
from renga.models.cwl.command_line_tool import CommandLineTool


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
        """Return a CWL that generated the path."""
        files = [
            file_ for file_ in commit.stats.files.keys()
            if file_.startswith(self.cwl_prefix) and file_.endswith('.cwl')
        ]

        if len(files) == 1:
            return os.path.relpath(Path(files[0]).resolve(), self.repo_path)

    def find_latest(self, start, path):
        """Return the latest commit for path."""
        commits = list(
            self.repo.git.iter_commits('{0}..'.format(start), paths=path))
        if commits:
            return commits[-1]

    def iter_inputs(self, tool, basedir):
        """Yield path of tool inputs."""
        if tool.stdin:
            raise NotImplemented(tool.stdin)
        for input_ in tool.inputs:
            if input_.type == 'File' and input_.default:
                yield os.path.relpath(
                    (basedir / input_.default.path).resolve(), self.repo_path)

    def add_tool(self, commit, path):
        """Add a tool and its dependencies to the graph."""
        data = (commit.tree / path).data_stream.read()
        tool = CommandLineTool.from_cwl(yaml.load(data))
        tool_key = self.add_node(commit, path, tool=tool)

        for input_path in self.iter_inputs(tool, os.path.dirname(path)):
            input_key = self.add_file(
                input_path, revision='{0}^'.format(commit))
            #: Edge from an input to the tool.
            self.G.add_edge(input_key, tool_key)

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
                self.G.add_edge(tool_key, file_key)
                return file_key

        if file_commits:
            #: Does not have a parent CWL.
            return self.add_node(file_commits[0], path)
