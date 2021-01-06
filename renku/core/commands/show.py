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
"""Renku show command."""

from collections import namedtuple

from renku.core import errors
from renku.core.commands.graph import Graph
from renku.core.incubation.command import Command
from renku.core.models.entities import Entity
from renku.core.models.provenance.activities import ProcessRun

Result = namedtuple("Result", ["path", "commit", "time", "workflow"])


def get_siblings():
    """Return siblings for given paths."""
    return Command().command(_get_siblings).require_migration()


def _get_siblings(client, revision, verbose, paths):
    def get_sibling_name(graph, node):
        """Return the display name of a sibling."""
        name = graph._format_path(node.path)
        return "{} @ {}".format(name, node.commit) if verbose else name

    graph = Graph(client)
    nodes = graph.build(paths=paths, revision=revision)
    nodes = [n for n in nodes if not isinstance(n, Entity) or n.parent]

    sibling_sets = {frozenset([n]) for n in set(nodes)}
    for node in nodes:
        try:
            sibling_sets.add(frozenset(graph.siblings(node)))
        except errors.InvalidOutputPath:
            # ignore nodes that aren't outputs if no path was supplied
            if paths:
                raise
            else:
                sibling_sets.discard({node})

    result_sets = []
    for candidate in sibling_sets:
        new_result = []

        for result in result_sets:
            if candidate & result:
                candidate |= result
            else:
                new_result.append(result)

        result_sets = new_result
        result_sets.append(candidate)

    return [[get_sibling_name(graph, node) for node in r] for r in result_sets]


def get_inputs():
    """Return inputs files in the repository."""
    return Command().command(_get_inputs).require_migration()


def _get_inputs(client, revision, paths):
    graph = Graph(client)
    paths = set(paths)
    nodes = graph.build(revision=revision)
    commits = {node.activity.commit if hasattr(node, "activity") else node.commit for node in nodes}
    commits |= {node.activity.commit for node in nodes if hasattr(node, "activity")}
    candidates = {(node.commit, node.path) for node in nodes if not paths or node.path in paths}

    input_paths = {}

    for commit in commits:
        activity = graph.activities.get(commit)
        if not activity:
            continue

        if isinstance(activity, ProcessRun):
            for usage in activity.qualified_usage:
                for entity in usage.entity.entities:
                    path = str((usage.client.path / entity.path).relative_to(client.path))
                    usage_key = (entity.commit, entity.path)

                    if path not in input_paths and usage_key in candidates:
                        input_paths[path] = Result(
                            path=path, commit=entity.commit, time=activity.started_at_time, workflow=activity.path
                        )

    return {graph._format_path(k): v for k, v in input_paths.items()}


def get_outputs():
    """Return output files in the repository."""
    return Command().command(_get_outputs).require_migration()


def _get_outputs(client, revision, paths):
    graph = Graph(client)
    filter_ = graph.build(paths=paths, revision=revision)
    output_paths = {}

    for activity in graph.activities.values():
        if isinstance(activity, ProcessRun):
            for entity in activity.generated:
                if entity.path not in graph.output_paths:
                    continue
                output_paths[entity.path] = Result(
                    path=entity.path, commit=entity.commit, time=activity.ended_at_time, workflow=activity.path
                )

    return filter_, {graph._format_path(k): v for k, v in output_paths.items()}
