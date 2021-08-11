# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 - Swiss Data Science Center (SDSC)
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
"""Dependency and Provenance graph building."""

import functools
import sys
from pathlib import Path

from pkg_resources import resource_filename

from renku.core import errors
from renku.core.management import LocalClient
from renku.core.management.command_builder.command import Command, inject
from renku.core.management.config import RENKU_HOME
from renku.core.management.datasets import DatasetsApiMixin
from renku.core.management.repository import RepositoryApiMixin
from renku.core.utils.shacl import validate_graph

GRAPH_METADATA_PATHS = [
    Path(RENKU_HOME) / Path(RepositoryApiMixin.DATABASE_PATH),
    Path(RENKU_HOME) / Path(RepositoryApiMixin.DEPENDENCY_GRAPH),
    Path(RENKU_HOME) / Path(RepositoryApiMixin.PROVENANCE_GRAPH),
    Path(RENKU_HOME) / Path(DatasetsApiMixin.DATASETS_PROVENANCE),
]

# def update():
#     """Return a command for generating the graph."""
#     command = Command().command(_update).lock_project().with_database(write=True)
#     return command.require_migration().require_clean().require_nodejs().with_commit(commit_if_empty=False)


# @inject.autoparams()
# def _update(dry_run, client: LocalClient, database: Database):
#     """Update outdated outputs."""
#     with measure("BUILD AND QUERY GRAPH"):
#         pg = ProvenanceGraph.from_json(client.provenance_graph_path, lazy=True)
#         plans_usages = pg.get_latest_plans_usages()

#     with measure("CALCULATE MODIFIED"):
#         modified, deleted = _get_modified_paths(plans_usages=plans_usages)

#     if not modified:
#         communication.echo("Everything is up-to-date.")
#         return

#     with measure("CALCULATE UPDATES"):
#         plans, plans_with_deleted_inputs = DependencyGraph.from_database(database).get_downstream(modified, deleted)

#     if plans_with_deleted_inputs:
#         formatted_deleted_plans = "".join((f"\n\t{p}" for p in plans_with_deleted_inputs))
#         communication.warn(
#         f"The following steps cannot be executed because one of their inputs is deleted: {formatted_deleted_plans}"
#         )

#     if dry_run:
#         formatted_plans = "".join((f"\n\t{p}" for p in plans))
#         communication.echo(f"The following steps would be executed:{formatted_plans}")
#         return

#     with measure("CONVERTING RUNS"):
#         entities_cache: Dict[str, Entity] = {}
#         runs = [p.to_run(entities_cache) for p in plans]
#         parent_process = Run()
#         for run in runs:
#             parent_process.add_subprocess(run)

#     execute_workflow(workflow=parent_process, output_paths=None, command_name="update", update_commits=True)


def export_graph():
    """Return a command for exporting graph data."""
    return Command().command(_export_graph).with_database(write=False)


@inject.autoparams()
def _export_graph(format, workflows_only, strict, client: LocalClient):
    """Output graph in specific format."""
    if not client.provenance_graph_path.exists():
        raise errors.ParameterError("Graph is not generated.")

    format = format.lower()
    if strict and format not in ["json-ld", "jsonld"]:
        raise errors.SHACLValidationError(f"'--strict' not supported for '{format}'")

    # pg = ProvenanceGraph.from_json(client.provenance_graph_path, lazy=True)

    # # TODO: Add dataset provenance to graph
    # # if not workflows_only:
    # #     pg.rdf_graph.parse(location=str(client.datasets_provenance_path), format="json-ld")

    # graph = pg.rdf_graph

    # if strict:
    #     if format == "jsonld":
    #         format = "json-ld"
    #     _validate_graph(graph, format)

    # return FORMATS[format](graph)


def _dot(rdf_graph, simple=True, debug=False, landscape=False):
    """Format graph as a dot file."""
    from rdflib.tools.rdf2dot import rdf2dot

    from renku.core.commands.format.graph import _rdf2dot_reduced, _rdf2dot_simple

    if debug:
        rdf2dot(rdf_graph, sys.stdout)
        return

    sys.stdout.write('digraph { \n node [ fontname="DejaVu Sans" ] ; \n ')
    if landscape:
        sys.stdout.write('rankdir="LR" \n')
    if simple:
        _rdf2dot_simple(rdf_graph, sys.stdout, graph=rdf_graph)
        return
    _rdf2dot_reduced(rdf_graph, sys.stdout)


_dot_full = functools.partial(_dot, simple=False, landscape=False)
_dot_landscape = functools.partial(_dot, simple=True, landscape=True)
_dot_full_landscape = functools.partial(_dot, simple=False, landscape=True)
_dot_debug = functools.partial(_dot, debug=True)


def _json_ld(rdf_graph):
    """Format graph as JSON-LD."""
    data = rdf_graph.serialize(format="json-ld").decode("utf-8")
    print(data)


FORMATS = {
    "dot": _dot,
    "dot-full": _dot_full,
    "dot-landscape": _dot_landscape,
    "dot-full-landscape": _dot_full_landscape,
    "dot-debug": _dot_debug,
    "json-ld": _json_ld,
    "jsonld": _json_ld,
}


def _validate_graph(rdf_graph, format):
    shacl_path = resource_filename("renku", "data/shacl_shape.json")
    r, _, t = validate_graph(rdf_graph, shacl_path=shacl_path, format=format)

    if not r:
        raise errors.SHACLValidationError(f"{t}\nCouldn't export: Invalid Knowledge Graph data")
