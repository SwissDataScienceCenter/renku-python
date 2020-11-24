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
"""PoC command for testing the new graph design."""
import functools
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict

import click
from git import NULL_TREE, GitCommandError

from renku.cli.update import execute_workflow
from renku.core.commands.client import pass_local_client
from renku.core.management.config import RENKU_HOME
from renku.core.management.repository import RepositoryApiMixin
from renku.core.models.entities import Entity
from renku.core.models.provenance.activities import Activity as ActivityRun
from renku.core.models.provenance.activity import ActivityCollection
from renku.core.models.provenance.provenance_graph import ProvenanceGraph
from renku.core.models.workflow.dependency_graph import DependencyGraph
from renku.core.models.workflow.run import Run
from renku.core.utils.contexts import measure

GRAPH_METADATA_PATHS = [
    Path(RENKU_HOME) / Path(RepositoryApiMixin.DEPENDENCY_GRAPH),
    Path(RENKU_HOME) / Path(RepositoryApiMixin.PROVENANCE),
    Path(RENKU_HOME) / Path(RepositoryApiMixin.PROVENANCE_GRAPH),
]


@click.group()
def graph():
    """Proof-of-Concept command for testing the new graph design."""


@graph.command()
@click.option("-f", "--force", is_flag=True, help="Delete existing metadata and regenerate all.")
@pass_local_client(requires_migration=True, commit=True, commit_empty=False, commit_only=GRAPH_METADATA_PATHS)
def generate(client, force):
    """Create new graph metadata."""
    commits = list(client.repo.iter_commits())
    n_commits = len(commits)
    commits = reversed(commits)

    # commits = list(commits)[2810:]

    if force:
        try:
            client.dependency_graph_path.unlink()
        except FileNotFoundError:
            pass
        try:
            client.provenance_graph_path.unlink()
        except FileNotFoundError:
            pass
    else:
        if client.dependency_graph_path.exists() or client.provenance_graph_path.exists():
            raise RuntimeError("Graph files exist. Use --force to regenerate the graph.")

    dependency_graph = DependencyGraph.from_json(client.dependency_graph_path)
    provenance_graph = ProvenanceGraph.from_json(client.provenance_graph_path)

    # client.provenance_path.mkdir(exist_ok=True)

    for n, commit in enumerate(commits):
        print(f"\rProcessing commits {n}/{n_commits}\r", end="", file=sys.stderr)

        for file_ in commit.diff(commit.parents or NULL_TREE):
            # Ignore deleted files (they appear as ADDED in this backwards diff)
            if file_.change_type == "A":
                continue

            path: str = file_.a_path

            if not path.startswith(".renku/workflow") or not path.endswith(".yaml"):
                continue

            # target_path = client.provenance_path / f"{Path(path).stem}.json"
            # if target_path.exists():
            #     raise RuntimeError(f"Target file exists: {target_path}. Use --force to regenerate the graph.")

            # print(f"\rProcessing commits {n}/{n_commits} workflow file: {os.path.basename(path)}\r", file=sys.stderr)

            workflow = ActivityRun.from_yaml(path=path, client=client)
            activity_collection = ActivityCollection.from_activity_run(workflow, dependency_graph, client)

            # activity_collection.to_json(path=target_path)
            provenance_graph.add(activity_collection)

    dependency_graph.to_json()
    provenance_graph.to_json()

    click.secho("OK", fg="green")


@graph.command()
# @click.argument("paths", type=click.Path(exists=True, dir_okay=False), nargs=-1)
@pass_local_client(requires_migration=False)
def status(client):
    r"""Equivalent of `renku status`."""
    with measure("BUILD AND QUERY GRAPH"):
        pg = ProvenanceGraph.from_json(client.provenance_graph_path, lazy=True)
        plans_usages = pg.get_latest_plans_usages()

    # print(plans_usages)

    with measure("CALCULATE MODIFIED"):
        modified, deleted = _get_modified_paths(client=client, plans_usages=plans_usages)

    if not modified and not deleted:
        click.secho("Everything is up-to-date.", fg="green")
        return

    stales = defaultdict(set)

    with measure("CALCULATE UPDATES"):
        dg = DependencyGraph.from_json(client.dependency_graph_path)
        for plan_id, path, _ in modified:
            paths = dg.get_dependent_paths(plan_id, path)
            for p in paths:
                stales[p].add(path)

    print(f"Updates: {len(stales)}", "".join(sorted([f"\n\t{k}: {', '.join(sorted(v))}" for k, v in stales.items()])))
    print()
    modified = {v[1] for v in modified}
    print(f"Modified: {len(modified)}", "".join(sorted([f"\n\t{v}" for v in modified])))
    print()
    deleted = {v[1] for v in deleted}
    print(f"Deleted: {len(deleted)}", "".join(sorted([f"\n\t{v}" for v in deleted])))


@graph.command()
@click.option("-n", "--dry-run", is_flag=True, help="Show steps that will be updated without running them.")
@pass_local_client(clean=True, requires_migration=True, commit=True, commit_empty=False)
def update(client, dry_run):
    r"""Equivalent of `renku update`."""
    with measure("BUILD AND QUERY GRAPH"):
        pg = ProvenanceGraph.from_json(client.provenance_graph_path, lazy=True)
        plans_usages = pg.get_latest_plans_usages()

    # print(plans_usages)

    with measure("CALCULATE MODIFIED"):
        modified, deleted = _get_modified_paths(client=client, plans_usages=plans_usages)

    if not modified:
        click.secho("Everything is up-to-date.", fg="green")
        return

    with measure("CALCULATE UPDATES"):
        dg = DependencyGraph.from_json(client.dependency_graph_path)
        plans, plans_with_deleted_inputs = dg.get_downstream(modified, deleted)

    if plans_with_deleted_inputs:
        print(
            "The following steps cannot be executed because one of their inputs is deleted:",
            "".join((f"\n\t{p}" for p in plans_with_deleted_inputs)),
        )

    if dry_run:
        print("The following steps will be executed:", "".join((f"\n\t{p}" for p in plans)))
        return

    with measure("CONVERTING RUNS"):
        entities_cache: Dict[str, Entity] = {}
        runs = [p.to_run(client, entities_cache) for p in plans]
        parent_process = Run(client=client)
        for run in runs:
            parent_process.add_subprocess(run)

    execute_workflow(client=client, workflow=parent_process, output_paths=None)


@graph.command()
@click.argument("path", type=click.Path(exists=False, dir_okay=False))
@pass_local_client
def save(client, path):
    r"""Save dependency graph as PNG."""
    with measure("CREATE DEPENDENCY GRAPH"):
        dg = DependencyGraph.from_json(client.dependency_graph_path)
        dg.to_png(path=path)


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
        _rdf2dot_simple(rdf_graph, sys.stdout, graph=graph)
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


_FORMATS = {
    "dot": _dot,
    "dot-full": _dot_full,
    "dot-landscape": _dot_landscape,
    "dot-full-landscape": _dot_full_landscape,
    "dot-debug": _dot_debug,
    "json-ld": _json_ld,
}


@graph.command()
@click.option("--format", type=click.Choice(_FORMATS), default="json-ld", help="Choose an output format.")
@pass_local_client(requires_migration=False)
def log(client, format):
    r"""Equivalent of `renku log --format json-ld`."""
    pg = ProvenanceGraph.from_json(client.provenance_graph_path, lazy=True)

    _FORMATS[format](pg.rdf_graph)


def _get_modified_paths(client, plans_usages):
    modified = set()
    deleted = set()
    for plan_usage in plans_usages:
        _, path, checksum = plan_usage
        try:
            current_checksum = client.repo.git.rev_parse(f"HEAD:{str(path)}")
        except GitCommandError:
            deleted.add(plan_usage)
        else:
            if current_checksum != checksum:
                modified.add(plan_usage)

    return modified, deleted


def _include_dataset_metadata(dependency_graph, client):
    """Add dataset metadata to the provenance graph."""
    for commit in client.iter_commits(path=[".renku/datasets/*"]):
        for file_ in commit.diff(commit.parents or NULL_TREE):
            # Ignore deleted files (they appear as ADDED in this backwards diff)
            if file_.change_type == "A":
                continue

            path: str = file_.a_path

            if not path.startswith(".renku/datasets") or not path.endswith(".yaml"):
                continue

            dataset_file = _migrate_dataset(client=client, metadata_path=client.path / path)

            dependency_graph._graph.parse(location=str(dataset_file), format="json-ld")


def _migrate_dataset(client, metadata_path: Path):
    uuid = metadata_path.parent
    return client.path / ".renku" / "tmp" / uuid
