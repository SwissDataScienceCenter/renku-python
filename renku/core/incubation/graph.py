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
import shutil
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict
from urllib.parse import urlparse

from git import GitCommandError
from pkg_resources import resource_filename

from renku.core import errors
from renku.core.commands.update import execute_workflow
from renku.core.management import LocalClient
from renku.core.management.command_builder.command import Command, inject
from renku.core.management.config import RENKU_HOME
from renku.core.management.datasets import DatasetsApiMixin
from renku.core.management.migrate import migrate
from renku.core.management.repository import RepositoryApiMixin
from renku.core.metadata.database import Database
from renku.core.models.entities import Entity
from renku.core.models.jsonld import load_yaml
from renku.core.models.provenance.provenance_graph import ProvenanceGraph
from renku.core.models.workflow.dependency_graph import DependencyGraph
from renku.core.models.workflow.plan import Plan
from renku.core.models.workflow.run import Run
from renku.core.utils import communication
from renku.core.utils.contexts import measure
from renku.core.utils.migrate import MigrationType, read_project_version_from_yaml
from renku.core.utils.shacl import validate_graph

GRAPH_METADATA_PATHS = [
    Path(RENKU_HOME) / Path(RepositoryApiMixin.DATABASE_PATH),
    Path(RENKU_HOME) / Path(RepositoryApiMixin.DEPENDENCY_GRAPH),
    Path(RENKU_HOME) / Path(RepositoryApiMixin.PROVENANCE_GRAPH),
    Path(RENKU_HOME) / Path(DatasetsApiMixin.DATASETS_PROVENANCE),
]


def generate_graph():
    """Return a command for generating the graph."""
    command = Command().command(_generate_graph).lock_project().require_migration()
    return command.with_commit(commit_only=GRAPH_METADATA_PATHS)


@inject.autoparams("client")
def _generate_graph(client: LocalClient, force=False):
    """Generate graph and dataset provenance metadata."""
    from renku.core.management.migrations.m_0009__new_metadata_storage import generate_new_metadata

    try:
        generate_new_metadata(force=force, remove=False)
    except errors.OperationError:
        raise errors.OperationError("Graph metadata exists. Use ``--force`` to regenerate it.")


def status():
    """Return a command for getting workflow graph status."""
    return Command().command(_status).with_database(write=False)


@inject.autoparams()
def _status(client: LocalClient, database: Database):
    """Get status of workflows."""
    with measure("BUILD AND QUERY GRAPH"):
        pg = ProvenanceGraph.from_json(client.provenance_graph_path, lazy=True)
        plans_usages = pg.get_latest_plans_usages()

    if client.has_external_files():
        communication.warn(
            "Changes in external files are not detected automatically. To update external files run "
            "`renku dataset update -e`."
        )

    try:
        communication.echo("On branch {0}".format(client.repo.active_branch))
    except TypeError:
        communication.warn("Git HEAD is detached!\n Please move back to your working branch to use renku\n")

    with measure("CALCULATE MODIFIED"):
        modified, deleted = _get_modified_paths(plans_usages=plans_usages)

    if not modified and not deleted:
        return None, None, None

    stales = defaultdict(set)

    with measure("CALCULATE UPDATES"):
        for plan_id, path, _ in modified:
            paths = DependencyGraph.from_database(database).get_dependent_paths(plan_id, path)
            for p in paths:
                stales[p].add(path)

    modified = {v[1] for v in modified}

    return stales, modified, deleted


def update():
    """Return a command for generating the graph."""
    command = Command().command(_update).lock_project().with_database(write=True)
    return command.require_migration().require_clean().require_nodejs().with_commit(commit_if_empty=False)


@inject.autoparams()
def _update(dry_run, client: LocalClient, database: Database):
    """Update outdated outputs."""
    with measure("BUILD AND QUERY GRAPH"):
        pg = ProvenanceGraph.from_json(client.provenance_graph_path, lazy=True)
        plans_usages = pg.get_latest_plans_usages()

    with measure("CALCULATE MODIFIED"):
        modified, deleted = _get_modified_paths(plans_usages=plans_usages)

    if not modified:
        communication.echo("Everything is up-to-date.")
        return

    with measure("CALCULATE UPDATES"):
        plans, plans_with_deleted_inputs = DependencyGraph.from_database(database).get_downstream(modified, deleted)

    if plans_with_deleted_inputs:
        formatted_deleted_plans = "".join((f"\n\t{p}" for p in plans_with_deleted_inputs))
        communication.warn(
            f"The following steps cannot be executed because one of their inputs is deleted: {formatted_deleted_plans}"
        )

    if dry_run:
        formatted_plans = "".join((f"\n\t{p}" for p in plans))
        communication.echo(f"The following steps would be executed:{formatted_plans}")
        return

    with measure("CONVERTING RUNS"):
        entities_cache: Dict[str, Entity] = {}
        runs = [p.to_run(entities_cache) for p in plans]
        parent_process = Run()
        for run in runs:
            parent_process.add_subprocess(run)

    execute_workflow(workflow=parent_process, output_paths=None, command_name="update", update_commits=True)


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

    pg = ProvenanceGraph.from_json(client.provenance_graph_path, lazy=True)

    # TODO: Add dataset provenance to graph
    # if not workflows_only:
    #     pg.rdf_graph.parse(location=str(client.datasets_provenance_path), format="json-ld")

    graph = pg.rdf_graph

    if strict:
        if format == "jsonld":
            format = "json-ld"
        _validate_graph(graph, format)

    return FORMATS[format](graph)


@inject.autoparams()
def _get_modified_paths(plans_usages, client: LocalClient):
    """Get modified and deleted usages/inputs of a plan."""
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


@inject.autoparams()
def _fetch_datasets(revision, paths, deleted_paths, client: LocalClient):
    from renku.core.models.dataset import Dataset

    datasets_path = client.path / ".renku" / "tmp" / "datasets"
    shutil.rmtree(datasets_path, ignore_errors=True)
    datasets_path.mkdir(parents=True, exist_ok=True)

    def read_project_version():
        """Read project version at revision."""
        try:
            project_file_content = client.repo.git.show(f"{revision}:.renku/metadata.yml")
        except GitCommandError:  # Project metadata file does not exist
            return 1

        try:
            yaml_data = load_yaml(project_file_content)
            return int(read_project_version_from_yaml(yaml_data))
        except ValueError:
            return 1

    def copy_and_migrate_datasets():
        existing = []
        deleted = []

        for path in paths:
            rev = revision
            if path in deleted_paths:
                rev = client.find_previous_commit(path, revision=f"{revision}~")
            identifier = Path(path).parent.name
            new_path = datasets_path / identifier / "metadata.yml"
            new_path.parent.mkdir(parents=True, exist_ok=True)
            content = client.repo.git.show(f"{rev}:{path}")
            new_path.write_text(content)
            if path in deleted_paths:
                deleted.append(new_path)
            else:
                existing.append(new_path)

        try:
            project_version = read_project_version()
            client.set_temporary_datasets_path(datasets_path)
            communication.disable()
            client.migration_type = MigrationType.DATASETS
            migrate(project_version=project_version, skip_template_update=True, skip_docker_update=True)
        finally:
            communication.enable()
            client.clear_temporary_datasets_path()

        return existing, deleted

    paths, deleted_paths = copy_and_migrate_datasets()

    datasets = []
    for path in paths:
        dataset = Dataset.from_yaml(path, client)
        # NOTE: Fixing dataset path after migration
        initial_identifier = Path(dataset.path).name
        dataset.path = f".renku/datasets/{initial_identifier}"
        datasets.append(dataset)

    deleted_datasets = []
    for path in deleted_paths:
        dataset = Dataset.from_yaml(path, client)
        # NOTE: Fixing dataset path after migration
        initial_identifier = Path(dataset.path).name
        dataset.path = f".renku/datasets/{initial_identifier}"
        deleted_datasets.append(dataset)

    return datasets, deleted_datasets


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
    shacl_path = resource_filename("renku", "data/new_graph_shacl_shape.json")
    r, _, t = validate_graph(rdf_graph, shacl_path=shacl_path, format=format)

    if not r:
        raise errors.SHACLValidationError(f"{t}\nCouldn't export: Invalid Knowledge Graph data")


def remove_workflow():
    """Return a command for removing workflow."""
    command = Command().command(_remove_workflow).lock_project()
    return command.require_migration().with_database(write=True).with_commit(commit_only=GRAPH_METADATA_PATHS)


@inject.autoparams()
def _remove_workflow(name: str, force: bool, client: LocalClient, database: Database):
    """Remove the given workflow."""
    now = datetime.utcnow()
    # TODO: refactor this once we switch to Database
    provenance_graph = ProvenanceGraph.from_database(database)
    pg_workflows = unique_workflow(provenance_graph)

    not_found_text = f'The specified workflow is "{name}" is not an active workflow.'
    plan = None
    parse_result = urlparse(name)
    if parse_result.scheme:
        plan = next(filter(lambda x: x.id == name, pg_workflows.values()), None)
    if not plan and name not in pg_workflows:
        raise errors.ParameterError(not_found_text)

    if not force:
        prompt_text = f'You are about to remove the following workflow "{name}".' + "\n" + "\nDo you wish to continue?"
        communication.confirm(prompt_text, abort=True, warning=True)

    plan = plan or pg_workflows[name]
    # FIXME: Remove this once plans are made immutable
    plan._v_immutable = False
    plan.invalidated_at = now
    plan.freeze()
    dependency_graph = DependencyGraph.from_database(database)
    for p in dependency_graph.plans:
        if p.id == plan.id:
            # FIXME: Remove this once plans are made immutable
            p._v_immutable = False
            p.invalidated_at = now
            p.freeze()


def unique_workflow(provenance_graph: ProvenanceGraph) -> Dict[str, Plan]:
    """Map of unique plans in the provenance graph indexed by name."""
    workflows = dict()
    for activity in provenance_graph.activities:
        plan = activity.association.plan
        if plan.invalidated_at is None and plan.name not in workflows:
            workflows[plan.name] = plan
    return workflows
