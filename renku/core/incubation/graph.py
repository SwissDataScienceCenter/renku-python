# -*- coding: utf-8 -*-
#
# Copyright 2018-2020 - Swiss Data Science Center (SDSC)
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

import shutil
from collections import defaultdict
from pathlib import Path
from typing import Dict

from git import NULL_TREE, GitCommandError

from renku.core import errors
from renku.core.commands.update import execute_workflow
from renku.core.incubation.command import Command
from renku.core.management.config import RENKU_HOME
from renku.core.management.datasets import DatasetsApiMixin
from renku.core.management.migrate import migrate
from renku.core.management.repository import RepositoryApiMixin
from renku.core.models.entities import Entity
from renku.core.models.jsonld import load_yaml
from renku.core.models.provenance.activities import Activity as ActivityRun
from renku.core.models.provenance.activity import ActivityCollection
from renku.core.models.provenance.provenance_graph import ProvenanceGraph
from renku.core.models.workflow.dependency_graph import DependencyGraph
from renku.core.models.workflow.run import Run
from renku.core.utils import communication
from renku.core.utils.contexts import measure
from renku.core.utils.migrate import read_project_version_from_yaml
from renku.core.utils.scm import git_unicode_unescape

GRAPH_METADATA_PATHS = [
    Path(RENKU_HOME) / Path(RepositoryApiMixin.DEPENDENCY_GRAPH),
    Path(RENKU_HOME) / Path(RepositoryApiMixin.PROVENANCE_GRAPH),
    Path(RENKU_HOME) / Path(DatasetsApiMixin.DATASETS_PROVENANCE),
]


def _generate_graph(client, force):
    """Generate graph metadata."""

    def create_empty_graph_files():
        # Create empty graph files as defaults
        client.dependency_graph_path.write_text("[]")
        client.provenance_graph_path.write_text("[]")

    commits = list(client.repo.iter_commits(paths=f"{client.workflow_path}*.yaml"))
    n_commits = len(commits)
    commits = reversed(commits)

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
        if client.has_graph_files():
            raise errors.OperationError("Graph files exist. Use ``--force`` to regenerate the graph.")

    create_empty_graph_files()

    dependency_graph = DependencyGraph.from_json(client.dependency_graph_path)
    provenance_graph = ProvenanceGraph.from_json(client.provenance_graph_path)

    for n, commit in enumerate(commits, start=1):
        communication.echo(f"\rProcessing commits {n}/{n_commits}\r")

        for file_ in commit.diff(commit.parents or NULL_TREE, paths=f"{client.workflow_path}*.yaml"):
            # Ignore deleted files (they appear as ADDED in this backwards diff)
            if file_.change_type == "A":
                continue

            path: str = git_unicode_unescape(file_.a_path)

            if not path.startswith(".renku/workflow") or not path.endswith(".yaml"):
                continue

            workflow = ActivityRun.from_yaml(path=path, client=client)
            activity_collection = ActivityCollection.from_activity_run(workflow, dependency_graph, client)

            provenance_graph.add(activity_collection)

    dependency_graph.to_json()
    provenance_graph.to_json()


def generate_graph():
    """Return a command for generating the graph."""
    command = Command().command(_generate_graph).lock_project()
    return command.require_migration().with_commit(commit_only=GRAPH_METADATA_PATHS)


def _status(client):
    """Get status of workflows."""
    with measure("BUILD AND QUERY GRAPH"):
        pg = ProvenanceGraph.from_json(client.provenance_graph_path, lazy=True)
        plans_usages = pg.get_latest_plans_usages()

    if client.has_external_files():
        communication.warn(
            "Changes in external files are not detected automatically. To "
            'update external files run "renku dataset update -e".'
        )

    try:
        communication.echo("On branch {0}".format(client.repo.active_branch))
    except TypeError:
        communication.warn("Git HEAD is detached!\n Please move back to your working branch to use renku\n")

    with measure("CALCULATE MODIFIED"):
        modified, deleted = _get_modified_paths(client=client, plans_usages=plans_usages)

    if not modified and not deleted:
        return None, None, None

    stales = defaultdict(set)

    with measure("CALCULATE UPDATES"):
        dg = DependencyGraph.from_json(client.dependency_graph_path)
        for plan_id, path, _ in modified:
            paths = dg.get_dependent_paths(plan_id, path)
            for p in paths:
                stales[p].add(path)

    modified = {v[1] for v in modified}

    return stales, modified, deleted


def status():
    """Return a command for getting workflow graph status."""
    return Command().command(_status)


def _update(client, dry_run):
    """Update outdated outputs."""
    with measure("BUILD AND QUERY GRAPH"):
        pg = ProvenanceGraph.from_json(client.provenance_graph_path, lazy=True)
        plans_usages = pg.get_latest_plans_usages()

    with measure("CALCULATE MODIFIED"):
        modified, deleted = _get_modified_paths(client=client, plans_usages=plans_usages)

    if not modified:
        communication.echo("Everything is up-to-date.")
        return

    with measure("CALCULATE UPDATES"):
        dg = DependencyGraph.from_json(client.dependency_graph_path)
        plans, plans_with_deleted_inputs = dg.get_downstream(modified, deleted)

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
        runs = [p.to_run(client, entities_cache) for p in plans]
        parent_process = Run(client=client)
        for run in runs:
            parent_process.add_subprocess(run)

    execute_workflow(
        client=client, workflow=parent_process, output_paths=None, command_name="update", update_commits=True
    )


def update():
    """Return a command for generating the graph."""
    command = Command().command(_update).lock_project()
    return command.require_migration().with_commit(commit_if_empty=False).require_clean()


def _export_graph(client, format, dataset):
    """Output graph in specific format."""
    if not client.provenance_graph_path.exists():
        raise errors.ParameterError("Graph is not generated.")

    pg = ProvenanceGraph.from_json(client.provenance_graph_path, lazy=True)

    if dataset:
        if not client.datasets_provenance_path.exists():
            raise errors.ParameterError("Dataset provenance is not generated.")
        pg.rdf_graph.parse(location=str(client.datasets_provenance_path), format="json-ld")

    return format(pg.rdf_graph)


def export_graph():
    """Return a command for exporting graph data."""
    return Command().command(_export_graph)


def _get_modified_paths(client, plans_usages):
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


def _generate_datasets_provenance(client, force=False):
    """Generate datasets provenance metadata."""
    commits = list(client.repo.iter_commits(paths=".renku/datasets/*"))
    n_commits = len(commits)
    commits = reversed(commits)

    if force:
        try:
            client.datasets_provenance_path.unlink()
        except FileNotFoundError:
            pass
    else:
        if client.datasets_provenance_path.exists():
            raise errors.OperationError("Dataset provenance file exists. Use ``--force`` to regenerate it.")

    # Create empty dataset provenance file
    client.datasets_provenance_path.write_text("[]")

    datasets_provenance = client.datasets_provenance

    for n, commit in enumerate(commits, start=1):
        communication.echo(f"\rProcessing commits {n}/{n_commits}\r")

        files_diff = list(commit.diff(commit.parents or NULL_TREE, paths=".renku/datasets/*"))
        paths = [git_unicode_unescape(f.a_path) for f in files_diff]
        deleted_paths = [git_unicode_unescape(f.a_path) for f in files_diff if f.change_type == "A"]

        datasets, deleted_datasets = _fetch_datasets(client, commit.hexsha, paths=paths, deleted_paths=deleted_paths)

        revision = commit.hexsha
        date = commit.authored_datetime

        for dataset in datasets:
            datasets_provenance.update_dataset(dataset, client=client, revision=revision, date=date)
        for dataset in deleted_datasets:
            datasets_provenance.remove_dataset(dataset, client=client, revision=revision, date=date)

    datasets_provenance.to_json()


def _fetch_datasets(client, revision, paths, deleted_paths):
    from renku.core.models.datasets import Dataset

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
            migrate(client, project_version=project_version, skip_template_update=True, skip_docker_update=True)
        finally:
            client.clear_temporary_datasets_path()

        return existing, deleted

    paths, deleted_paths = copy_and_migrate_datasets()

    datasets = []
    for path in paths:
        dataset = Dataset.from_yaml(path, client=client)
        # NOTE: Fixing dataset path after migration
        original_identifier = Path(dataset.path).name
        dataset.path = f".renku/datasets/{original_identifier}"
        datasets.append(dataset)

    deleted_datasets = []
    for path in deleted_paths:
        dataset = Dataset.from_yaml(path, client=client)
        # NOTE: Fixing dataset path after migration
        original_identifier = Path(dataset.path).name
        dataset.path = f".renku/datasets/{original_identifier}"
        deleted_datasets.append(dataset)

    return datasets, deleted_datasets


def generate_datasets_provenance():
    """Return a command for generating dataset provenance."""
    command = Command().command(_generate_datasets_provenance).lock_project()
    return command.require_migration().with_commit(commit_only=GRAPH_METADATA_PATHS)
