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
from pathlib import Path
from typing import Dict

import git
from git import NULL_TREE, GitCommandError
from pkg_resources import resource_filename

from renku.core import errors
from renku.core.commands.dataset import create_dataset_helper
from renku.core.commands.update import execute_workflow
from renku.core.incubation.command import Command
from renku.core.management.config import RENKU_HOME
from renku.core.management.datasets import DATASET_METADATA_PATHS, DatasetsApiMixin
from renku.core.management.migrate import migrate
from renku.core.management.repository import RepositoryApiMixin
from renku.core.models.entities import Entity
from renku.core.models.jsonld import load_yaml
from renku.core.models.provenance.activities import Activity as ActivityRun
from renku.core.models.provenance.activity import ActivityCollection
from renku.core.models.provenance.provenance_graph import ProvenanceGraph
from renku.core.models.workflow.run import Run
from renku.core.utils import communication
from renku.core.utils.contexts import measure
from renku.core.utils.migrate import read_project_version_from_yaml
from renku.core.utils.scm import git_unicode_unescape
from renku.core.utils.shacl import validate_graph

GRAPH_METADATA_PATHS = [
    Path(RENKU_HOME) / Path(RepositoryApiMixin.DEPENDENCY_GRAPH),
    Path(RENKU_HOME) / Path(RepositoryApiMixin.PROVENANCE_GRAPH),
    Path(RENKU_HOME) / Path(DatasetsApiMixin.DATASETS_PROVENANCE),
]


def generate_graph():
    """Return a command for generating the graph."""
    command = Command().command(_generate_graph).lock_project()
    return command.require_migration().with_commit(commit_only=GRAPH_METADATA_PATHS)


def _generate_graph(client, force):
    """Generate graph metadata."""

    def process_workflows(commit, provenance_graph):
        for file_ in commit.diff(commit.parents or NULL_TREE, paths=f"{client.workflow_path}/*.yaml"):
            # Ignore deleted files (they appear as ADDED in this backwards diff)
            if file_.change_type == "A":
                continue

            path: str = git_unicode_unescape(file_.a_path)

            if not path.startswith(".renku/workflow") or not path.endswith(".yaml"):
                continue

            workflow = ActivityRun.from_yaml(path=path, client=client)
            activity_collection = ActivityCollection.from_activity_run(workflow, client.dependency_graph, client)

            provenance_graph.add(activity_collection)

    def process_datasets(commit):
        files_diff = list(commit.diff(commit.parents or NULL_TREE, paths=".renku/datasets/*/*.yml"))
        paths = [git_unicode_unescape(f.a_path) for f in files_diff]
        deleted_paths = [git_unicode_unescape(f.a_path) for f in files_diff if f.change_type == "A"]

        datasets, deleted_datasets = _fetch_datasets(client, commit.hexsha, paths=paths, deleted_paths=deleted_paths)

        revision = commit.hexsha
        date = commit.authored_datetime

        for dataset in datasets:
            client.datasets_provenance.update_dataset(dataset, client=client, revision=revision, date=date)
        for dataset in deleted_datasets:
            client.datasets_provenance.remove_dataset(dataset, client=client, revision=revision, date=date)

    commits = list(client.repo.iter_commits(paths=[f"{client.workflow_path}/*.yaml", ".renku/datasets/*/*.yml"]))
    n_commits = len(commits)
    commits = reversed(commits)

    if force:
        client.remove_graph_files()
        client.remove_datasets_provenance_file()
    elif client.has_graph_files() or client.has_datasets_provenance():
        raise errors.OperationError("Graph metadata exists. Use ``--force`` to regenerate it.")

    client.initialize_graph()
    client.initialize_datasets_provenance()

    provenance_graph = ProvenanceGraph.from_json(client.provenance_graph_path)

    for n, commit in enumerate(commits, start=1):
        communication.echo(f"Processing commits {n}/{n_commits}", end="\r")

        process_workflows(commit, provenance_graph)
        process_datasets(commit)

    client.dependency_graph.to_json()
    provenance_graph.to_json()
    client.datasets_provenance.to_json()


def status():
    """Return a command for getting workflow graph status."""
    return Command().command(_status)


def _status(client):
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
        modified, deleted = _get_modified_paths(client=client, plans_usages=plans_usages)

    if not modified and not deleted:
        return None, None, None

    stales = defaultdict(set)

    with measure("CALCULATE UPDATES"):
        for plan_id, path, _ in modified:
            paths = client.dependency_graph.get_dependent_paths(plan_id, path)
            for p in paths:
                stales[p].add(path)

    modified = {v[1] for v in modified}

    return stales, modified, deleted


def update():
    """Return a command for generating the graph."""
    command = Command().command(_update).lock_project()
    return command.require_migration().with_commit(commit_if_empty=False).require_clean()


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
        plans, plans_with_deleted_inputs = client.dependency_graph.get_downstream(modified, deleted)

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


def export_graph():
    """Return a command for exporting graph data."""
    return Command().command(_export_graph)


def _export_graph(client, format, workflows_only, strict):
    """Output graph in specific format."""
    if not client.provenance_graph_path.exists():
        raise errors.ParameterError("Graph is not generated.")

    pg = ProvenanceGraph.from_json(client.provenance_graph_path, lazy=True)
    format = format.lower()
    if strict and format not in ["json-ld", "jsonld"]:
        raise errors.SHACLValidationError(f"'--strict' not supported for '{format}'")

    pg = ProvenanceGraph.from_json(client.provenance_graph_path, lazy=True)

    if not workflows_only:
        pg.rdf_graph.parse(location=str(client.datasets_provenance_path), format="json-ld")

    graph = pg.rdf_graph

    if strict:
        if format == "jsonld":
            format = "json-ld"
        _validate_graph(graph, format)

    return FORMATS[format](graph)


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


def create_dataset():
    """Return a command for creating an empty dataset in the current repo."""
    command = Command().command(_create_dataset).lock_dataset()
    return command.require_migration().with_commit(commit_only=DATASET_METADATA_PATHS)


def _create_dataset(client, name, title=None, description="", creators=None, keywords=None):
    """Create a dataset in the repository."""
    if not client.has_datasets_provenance():
        raise errors.OperationError("Dataset provenance is not generated. Run `renku graph generate-dataset`.")

    return create_dataset_helper(
        client=client, name=name, title=title, description=description, creators=creators, keywords=keywords
    )


def add_to_dataset():
    """Return a command for adding data to a dataset."""
    command = Command().command(_add_to_dataset).lock_dataset()
    return command.require_migration().with_commit(raise_if_empty=True, commit_only=DATASET_METADATA_PATHS)


def _add_to_dataset(
    client,
    urls,
    name,
    external=False,
    force=False,
    overwrite=False,
    create=False,
    sources=(),
    destination="",
    ref=None,
):
    """Add data to a dataset."""
    if not client.has_datasets_provenance():
        raise errors.OperationError("Dataset provenance is not generated. Run `renku graph generate-dataset`.")

    if len(urls) == 0:
        raise errors.UsageError("No URL is specified")
    if sources and len(urls) > 1:
        raise errors.UsageError("Cannot use `--source` with multiple URLs.")

    try:
        with client.with_dataset_provenance(name=name, create=create) as dataset:
            client.add_data_to_dataset(
                dataset,
                urls=urls,
                external=external,
                force=force,
                overwrite=overwrite,
                sources=sources,
                destination=destination,
                ref=ref,
            )

        client.update_datasets_provenance(dataset)
    except errors.DatasetNotFound:
        raise errors.DatasetNotFound(
            message=f"Dataset `{name}` does not exist.\nUse `renku dataset create {name}` to create the dataset or "
            f"retry with `--create` option for automatic dataset creation."
        )
    except (FileNotFoundError, git.exc.NoSuchPathError) as e:
        message = "\n\t".join(urls)
        raise errors.ParameterError(f"Could not find paths/URLs: \n\t{message}") from e
