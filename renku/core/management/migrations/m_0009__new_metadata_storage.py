# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Migrate old metadata to new storage format."""

import os
import shutil
import traceback
import uuid
from pathlib import Path, PurePosixPath
from typing import List, Optional, Union
from urllib.parse import urlparse

from git import NULL_TREE, Commit, GitCommandError

import renku.core.management.migrate
from renku.core import errors
from renku.core.management import LocalClient
from renku.core.management.command_builder import inject
from renku.core.management.config import RENKU_HOME
from renku.core.management.dataset.datasets_provenance import DatasetsProvenance
from renku.core.management.datasets import DatasetsApiMixin
from renku.core.management.interface.activity_gateway import IActivityGateway
from renku.core.management.interface.database_gateway import IDatabaseGateway
from renku.core.management.interface.project_gateway import IProjectGateway
from renku.core.management.migrations.models import v9 as old_schema
from renku.core.management.repository import RepositoryApiMixin
from renku.core.models.entity import Collection, Entity
from renku.core.models.jsonld import load_yaml
from renku.core.models.project import Project
from renku.core.models.provenance.activity import Activity, Association, Generation, Usage
from renku.core.models.provenance.agent import Person, SoftwareAgent
from renku.core.models.provenance.parameter import PathParameterValue, VariableParameterValue
from renku.core.models.workflow.parameter import CommandInput, CommandOutput, CommandParameter, MappedIOStream
from renku.core.models.workflow.plan import Plan
from renku.core.utils import communication
from renku.core.utils.git import get_object_hash
from renku.core.utils.metadata import convert_dataset
from renku.core.utils.migrate import OLD_METADATA_PATH, MigrationType, read_project_version_from_yaml
from renku.core.utils.scm import git_unicode_unescape

GRAPH_METADATA_PATHS = [
    Path(RENKU_HOME) / RepositoryApiMixin.DATABASE_PATH,
    Path(RENKU_HOME) / RepositoryApiMixin.DEPENDENCY_GRAPH,
    Path(RENKU_HOME) / RepositoryApiMixin.PROVENANCE_GRAPH,
    Path(RENKU_HOME) / DatasetsApiMixin.DATASETS_PROVENANCE,
]

NON_EXISTING_ENTITY_CHECKSUM = "0" * 40


def migrate(client):
    """Migration function."""
    committed = _commit_previous_changes(client)
    # TODO: set remove=True once the migration to the new metadata is finalized
    generate_new_metadata(remove=False, committed=committed)
    _remove_dataset_metadata_files(client)
    metadata_path = client.renku_path.joinpath(OLD_METADATA_PATH)
    metadata_path.unlink()


def _commit_previous_changes(client):
    client.repo.git.add(str(client.renku_path))

    staged_files = client.repo.index.diff("HEAD")
    if staged_files:
        project_path = client.renku_path.joinpath(OLD_METADATA_PATH)
        project = old_schema.Project.from_yaml(project_path, client)
        project.version = "8"
        project.to_yaml(client.renku_path.joinpath(project_path))

        client.repo.git.add(str(client.renku_path))

        client.repo.index.commit("renku migrate: committing structural changes", skip_hooks=True)
        return True

    return False


@inject.autoparams()
def maybe_migrate_project_to_database(client, project_gateway: IProjectGateway):
    """Migrate project to database if necessary."""
    metadata_path = client.renku_path.joinpath(OLD_METADATA_PATH)

    if metadata_path.exists():
        old_project = old_schema.Project.from_yaml(metadata_path)

        id_path = urlparse(old_project._id).path
        id_path = id_path.replace("/projects/", "")
        id_path = Path(id_path)
        namespace, name = str(id_path.parent), id_path.name
        id = Project.generate_id(namespace=namespace, name=name)

        new_project = Project(
            agent_version=old_project.agent_version,
            automated_update=old_project.automated_update,
            creator=_old_agent_to_new_agent(old_project.creator),
            date_created=old_project.created,
            id=id,
            immutable_template_files=old_project.immutable_template_files,
            name=old_project.name,
            template_id=old_project.template_id,
            template_metadata=old_project.template_metadata,
            template_ref=old_project.template_ref,
            template_source=old_project.template_source,
            template_version=old_project.template_version,
            version=old_project.version,
        )

        project_gateway.update_project(new_project)


@inject.autoparams()
def generate_new_metadata(
    client: LocalClient,
    database_gateway: IDatabaseGateway,
    activity_gateway: IActivityGateway,
    force=True,
    remove=True,
    committed=False,
):
    """Generate graph and dataset provenance metadata."""
    if force:
        client.remove_graph_files()
    elif client.has_graph_files():
        raise errors.OperationError("Graph metadata exists.")

    database_gateway.initialize()

    maybe_migrate_project_to_database(client)

    datasets_provenance = DatasetsProvenance()

    commits = list(
        client.repo.iter_commits(paths=[f"{client.workflow_path}/*.yaml", ".renku/datasets/*/*.yml"], reverse=True)
    )
    n_commits = len(commits)

    for n, commit in enumerate(commits, start=1):
        communication.echo(f"Processing commits {n}/{n_commits} {commit.hexsha}", end="\n")

        # NOTE: Treat the last commit differently if it was done by this migration
        is_last_commit = committed and n == n_commits

        try:
            # NOTE: Don't migrate workflows for dataset-only migrations
            if MigrationType.WORKFLOWS in client.migration_type:
                _process_workflows(client=client, activity_gateway=activity_gateway, commit=commit, remove=remove)
            _process_datasets(
                client=client, commit=commit, datasets_provenance=datasets_provenance, is_last_commit=is_last_commit
            )
        except errors.MigrationError:
            communication.echo("")
            communication.warn(f"Cannot process commit '{commit.hexsha}' - Migration failed: {traceback.format_exc()}")
        except Exception:
            communication.echo("")
            communication.warn(f"Cannot process commit '{commit.hexsha}' - Exception: {traceback.format_exc()}")

        # NOTE: Commit changes after each step
        database_gateway.commit()

    database_gateway.commit()


def _convert_run_to_plan(run: old_schema.Run) -> Plan:
    """Create a Plan from a Run."""
    assert not run.subprocesses, f"Cannot create a Plan from a Run with subprocesses: {run._id}"

    def extract_run_uuid(run_id: str) -> str:
        # https://localhost/runs/723fd784-9347-4081-84de-a6dbb067545b/
        return run_id.rstrip("/").rsplit("/", maxsplit=1)[-1]

    uuid = extract_run_uuid(run._id)
    plan_id = Plan.generate_id(uuid=uuid)

    def convert_argument(argument: old_schema.CommandArgument) -> CommandParameter:
        """Convert an old CommandArgument to a new CommandParameter."""
        assert isinstance(argument, old_schema.CommandArgument)

        return CommandParameter(
            default_value=argument.value,
            description=argument.description,
            id=CommandParameter.generate_id(plan_id=plan_id, postfix=PurePosixPath(argument._id).name),
            name=argument.name,
            position=argument.position,
            prefix=argument.prefix,
        )

    def convert_input(input: old_schema.CommandInput) -> CommandInput:
        """Convert an old CommandInput to a new CommandInput."""
        assert isinstance(input, old_schema.CommandInput)

        mapped_to = input.mapped_to
        if mapped_to:
            mapped_to = MappedIOStream(stream_type=mapped_to.stream_type)

        return CommandInput(
            default_value=input.consumes.path,
            description=input.description,
            id=CommandInput.generate_id(plan_id=plan_id, postfix=PurePosixPath(input._id).name),
            mapped_to=mapped_to,
            name=input.name,
            position=input.position,
            prefix=input.prefix,
        )

    def convert_output(output: old_schema.CommandOutput) -> CommandOutput:
        """Convert an old CommandOutput to a new CommandOutput."""
        assert isinstance(output, old_schema.CommandOutput)

        mapped_to = output.mapped_to
        if mapped_to:
            mapped_to = MappedIOStream(stream_type=mapped_to.stream_type)

        return CommandOutput(
            create_folder=output.create_folder,
            default_value=output.produces.path,
            description=output.description,
            id=CommandOutput.generate_id(plan_id=plan_id, postfix=PurePosixPath(output._id).name),
            mapped_to=mapped_to,
            name=output.name,
            position=output.position,
            prefix=output.prefix,
        )

    return Plan(
        command=run.command,
        description=run.description,
        id=plan_id,
        inputs=[convert_input(i) for i in run.inputs],
        keywords=run.keywords,
        name=run.name,
        outputs=[convert_output(o) for o in run.outputs],
        parameters=[convert_argument(a) for a in run.arguments],
        success_codes=run.successcodes,
    )


def _get_process_runs(workflow_run: old_schema.WorkflowRun) -> List[old_schema.ProcessRun]:
    # NOTE: Use Plan subprocesses to get activities because it is guaranteed to have correct order
    sorted_ids = [s.process._id for s in workflow_run.association.plan.subprocesses]
    activities = []
    # NOTE: it's possible to have subprocesses with similar ids but it does not matter since they have the same
    # plan
    # TODO: Remove these redundant subprocesses
    for id_ in sorted_ids:
        for s in workflow_run.subprocesses.values():
            if s.association.plan._id == id_:
                activities.append(s)
                break
    assert len(activities) == len(workflow_run.subprocesses)
    return activities


def _process_workflows(client: LocalClient, activity_gateway: IActivityGateway, commit: Commit, remove: bool):
    for file_ in commit.diff(commit.parents or NULL_TREE, paths=f"{client.workflow_path}/*.yaml"):
        # Ignore deleted files (they appear as ADDED in this backwards diff)
        if file_.change_type == "A":
            continue

        path: str = git_unicode_unescape(file_.a_path)

        if not path.startswith(".renku/workflow") or not path.endswith(".yaml"):
            continue

        if not (client.path / path).exists():
            communication.warn(f"Workflow file does not exists: '{path}'")
            continue

        workflow = old_schema.Activity.from_yaml(path=path, client=client)

        if isinstance(workflow, old_schema.ProcessRun):
            activities = [workflow]
        else:
            activities = _get_process_runs(workflow)

        for old_activity in activities:
            new_activity = _process_run_to_new_activity(old_activity, client=client)
            activity_gateway.add(new_activity)

        if remove:
            try:
                os.remove(file_.a_path)
            except FileNotFoundError:
                pass


def _process_run_to_new_activity(process_run: old_schema.ProcessRun, client: LocalClient) -> Activity:
    """Convert a ProcessRun to a new Activity."""
    assert not isinstance(process_run, old_schema.WorkflowRun)

    activity_id = Activity.generate_id()

    run = process_run.association.plan

    if run.subprocesses:
        assert len(run.subprocesses) == 1, f"Run in ProcessRun has multiple steps: {run._id}"
        run = run.subprocesses[0].process

    plan = _convert_run_to_plan(run)

    agents = [_old_agent_to_new_agent(a) for a in process_run.agents or []]
    association_agent = _old_agent_to_new_agent(process_run.association.agent)
    association = Association(agent=association_agent, id=Association.generate_id(activity_id), plan=plan)

    # NOTE: The same entity can have the same id during different times in its lifetime (e.g. different commit_sha,
    # but the same content). When it gets flattened, some fields will have multiple values which will cause an error
    # during deserialization. Make sure that no such Entity attributes exists (store those information in the
    # Generation object).

    invalidations = [_convert_invalidated_entity(e, client) for e in (process_run.invalidated or [])]
    generations = [_convert_generation(g, activity_id, client) for g in (process_run.generated or [])]
    usages = [_convert_usage(u, activity_id, client) for u in (process_run.qualified_usage or [])]

    parameters = _create_parameters(activity_id=activity_id, plan=plan, usages=usages, generations=generations)

    return Activity(
        agents=agents,
        annotations=process_run.annotations,
        association=association,
        ended_at_time=process_run.ended_at_time,
        generations=generations,
        id=activity_id,
        invalidations=invalidations,
        parameters=parameters,
        # project=process_run._project,
        started_at_time=process_run.started_at_time,
        usages=usages,
    )


def _convert_usage(usage: old_schema.Usage, activity_id: str, client) -> Usage:
    """Convert an old qualified Usage to a new one."""
    commit_sha = _extract_commit_sha(entity_id=usage.entity._id)
    entity = _convert_used_entity(usage.entity, commit_sha, activity_id, client)
    assert entity, f"Top entity was not found for Usage: {usage._id}, {usage.entity.path}"

    return Usage(id=Usage.generate_id(activity_id), entity=entity)


def _convert_generation(generation: old_schema.Generation, activity_id: str, client) -> Generation:
    """Convert an old Generation to a new one."""
    commit_sha = _extract_commit_sha(entity_id=generation.entity._id)
    entity = _convert_generated_entity(generation.entity, commit_sha, activity_id, client)
    assert entity, f"Root entity was not found for Generation: {generation._id}"

    return Generation(id=Generation.generate_id(activity_id), entity=entity)


def _convert_used_entity(entity: old_schema.Entity, revision: str, activity_id: str, client) -> Entity:
    """Convert an old Entity to one with proper metadata.

    For Collections, add members that are modified in the same commit or before the revision.
    """
    assert isinstance(entity, old_schema.Entity)

    checksum = get_object_hash(repo=client.repo, revision=revision, path=entity.path)
    if not checksum:
        communication.warn(f"Entity '{entity.path}' not found at '{revision}'")
        checksum = NON_EXISTING_ENTITY_CHECKSUM

    if isinstance(entity, old_schema.Collection):
        members = []
        for child in entity.members:
            new_child = _convert_used_entity(child, revision, activity_id, client)
            if not new_child:
                continue
            members.append(new_child)

        new_entity = Collection(checksum=checksum, path=entity.path, members=members)
    else:
        new_entity = Entity(checksum=checksum, path=entity.path)

    assert new_entity.__class__.__name__ == entity.__class__.__name__

    return new_entity


def _convert_generated_entity(entity: old_schema.Entity, revision: str, activity_id: str, client) -> Optional[Entity]:
    """Convert an Entity to one with proper metadata.

    For Collections, add members that are modified in the same commit as revision.
    """
    assert isinstance(entity, old_schema.Entity)

    try:
        entity_commit = client.find_previous_commit(paths=entity.path, revision=revision)
    except KeyError:
        return None
    if entity_commit.hexsha != revision:
        return None

    checksum = get_object_hash(repo=client.repo, revision=revision, path=entity.path)
    if not checksum:
        communication.warn(f"Entity '{entity.path}' not found at '{revision}'")
        checksum = NON_EXISTING_ENTITY_CHECKSUM

    if isinstance(entity, old_schema.Collection):
        members = []
        for child in entity.members:
            new_child = _convert_generated_entity(child, revision, activity_id, client)
            if not new_child:
                continue
            members.append(new_child)

        new_entity = Collection(checksum=checksum, path=entity.path, members=members)
    else:
        new_entity = Entity(checksum=checksum, path=entity.path)

    assert new_entity.__class__.__name__ == entity.__class__.__name__

    return new_entity


def _convert_invalidated_entity(entity: old_schema.Entity, client) -> Optional[Entity]:
    """Convert an Entity to one with proper metadata."""
    assert isinstance(entity, old_schema.Entity)
    assert not isinstance(entity, old_schema.Collection), f"Collection passed as invalidated: {entity._id}"

    commit_sha = _extract_commit_sha(entity_id=entity._id)
    commit = client.find_previous_commit(revision=commit_sha, paths=entity.path)
    revision = commit.hexsha
    checksum = get_object_hash(repo=client.repo, revision=revision, path=entity.path)
    if not checksum:
        # Entity was deleted at revision; get the one before it to have object_id
        checksum = get_object_hash(repo=client.repo, revision=f"{revision}~", path=entity.path)
        if not checksum:
            communication.warn(f"Entity '{entity.path}' not found at '{revision}'")
            checksum = NON_EXISTING_ENTITY_CHECKSUM

    new_entity = Entity(checksum=checksum, path=entity.path)

    assert new_entity.__class__.__name__ == entity.__class__.__name__

    return new_entity


def _extract_commit_sha(entity_id: str) -> str:
    # NOTE: extracts commit sha from ids like /blob/a3bf8a165dd56da078b96f2ca2ff22f14a3bdd57/input
    path = urlparse(entity_id).path
    assert path.startswith("/blob/"), f"Invalid entity identifier: {entity_id}"

    commit_sha = path[len("/blob/") :].split("/", 1)[0]
    assert len(commit_sha) == 40, f"Entity does not have valid commit SHA: {entity_id}"

    return commit_sha


def _create_parameters(activity_id, plan: Plan, usages: List[Usage], generations: List[Generation]):
    parameters = []

    inputs = {i.default_value: i for i in plan.inputs}
    for usage in usages:
        input = inputs.pop(usage.entity.path, None)
        assert input is not None, f"Cannot find usage path '{usage.entity.path}' in plan {plan.id}"
        id = PathParameterValue.generate_id(activity_id)
        parameters.append(PathParameterValue(id=id, parameter=input, path=usage.entity.path))

    assert not inputs, f"Not all inputs are converted: {inputs}"

    outputs = {o.default_value: o for o in plan.outputs}
    for generation in generations:
        output = outputs.pop(generation.entity.path, None)
        assert output is not None, f"Cannot find generation path '{generation.entity.path}' in plan {plan.id}"
        id = PathParameterValue.generate_id(activity_id)
        parameters.append(PathParameterValue(id=id, parameter=output, path=generation.entity.path))

    assert not outputs, f"Not all outputs are converted: {outputs}"

    for parameter in plan.parameters:
        id = VariableParameterValue.generate_id(activity_id)
        parameters.append(VariableParameterValue(id=id, parameter=parameter, value=parameter.default_value))

    return parameters


def _old_agent_to_new_agent(
    agent: Optional[Union[old_schema.Person, old_schema.SoftwareAgent]]
) -> Optional[Union[Person, SoftwareAgent]]:
    """Create an instance from Person/SoftwareAgent."""
    if isinstance(agent, old_schema.SoftwareAgent):
        return SoftwareAgent(id=agent.id, name=agent.label)

    assert not agent or isinstance(agent, old_schema.Person), f"Invalid type {type(agent)}"
    return Person(
        affiliation=agent.affiliation,
        alternate_name=agent.alternate_name,
        email=agent.email,
        id=None,
        name=agent.name,
    )


def _process_datasets(client: LocalClient, commit: Commit, datasets_provenance: DatasetsProvenance, is_last_commit):
    files_diff = list(commit.diff(commit.parents or NULL_TREE, paths=".renku/datasets/*/*.yml"))
    paths = [git_unicode_unescape(f.a_path) for f in files_diff]
    paths = [p for p in paths if len(Path(p).parents) == 4]  # Exclude files that are not in the right place
    deleted_paths = [git_unicode_unescape(f.a_path) for f in files_diff if f.change_type == "A"]

    datasets, deleted_datasets = _fetch_datasets(
        client=client, revision=commit.hexsha, paths=paths, deleted_paths=deleted_paths
    )

    revision = commit.hexsha
    date = commit.authored_datetime

    for dataset in datasets:
        dataset, tags = convert_dataset(dataset=dataset, client=client, revision=revision)
        if is_last_commit:
            datasets_provenance.update_during_migration(
                dataset, commit_sha=revision, date=date, tags=tags, replace=True
            )
        else:
            datasets_provenance.update_during_migration(dataset, commit_sha=revision, date=date, tags=tags)
    for dataset in deleted_datasets:
        dataset, _ = convert_dataset(dataset=dataset, client=client, revision=revision)
        datasets_provenance.update_during_migration(dataset, commit_sha=revision, date=date, remove=True)


def _fetch_datasets(client: LocalClient, revision: str, paths: List[str], deleted_paths: List[str]):
    from renku.core.management.migrations.models.v9 import Dataset

    datasets_path = client.path / ".renku" / "tmp" / "datasets"
    shutil.rmtree(datasets_path, ignore_errors=True)
    datasets_path.mkdir(parents=True, exist_ok=True)

    client.dataset_migration_context = _DatasetMigrationContext(client=client, revision=revision)

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

    def get_dataset_identifier(path):
        identifier = Path(path).parent.name
        try:
            identifier = str(uuid.UUID(identifier))
        except ValueError:
            identifier = identifier.rsplit("%2F", maxsplit=1)[-1]
            try:
                identifier = str(uuid.UUID(identifier))
            except ValueError:
                return
            else:
                return identifier
        else:
            return identifier

    def copy_and_migrate_datasets():
        existing = []
        deleted = []

        for path in paths:
            rev = revision
            if path in deleted_paths:
                rev = client.find_previous_commit(path, revision=f"{revision}~")
            identifier = get_dataset_identifier(path)
            if not identifier:
                continue
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
            previous_migration_type = client.migration_type
            client.migration_type = MigrationType.DATASETS
            renku.core.management.migrate.migrate(
                project_version=project_version,
                skip_template_update=True,
                skip_docker_update=True,
                max_version=8,
            )
            client.migration_type = previous_migration_type
        finally:
            communication.enable()
            client.clear_temporary_datasets_path()

        return existing, deleted

    paths, deleted_paths = copy_and_migrate_datasets()

    datasets = []
    for metadata_path in paths:
        dataset = Dataset.from_yaml(metadata_path, client)
        # NOTE: Fixing dataset path after migration
        initial_identifier = Path(dataset.path).name
        dataset.path = f".renku/datasets/{initial_identifier}"
        datasets.append(dataset)

    deleted_datasets = []
    for metadata_path in deleted_paths:
        dataset = Dataset.from_yaml(metadata_path, client)
        # NOTE: Fixing dataset path after migration
        initial_identifier = Path(dataset.path).name
        dataset.path = f".renku/datasets/{initial_identifier}"
        deleted_datasets.append(dataset)

    return datasets, deleted_datasets


class _DatasetMigrationContext:
    def __init__(self, client, revision):
        self.client = client
        self.revision = revision

    def exists(self, path) -> bool:
        try:
            self.client.repo.git.cat_file("-e", f"{self.revision}:{str(path)}")
        except GitCommandError:
            return False
        else:
            return True

    def is_dir(self, path) -> bool:
        try:
            result = self.client.repo.git.cat_file("-t", f"{self.revision}:{str(path)}")
        except GitCommandError:
            return False
        else:
            return "tree" in result

    def find_previous_commit(self, path):
        return self.client.find_previous_commit(path, revision=self.revision)


def _remove_dataset_metadata_files(client: LocalClient):
    """Remove old dataset metadata."""
    try:
        shutil.rmtree(os.path.join(client.renku_path, client.DATASETS))
    except FileNotFoundError:
        pass
    try:
        shutil.rmtree(os.path.join(client.renku_path, "refs", client.DATASETS))
    except FileNotFoundError:
        pass
