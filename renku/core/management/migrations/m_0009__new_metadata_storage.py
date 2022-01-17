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
from collections import defaultdict
from hashlib import sha1
from itertools import chain
from pathlib import Path, PurePosixPath
from typing import List, Optional, Union
from urllib.parse import urlparse

import renku.core.management.migrate
from renku.core import errors
from renku.core.management.client import LocalClient
from renku.core.management.command_builder import inject
from renku.core.management.dataset.datasets_provenance import DatasetsProvenance
from renku.core.management.interface.activity_gateway import IActivityGateway
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.interface.database_gateway import IDatabaseGateway
from renku.core.management.interface.project_gateway import IProjectGateway
from renku.core.management.migrations.models import v9 as old_schema
from renku.core.management.migrations.utils import (
    OLD_DATASETS_PATH,
    OLD_METADATA_PATH,
    MigrationType,
    read_project_version_from_yaml,
    set_temporary_datasets_path,
    unset_temporary_datasets_path,
)
from renku.core.management.migrations.utils.conversion import convert_dataset
from renku.core.metadata.repository import Commit
from renku.core.models.entity import Collection, Entity
from renku.core.models.jsonld import load_yaml
from renku.core.models.project import Project
from renku.core.models.provenance.activity import Activity, Association, Generation, Usage
from renku.core.models.provenance.agent import Person, SoftwareAgent
from renku.core.models.provenance.parameter import ParameterValue
from renku.core.models.workflow.parameter import (
    DIRECTORY_MIME_TYPE,
    CommandInput,
    CommandOutput,
    CommandParameter,
    MappedIOStream,
)
from renku.core.models.workflow.plan import Plan
from renku.core.utils import communication

NON_EXISTING_ENTITY_CHECKSUM = "0" * 40

PLAN_CACHE = {}


def migrate(migration_context):
    """Migration function."""
    client = migration_context.client
    committed = _commit_previous_changes(client)
    # NOTE: Initialize submodules
    _ = client.repository.submodules
    _generate_new_metadata(
        committed=committed,
        strict=migration_context.options.strict,
        migration_type=migration_context.options.type,
        preserve_identifiers=migration_context.options.preserve_identifiers,
    )
    _remove_dataset_metadata_files(client)

    metadata_path = client.renku_path.joinpath(OLD_METADATA_PATH)
    with open(metadata_path, "w") as f:
        f.write(
            "# Dummy file kept for backwards compatibility, does not contain actual version\n"
            "'http://schema.org/schemaVersion': '9'"
        )


def _commit_previous_changes(client):
    if client.repository.is_dirty():
        project_path = client.renku_path.joinpath(OLD_METADATA_PATH)
        project = old_schema.Project.from_yaml(project_path, client)
        project.version = "8"
        project.to_yaml(client.renku_path.joinpath(project_path))

        client.repository.add(client.renku_path)
        client.repository.commit("renku migrate: committing structural changes" + client.transaction_id, no_verify=True)
        return True

    return False


@inject.autoparams()
def maybe_migrate_project_to_database(client, project_gateway: IProjectGateway):
    """Migrate project to database if necessary."""
    metadata_path = client.renku_path.joinpath(OLD_METADATA_PATH)

    if metadata_path.exists():
        old_project = old_schema.Project.from_yaml(metadata_path, client=client)

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


def remove_graph_files(client):
    """Remove all graph files."""
    # NOTE: These are required for projects that have new graph files
    try:
        (client.path / "provenance.json").unlink()
    except FileNotFoundError:
        pass
    try:
        (client.path / "dependency.json").unlink()
    except FileNotFoundError:
        pass
    try:
        shutil.rmtree(client.database_path)
    except FileNotFoundError:
        pass
    try:
        (client.path / "dataset.json").unlink()
    except FileNotFoundError:
        pass


@inject.autoparams()
def _generate_new_metadata(
    strict,
    migration_type: MigrationType,
    client_dispatcher: IClientDispatcher,
    database_gateway: IDatabaseGateway,
    activity_gateway: IActivityGateway,
    force=True,
    remove=True,
    committed=False,
    preserve_identifiers=False,
):
    """Generate graph and dataset provenance metadata."""
    client = client_dispatcher.current_client

    if force:
        remove_graph_files(client)
    elif client.has_graph_files():
        raise errors.OperationError("Graph metadata exists.")

    database_gateway.initialize()

    maybe_migrate_project_to_database(client)

    datasets_provenance = DatasetsProvenance()

    commits = list(
        client.repository.iterate_commits(
            f"{client.renku_path}/workflow/*.yaml", ".renku/datasets/*/*.yml", reverse=True
        )
    )
    n_commits = len(commits)

    for n, commit in enumerate(commits, start=1):
        communication.echo(f"Processing commits {n}/{n_commits} {commit.hexsha}", end="\r")

        # NOTE: Treat the last commit differently if it was done by this migration
        is_last_commit = committed and n == n_commits

        try:
            # NOTE: Don't migrate workflows for dataset-only migrations
            if MigrationType.WORKFLOWS in migration_type:
                _process_workflows(activity_gateway=activity_gateway, commit=commit, remove=remove, client=client)
            _process_datasets(
                client=client,
                commit=commit,
                datasets_provenance=datasets_provenance,
                is_last_commit=is_last_commit,
                preserve_identifiers=preserve_identifiers,
            )
        except errors.MigrationError:
            if strict:
                raise
            communication.echo("")
            communication.warn(f"Cannot process commit '{commit.hexsha}' - Migration failed: {traceback.format_exc()}")
        except Exception:
            if strict:
                raise
            communication.echo("")
            communication.warn(f"Cannot process commit '{commit.hexsha}' - Exception: {traceback.format_exc()}")

        # NOTE: Commit changes after each step
        database_gateway.commit()

    database_gateway.commit()


def _convert_run_to_plan(run: old_schema.Run, project_id) -> Plan:
    """Create a Plan from a Run."""
    assert not run.subprocesses, f"Cannot create a Plan from a Run with subprocesses: {run._id}"

    def extract_run_uuid(run_id: str) -> str:
        # https://localhost/runs/723fd784-9347-4081-84de-a6dbb067545b/
        return run_id.rstrip("/").rsplit("/", maxsplit=1)[-1]

    uuid = extract_run_uuid(run._id)

    if uuid in PLAN_CACHE:
        return PLAN_CACHE[uuid]

    plan_id = Plan.generate_id(uuid=uuid)

    def get_mime_type(entity: Union[old_schema.Entity, old_schema.Collection]) -> List[str]:
        return [DIRECTORY_MIME_TYPE] if isinstance(entity, old_schema.Collection) else ["application/octet-stream"]

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
            postfix=PurePosixPath(argument._id).name,
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
            encoding_format=get_mime_type(input.consumes),
            postfix=PurePosixPath(input._id).name,
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
            encoding_format=get_mime_type(output.produces),
            postfix=PurePosixPath(output._id).name,
        )

    plan = Plan(
        command=run.command,
        description=run.description,
        id=plan_id,
        inputs=[convert_input(i) for i in run.inputs],
        keywords=run.keywords,
        name=run.name,
        outputs=[convert_output(o) for o in run.outputs],
        parameters=[convert_argument(a) for a in run.arguments],
        project_id=project_id,
        success_codes=run.successcodes,
    )

    PLAN_CACHE[uuid] = plan

    return plan


def _get_process_runs(workflow_run: old_schema.WorkflowRun) -> List[old_schema.ProcessRun]:
    # NOTE: Use Plan subprocesses to get activities because it is guaranteed to have correct order
    assert (
        workflow_run.association.plan.subprocesses
    ), "Can only get subprocesses if the associated plan has subprocesses"
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

    for file in commit.get_changes(paths=f"{client.renku_path}/workflow/*.yaml"):
        if file.deleted:
            continue

        path: str = file.b_path

        if not path.startswith(".renku/workflow") or not path.endswith(".yaml"):
            continue

        if not (client.path / path).exists():
            communication.warn(f"Workflow file does not exists: '{path}'")
            continue

        workflow = old_schema.Activity.from_yaml(path=path, client=client)

        if isinstance(workflow, old_schema.WorkflowRun):
            activities = _get_process_runs(workflow)
        else:
            activities = [workflow]

        for old_activity in activities:
            new_activities = _process_run_to_new_activity(process_run=old_activity, client=client)
            for new_activity in new_activities:
                activity_gateway.add(new_activity)

        if remove:
            try:
                os.remove(file.b_path)
            except FileNotFoundError:
                pass


def _process_run_to_new_activity(process_run: old_schema.ProcessRun, client: LocalClient) -> List[Activity]:
    """Convert a ProcessRun to a new Activity."""

    def generate_activity_id(process_run_id: str, suffix: str = None) -> str:
        # https://localhost/activities/commit/b092dca8866c940dd4ff119b7d76d163a1be6a65
        # https://localhost/activities/commit/b092dca8866c940dd4ff119b7d76d163a1be6a65/steps/step_1
        parts = process_run_id.rstrip("/").rsplit("/", maxsplit=3)

        if parts[-2] == "steps":
            parts = parts[-3:]
        else:
            parts = [parts[-1]]

        original_id = "/".join(parts)

        if suffix:
            original_id += suffix

        return sha1(original_id.encode("utf-8")).hexdigest()

    assert not isinstance(process_run, old_schema.WorkflowRun)

    project_id = client.project.id

    run = process_run.association.plan

    if run.subprocesses:
        runs = [s.process for s in run.subprocesses]
    else:
        runs = [run]

    activities = []
    for i, run in enumerate(runs):
        activity_id = generate_activity_id(process_run._id, suffix=str(i) if i else None)

        plan = _convert_run_to_plan(run, project_id=project_id)

        agents = [_old_agent_to_new_agent(a) for a in process_run.agents or []]
        association_agent = _old_agent_to_new_agent(process_run.association.agent)
        association = Association(agent=association_agent, id=Association.generate_id(activity_id), plan=plan)

        # NOTE: The same entity can have the same id during different times in its lifetime (e.g. different commit_sha,
        # but the same content). When it gets flattened, some fields will have multiple values which will cause an error
        # during deserialization. Make sure that no such Entity attributes exists (store those information in the
        # Generation object).

        invalidations = [_convert_invalidated_entity(e, client) for e in (process_run.invalidated or [])]

        generations = []

        for potential_generation in chain(process_run.generated or [], process_run.qualified_usage or []):
            if all(output.produces.path != potential_generation.entity.path for output in run.outputs):
                continue

            generations.append(_convert_generation(potential_generation, activity_id, client))

        usages = []
        for potential_usage in process_run.qualified_usage or []:
            if all(input.consumes.path != potential_usage.entity.path for input in run.inputs):
                continue

            usages.append(_convert_usage(potential_usage, activity_id, client))

        parameters = _create_parameters(activity_id=activity_id, plan=plan, usages=usages, generations=generations)

        activities.append(
            Activity(
                agents=agents,
                annotations=process_run.annotations,
                association=association,
                ended_at_time=process_run.ended_at_time,
                generations=generations,
                id=activity_id,
                invalidations=invalidations,
                parameters=parameters,
                project_id=project_id,
                started_at_time=process_run.started_at_time,
                usages=usages,
            )
        )

    return activities


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

    checksum = client.repository.get_object_hash(revision=revision, path=entity.path)
    if not checksum:
        _EntityWarningCache.warn(path=entity.path, revision=revision)
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
        entity_commit = client.repository.get_previous_commit(path=entity.path, revision=revision, submodule=True)
    except errors.GitCommitNotFoundError:
        return None

    if entity_commit.hexsha != revision:
        return None

    checksum = client.repository.get_object_hash(revision=revision, path=entity.path)
    if not checksum:
        _EntityWarningCache.warn(path=entity.path, revision=revision)
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
    commit = client.repository.get_previous_commit(revision=commit_sha, path=entity.path, submodule=True)
    revision = commit.hexsha
    checksum = client.repository.get_object_hash(revision=revision, path=entity.path)
    if not checksum:
        # Entity was deleted at revision; get the one before it to have object_id
        checksum = client.repository.get_object_hash(revision=f"{revision}~", path=entity.path)
        if not checksum:
            _EntityWarningCache.warn(path=entity.path, revision=revision)
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
        id = ParameterValue.generate_id(activity_id)
        parameters.append(ParameterValue(id=id, parameter_id=input.id, value=usage.entity.path))

    outputs = {o.default_value: o for o in plan.outputs}

    for generation in generations:
        output = outputs.pop(generation.entity.path, None)
        assert output is not None, f"Cannot find generation path '{generation.entity.path}' in plan {plan.id}"
        id = ParameterValue.generate_id(activity_id)
        parameters.append(ParameterValue(id=id, parameter_id=output.id, value=generation.entity.path))

    for parameter in plan.parameters:
        id = ParameterValue.generate_id(activity_id)
        parameters.append(ParameterValue(id=id, parameter_id=parameter.id, value=parameter.default_value))

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


def _process_datasets(
    client: LocalClient, commit: Commit, datasets_provenance: DatasetsProvenance, is_last_commit, preserve_identifiers
):
    changes = commit.get_changes(paths=".renku/datasets/*/*.yml")
    changed_paths = [c.b_path for c in changes if not c.deleted]
    paths = [p for p in changed_paths if len(Path(p).parents) == 4]  # Exclude files that are not in the right place
    deleted_paths = [c.a_path for c in changes if c.deleted]

    datasets, deleted_datasets = _fetch_datasets(
        client=client, revision=commit.hexsha, paths=paths, deleted_paths=deleted_paths
    )

    revision = commit.hexsha
    date = commit.authored_datetime

    for dataset in datasets:
        dataset, tags = convert_dataset(dataset=dataset, client=client, revision=revision)
        if is_last_commit:
            datasets_provenance.update_during_migration(
                dataset,
                commit_sha=revision,
                date=date,
                tags=tags,
                replace=True,
                preserve_identifiers=preserve_identifiers,
            )
        else:
            datasets_provenance.update_during_migration(
                dataset, commit_sha=revision, date=date, tags=tags, preserve_identifiers=preserve_identifiers
            )
    for dataset in deleted_datasets:
        dataset, _ = convert_dataset(dataset=dataset, client=client, revision=revision)
        datasets_provenance.update_during_migration(
            dataset, commit_sha=revision, date=date, remove=True, preserve_identifiers=preserve_identifiers
        )


def _fetch_datasets(client: LocalClient, revision: str, paths: List[str], deleted_paths: List[str]):
    from renku.core.management.migrations.models.v9 import Dataset

    datasets_path = client.path / ".renku" / "tmp" / OLD_DATASETS_PATH
    shutil.rmtree(datasets_path, ignore_errors=True)
    datasets_path.mkdir(parents=True, exist_ok=True)

    client.dataset_migration_context = _DatasetMigrationContext(client=client, revision=revision)

    def read_project_version():
        """Read project version at revision."""
        try:
            project_file_content = client.repository.get_content(path=".renku/metadata.yml", revision=revision)
        except errors.GitCommandError:  # Project metadata file does not exist
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
                rev = client.repository.get_previous_commit(path, revision=f"{revision}~", submodule=True)
            identifier = get_dataset_identifier(path)
            if not identifier:
                continue
            new_path = datasets_path / identifier / "metadata.yml"
            new_path.parent.mkdir(parents=True, exist_ok=True)
            content = client.repository.get_content(path=path, revision=str(rev))
            new_path.write_text(content)
            if path in deleted_paths:
                deleted.append(new_path)
            else:
                existing.append(new_path)

        try:
            project_version = read_project_version()
            set_temporary_datasets_path(datasets_path)
            communication.disable()
            renku.core.management.migrate.migrate(
                project_version=project_version,
                skip_template_update=True,
                skip_docker_update=True,
                max_version=8,
                migration_type=MigrationType.DATASETS,
            )
        finally:
            communication.enable()
            unset_temporary_datasets_path()

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
            self.client.repository.run_git_command("cat-file", "-e", f"{self.revision}:{path}")
        except errors.GitCommandError:
            return False
        else:
            return True

    def is_dir(self, path) -> bool:
        try:
            result = self.client.repository.run_git_command("cat-file", "-t", f"{self.revision}:{path}")
        except errors.GitCommandError:
            return False
        else:
            return "tree" in result

    def get_previous_commit(self, path):
        return self.client.repository.get_previous_commit(path, revision=self.revision)


def _remove_dataset_metadata_files(client: LocalClient):
    """Remove old dataset metadata."""

    try:
        shutil.rmtree(os.path.join(client.renku_path, OLD_DATASETS_PATH))
    except FileNotFoundError:
        pass
    try:
        shutil.rmtree(os.path.join(client.renku_path, "refs", OLD_DATASETS_PATH))
    except FileNotFoundError:
        pass


class _EntityWarningCache:
    """Cache warning messages to avoid re-prints."""

    cache = defaultdict(set)

    @staticmethod
    def warn(path, revision):
        """Print a warning if it's not already printed for the path/revision."""
        path = str(path)
        revision = str(revision)

        already_warned = _EntityWarningCache.cache[path]
        if revision in already_warned:
            return

        already_warned.add(revision)
        communication.warn(f"Entity '{path}' not found at '{revision}'")
