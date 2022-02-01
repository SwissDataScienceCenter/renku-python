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
"""Project initialization logic."""

from pathlib import Path
from uuid import uuid4

import attr

from renku.core import errors
from renku.core.commands.git import set_git_home
from renku.core.management import RENKU_HOME
from renku.core.management.command_builder.command import Command, inject
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.interface.database_dispatcher import IDatabaseDispatcher
from renku.core.management.interface.database_gateway import IDatabaseGateway
from renku.core.management.migrations.utils import OLD_METADATA_PATH
from renku.core.management.template.template import (
    TEMPLATE_INIT_APPEND_FILES,
    TEMPLATE_KEEP_FILES,
    MetadataManager,
    fetch_templates_source,
    update_project_metadata,
)
from renku.core.management.template.usecase import create_project_from_template, get_template_files, select_template
from renku.core.models.template import SourceTemplate
from renku.core.utils import communication
from renku.core.utils.os import is_path_empty
from renku.version import __version__, is_release


def store_directory(value):
    """Store directory as a new Git home."""
    value = Path(value)
    value.mkdir(parents=True, exist_ok=True)
    set_git_home(value)
    return value


@inject.autoparams()
def create_backup_branch(path, client_dispatcher: IClientDispatcher):
    """Creates a backup branch of the repository."""
    client = client_dispatcher.current_client

    branch_name = None
    if not is_path_empty(path):
        try:
            if client.repository.head.is_valid():
                commit = client.repository.head.commit
                hexsha = commit.hexsha[:7]

                branch_name = f"pre_renku_init_{hexsha}"

                for ref in client.repository.branches:
                    if branch_name == ref.name:
                        branch_name = f"pre_renku_init_{hexsha}_{uuid4().hex}"
                        break

                with client.worktree(
                    branch_name=branch_name,
                    commit=commit,
                    merge_args=["--no-ff", "-s", "recursive", "-X", "ours", "--allow-unrelated-histories"],
                ):
                    communication.warn("Saving current data in branch {0}".format(branch_name))
        except AttributeError:
            communication.echo("Warning! Overwriting non-empty folder.")
        except errors.GitCommandError:
            raise

    return branch_name


@inject.autoparams()
def _init(
    ctx,
    external_storage_requested,
    path,
    name,
    description,
    keywords,
    template_id,
    template_source,
    template_ref,
    input_parameters,
    custom_metadata,
    force,
    data_dir,
    initial_branch,
    client_dispatcher: IClientDispatcher,
    database_dispatcher: IDatabaseDispatcher,
):
    """Initialize a renku project."""
    client = client_dispatcher.current_client

    # NOTE: set local path and storage
    store_directory(path)
    if not client.external_storage_requested:
        external_storage_requested = False

    # NOTE: create new copy of LocalClient with modified values
    ctx.obj = client = attr.evolve(
        client, path=path, data_dir=data_dir, external_storage_requested=external_storage_requested
    )
    client_dispatcher.push_created_client_to_stack(client)
    database_dispatcher.push_database_to_stack(client.database_path, commit=True)

    communication.echo("Initializing Git repository...")
    client.init_repository(force, None, initial_branch=initial_branch)

    templates_source = fetch_templates_source(source=template_source, reference=template_ref)
    source_template = select_template(templates_source=templates_source, id=template_id)

    metadata = dict()
    # NOTE: supply metadata
    metadata["__template_source__"] = template_source
    metadata["__template_ref__"] = template_ref
    metadata["__template_id__"] = template_id
    metadata["__namespace__"] = ""
    metadata["__sanitized_project_name__"] = ""
    metadata["__repository__"] = ""
    metadata["__project_slug__"] = ""
    metadata["__project_description__"] = description
    if is_release() and "__renku_version__" not in metadata:
        metadata["__renku_version__"] = __version__
    metadata["name"] = name  # NOTE: kept for backwards compatibility
    metadata["__name__"] = name

    metadata_manager = MetadataManager.from_metadata(metadata=metadata)
    metadata_manager.update_from_template(template=source_template)
    # TODO: Validate input_parameters to make sure they don't contain __\w+__ keys
    metadata_manager.set_template_variables(template=source_template, input_parameters=input_parameters)

    existing = [
        p
        for p in get_template_files(template_base=source_template.path, template_metadata=metadata)
        if (client.path / p).exists()
    ]

    append = list(filter(lambda x: x.lower() in TEMPLATE_INIT_APPEND_FILES, existing))
    existing = list(filter(lambda x: x.lower() not in TEMPLATE_INIT_APPEND_FILES + TEMPLATE_KEEP_FILES, existing))

    if (existing or append) and not force:
        message = ""

        if existing:
            existing = sorted(existing)
            existing_paths = "\n\t".join(existing)
            message += f"The following files exist in the directory and will be overwritten:\n\t{existing_paths}\n"

        if append:
            append = sorted(append)
            append_paths = "\n\t".join(append)
            message += f"The following files exist in the directory and will be appended to:\n\t{append_paths}\n"

        communication.confirm(f"{message}Proceed?", abort=True, warning=True)

    branch_name = create_backup_branch(path=path)

    # Initialize an empty database
    database_gateway = inject.instance(IDatabaseGateway)
    database_gateway.initialize()

    # add metadata.yml for backwards compatibility
    metadata_path = client.renku_path.joinpath(OLD_METADATA_PATH)
    with open(metadata_path, "w") as f:
        f.write(
            "# Dummy file kept for backwards compatibility, does not contain actual version\n"
            "'http://schema.org/schemaVersion': '9'"
        )

    # NOTE: clone the repo
    communication.echo("Initializing new Renku repository... ")
    with client.lock:
        try:
            create_from_template(
                template_path=source_template.path,
                client=client,
                name=name,
                metadata=metadata_manager.metadata,
                custom_metadata=custom_metadata,
                template_version=source_template.version,
                immutable_template_files=source_template.immutable_files or [],
                automated_update=True,  # TODO: This should come from a command line flag
                force=force,
                data_dir=data_dir,
                description=description,
                keywords=keywords,
            )
        except FileExistsError as e:
            raise errors.InvalidFileOperation(e)
    if branch_name:
        communication.echo(
            "Project initialized.\n"
            f"You can undo this command by running 'git reset --hard {branch_name}'\n"
            f"You can see changes made by running 'git diff {branch_name} {client.repository.head.reference.name}'"
        )
    else:
        communication.echo("Project initialized.")


def init_command():
    """Init command builder."""
    return Command().command(_init).with_database()


def create_from_template(
    template_path,
    client,
    name=None,
    metadata={},
    custom_metadata=None,
    template_version=None,
    immutable_template_files=[],
    automated_update=True,
    force=None,
    data_dir=None,
    user=None,
    commit_message=None,
    description=None,
    keywords=None,
):
    """Initialize a new project from a template."""
    template_files = list(get_template_files(template_base=template_path, template_metadata=metadata))

    commit_only = [f"{RENKU_HOME}/"] + template_files

    if data_dir:
        data_path = client.path / data_dir
        data_path.mkdir(parents=True, exist_ok=True)
        keep = data_path / ".gitkeep"
        keep.touch(exist_ok=True)
        commit_only.append(keep)

    if "__name__" not in metadata:
        metadata["name"] = name
        metadata["__name__"] = name

    source_template = SourceTemplate(
        id=metadata["__template_id__"],
        name="",
        description="",
        parameters={},
        icon="",
        immutable_files=immutable_template_files,
        allow_update=True,  # TODO: Get this from a command line param
        source=metadata["__template_source__"],
        reference=metadata["__template_ref__"],
        version=template_version,
        path=template_path,
        templates_source=None,
    )

    with client.commit(commit_message=commit_message, commit_only=commit_only, skip_dirty_checks=True):
        with client.with_metadata(
            name=name, description=description, custom_metadata=custom_metadata, keywords=keywords
        ) as project:
            metadata["__template_version__"] = template_version

            update_project_metadata(
                project=project, template_metadata=metadata, immutable_files=immutable_template_files
            )
            create_project_from_template(client=client, source_template=source_template, template_metadata=metadata)

        if data_dir:
            client.set_value("renku", client.DATA_DIR_CONFIG_KEY, str(data_dir))


@inject.autoparams()
def _create_from_template_local(
    template_path,
    name,
    client_dispatcher: IClientDispatcher,
    metadata={},
    custom_metadata=None,
    default_metadata={},
    template_version=None,
    immutable_template_files=[],
    automated_template_update=True,
    user=None,
    source=None,
    ref=None,
    invoked_from=None,
    initial_branch=None,
    commit_message=None,
    description=None,
    keywords=None,
):
    """Initialize a new project from a template."""
    client = client_dispatcher.current_client

    metadata = {**default_metadata, **metadata}

    client.init_repository(False, user, initial_branch=initial_branch)

    # Initialize an empty database
    database_gateway = inject.instance(IDatabaseGateway)
    database_gateway.initialize()

    create_from_template(
        template_path=template_path,
        client=client,
        name=name,
        metadata=metadata,
        custom_metadata=custom_metadata,
        template_version=template_version,
        immutable_template_files=immutable_template_files,
        automated_update=automated_template_update,
        force=False,
        user=user,
        commit_message=commit_message,
        description=description,
        keywords=keywords,
    )


def create_from_template_local_command():
    """Command to initialize a new project from a template."""
    return Command().command(_create_from_template_local).with_database()
