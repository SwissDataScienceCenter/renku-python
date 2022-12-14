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

import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union
from uuid import uuid4

from pydantic import validate_arguments

from renku.command.command_builder.command import inject
from renku.command.mergetool import setup_mergetool
from renku.core import errors
from renku.core.config import set_value
from renku.core.constant import DATA_DIR_CONFIG_KEY, RENKU_HOME
from renku.core.git import with_worktree
from renku.core.githooks import install_githooks
from renku.core.interface.database_gateway import IDatabaseGateway
from renku.core.migration.utils import OLD_METADATA_PATH
from renku.core.storage import init_external_storage, storage_installed
from renku.core.template.template import (
    FileAction,
    RenderedTemplate,
    TemplateAction,
    copy_template_to_project,
    fetch_templates_source,
    get_file_actions,
    set_template_parameters,
)
from renku.core.template.usecase import select_template
from renku.core.util import communication
from renku.core.util.contexts import with_project_metadata
from renku.core.util.git import with_commit
from renku.core.util.os import is_path_empty
from renku.domain_model.project import Project
from renku.domain_model.project_context import project_context
from renku.domain_model.template import Template, TemplateMetadata
from renku.version import __version__, is_release

if TYPE_CHECKING:
    from renku.infrastructure.repository import Repository


def create_backup_branch(path):
    """Creates a backup branch of the repository.

    Args:
        path: Repository path.

    Returns:
        Name of the backup branch.
    """
    repository = project_context.repository

    branch_name = None
    if not is_path_empty(path):
        try:
            if repository.head.is_valid():
                hexsha = repository.head.commit.hexsha[:7]

                branch_name = f"pre_renku_init_{hexsha}"

                for ref in repository.branches:
                    if branch_name == ref.name:
                        branch_name = f"pre_renku_init_{hexsha}_{uuid4().hex}"
                        break

                with with_worktree(
                    branch_name=branch_name,
                    commit=repository.head.commit,
                    merge_args=["--no-ff", "-s", "recursive", "-X", "ours", "--allow-unrelated-histories"],
                ):
                    communication.warn("Saving current data in branch {0}".format(branch_name))
        except AttributeError:
            communication.echo("Warning! Overwriting non-empty folder.")
        except errors.GitCommandError:
            raise

    return branch_name


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def init_project(
    external_storage_requested: bool,
    path: str,
    name: Optional[str],
    description: Optional[str],
    keywords: Optional[List[str]],
    template_id: Optional[str],
    template_source: Optional[str],
    template_ref: Optional[str],
    input_parameters: Dict[str, str],
    custom_metadata: Optional[Dict[str, Any]],
    force: bool,
    data_dir: Optional[Path],
    initial_branch: Optional[str],
    install_mergetool: bool,
):
    """Initialize a renku project.

    Args:
        external_storage_requested: Whether or not external storage should be used.
        path: Path to initialize repository at.
        name: Name of the project.
        description: Description of the project.
        keywords: keywords for the project.
        template_id: id of the template to use.
        template_source: Source to get the template from.
        template_ref: Reference to use to get the template.
        input_parameters: Template parameters.
        custom_metadata: Custom JSON-LD metadata for project.
        force: Whether to overwrite existing files and delete existing metadata.
        data_dir: Where to store dataset data.
        initial_branch: Default git branch.
        install_mergetool(bool): Whether to set up the renku metadata mergetool in the created project.
    """
    if not project_context.external_storage_requested:
        external_storage_requested = False

    project_context.push_path(path, save_changes=True)
    if data_dir is not None:
        project_context.datadir = str(data_dir)
    project_context.external_storage_requested = external_storage_requested

    communication.echo("Initializing Git repository...")
    project_context.repository = init_repository(force=force, user=None, initial_branch=initial_branch)

    # Initialize an empty database
    database_gateway = inject.instance(IDatabaseGateway)
    database_gateway.initialize()

    templates_source = fetch_templates_source(source=template_source, reference=template_ref)
    template = select_template(templates_source=templates_source, id=template_id)

    if template is None:
        raise errors.TemplateNotFoundError(f"Couldn't find template with id {template_id}")

    namespace, name = Project.get_namespace_and_name(
        remote=project_context.remote, name=name, repository=project_context.repository
    )
    name = name or os.path.basename(path.rstrip(os.path.sep))

    metadata: Dict[str, Any] = dict()
    # NOTE: supply metadata
    metadata["__template_source__"] = template_source
    metadata["__template_ref__"] = template_ref
    metadata["__template_id__"] = template_id
    metadata["__namespace__"] = namespace or ""
    metadata["__sanitized_project_name__"] = ""
    metadata["__repository__"] = ""
    metadata["__project_slug__"] = ""
    metadata["__project_description__"] = description
    if is_release() and "__renku_version__" not in metadata:
        metadata["__renku_version__"] = __version__
    metadata["__name__"] = name
    metadata["name"] = name  # NOTE: kept for backwards compatibility
    metadata["__template_version__"] = template.version
    metadata["__automated_update__"] = True  # TODO: This should come from a command line flag

    template_metadata = TemplateMetadata.from_dict(metadata=metadata)
    template_metadata.update(template=template)
    # TODO: Validate input_parameters to make sure they don't contain __\w+__ keys
    set_template_parameters(template=template, template_metadata=template_metadata, input_parameters=input_parameters)

    rendered_template = template.render(metadata=template_metadata)
    actions = get_file_actions(
        rendered_template=rendered_template, template_action=TemplateAction.INITIALIZE, interactive=False
    )

    if not force:
        appends = [k for k, v in actions.items() if v == FileAction.APPEND]
        overwrites = [k for k, v in actions.items() if v == FileAction.OVERWRITE]

        message = ""
        if overwrites:
            overwrites_str = "\n\t".join(sorted(overwrites))
            message += f"The following files exist in the directory and will be overwritten:\n\t{overwrites_str}\n"
        if appends:
            appends_str = "\n\t".join(sorted(appends))
            message += f"The following files exist in the directory and will be appended to:\n\t{appends_str}\n"
        if message:
            communication.confirm(f"{message}Proceed?", abort=True, warning=True)

    branch_name = create_backup_branch(path=path)

    # NOTE: clone the repo
    communication.echo("Initializing new Renku repository... ")
    with project_context.lock:
        try:
            create_from_template(
                rendered_template=rendered_template,
                actions=actions,
                name=name,
                custom_metadata=custom_metadata,
                data_dir=data_dir,
                description=description,
                keywords=keywords,
                install_mergetool=install_mergetool,
            )
        except FileExistsError as e:
            raise errors.InvalidFileOperation(e)

    # NOTE: Installing githooks
    warning_messages = install_githooks(force=force, path=project_context.path)
    if warning_messages:
        for message in warning_messages:
            communication.warn(message)

    if branch_name:
        communication.echo(
            f"Project initialized.\nYou can undo this command by running 'git reset --hard {branch_name}'\n"
            f"You can see changes made by running 'git diff {branch_name} HEAD'"
        )
    else:
        communication.echo("Project initialized.")


def init_repository(force=False, user=None, initial_branch=None) -> "Repository":
    """Initialize an empty Renku repository."""
    from renku.infrastructure.repository import Repository

    # initialize repo and set user data
    path = project_context.path
    if force and (path / RENKU_HOME).exists():
        shutil.rmtree(path / RENKU_HOME)
    repository = Repository.initialize(path=path, branch=initial_branch)
    if user:
        with repository.get_configuration(writable=True) as config_writer:
            for key, value in user.items():
                config_writer.set_value("user", key, value)

    # verify if git user information is available
    _ = repository.get_user()

    # initialize LFS if it is requested and installed
    if project_context.external_storage_requested and storage_installed():
        init_external_storage(force=force)

    return repository


def create_from_template(
    rendered_template: RenderedTemplate,
    actions: Dict[str, FileAction],
    name: Optional[str] = None,
    namespace: Optional[str] = None,
    custom_metadata: Optional[Dict] = None,
    data_dir: Optional[Union[str, Path]] = None,
    commit_message: Optional[str] = None,
    description: Optional[str] = None,
    keywords: Optional[List[str]] = None,
    install_mergetool: bool = False,
):
    """Initialize a new project from a template.

    Args:
        rendered_template(RenderedTemplate): Rendered template.
        actions(Dict[str, FileAction]): mapping of paths and actions to take.
        name(Optional[str]): Name of the project (Default value = None).
        namespace(Optional[str]): Namespace of the project (Default value = None).
        custom_metadata(Optional[Dict]): Custom JSON-LD metadata (Default value = None).
        data_dir(Optional[str]): Where to store dataset data (Default value = None).
        commit_message(Optional[str]): Message for initial commit (Default value = None).
        description(Optional[str]): Description of the project (Default value = None).
        keywords(Optional[List[str]]): Keywords for project (Default value = None).
        install_mergetool(bool): Whether to setup renku metadata mergetool (Default value = False).
    """
    commit_only = [f"{RENKU_HOME}/", str(project_context.template_checksums_path)] + list(rendered_template.get_files())

    if install_mergetool:
        commit_only.append(".gitattributes")

    if data_dir:
        data_path = project_context.path / data_dir
        data_path.mkdir(parents=True, exist_ok=True)
        keep = data_path / ".gitkeep"
        keep.touch(exist_ok=True)
        commit_only.append(str(keep))

    # add metadata.yml for backwards compatibility
    metadata_path = project_context.metadata_path.joinpath(OLD_METADATA_PATH)
    with open(metadata_path, "w") as f:
        f.write(
            "# Dummy file kept for backwards compatibility, does not contain actual version\n"
            "'http://schema.org/schemaVersion': '9'"
        )

    with with_commit(
        repository=project_context.repository,
        transaction_id=project_context.transaction_id,
        commit_message=commit_message,
        commit_only=commit_only,
        skip_dirty_checks=True,
    ):
        with with_project_metadata(
            name=name, namespace=namespace, description=description, custom_metadata=custom_metadata, keywords=keywords
        ) as project:
            copy_template_to_project(rendered_template=rendered_template, project=project, actions=actions)

        if install_mergetool:
            setup_mergetool()

        if data_dir:
            set_value("renku", DATA_DIR_CONFIG_KEY, str(data_dir))


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def create_from_template_local(
    template_path: Path,
    name: str,
    namespace: str,
    metadata: Optional[Dict] = None,
    custom_metadata: Optional[Dict] = None,
    default_metadata: Optional[Dict] = None,
    template_version: Optional[str] = None,
    immutable_template_files: Optional[List[str]] = None,
    automated_template_update: bool = True,
    user: Optional[Dict[str, str]] = None,
    initial_branch: Optional[str] = None,
    commit_message: Optional[str] = None,
    description: Optional[str] = None,
    keywords: Optional[List[str]] = None,
    data_dir: Optional[str] = None,
):
    """Initialize a new project from a template.

    Args:
        template_path(Path): Path to template.
        name(str): project name.
        namespace(str): project namespace.
        metadata(Optional[Dict]): Project metadata (Default value = None).
        custom_metadata(Optional[Dict]): Custom JSON-LD metadata (Default value = None).
        default_metadata(Optional[Dict]): Default project metadata (Default value = None).
        template_version(Optional[str]): Version of the template (Default value = None).
        immutable_template_files(Optional[List[str]]): Immutable template files (Default value = None).
        automated_template_update(bool): If template can be updated automatically (Default value = True).
        user(Optional[Dict[str, str]]): Git user (Default value = None).
        initial_branch(Optional[str]): Name of initial/main branch (Default value = None).
        commit_message(Optional[str]): Message of initial commit (Default value = None).
        description(Optional[str]): Project description (Default value = None).
        keywords(Optional[List[str]]): Project keywords (Default value = None).
        data_dir(Optional[str]): Project base data directory (Default value = None).
    """
    metadata = metadata or {}
    default_metadata = default_metadata or {}

    metadata = {**default_metadata, **metadata}

    project_context.repository = init_repository(force=False, user=user, initial_branch=initial_branch)

    # Initialize an empty database
    database_gateway = inject.instance(IDatabaseGateway)
    database_gateway.initialize()

    if "__name__" not in metadata:
        metadata["name"] = name
        metadata["__name__"] = name

    metadata["__template_version__"] = template_version

    template = Template(
        id=metadata["__template_id__"],
        name="",
        description="",
        parameters={},
        icon="",
        immutable_files=immutable_template_files or [],
        allow_update=automated_template_update,
        source=metadata["__template_source__"],
        reference=metadata["__template_ref__"],
        version=template_version,
        path=template_path,
        templates_source=None,
    )

    template_metadata = TemplateMetadata.from_dict(metadata=metadata)
    template_metadata.update(template=template)
    set_template_parameters(template=template, template_metadata=template_metadata, input_parameters={})

    rendered_template = template.render(metadata=template_metadata)
    actions = get_file_actions(
        rendered_template=rendered_template, template_action=TemplateAction.INITIALIZE, interactive=False
    )

    create_from_template(
        rendered_template=rendered_template,
        actions=actions,
        name=name,
        namespace=namespace,
        custom_metadata=custom_metadata,
        commit_message=commit_message,
        description=description,
        keywords=keywords,
        data_dir=data_dir,
    )
