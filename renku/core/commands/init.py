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

import json
import re
from collections import OrderedDict, namedtuple
from pathlib import Path
from tempfile import mkdtemp
from uuid import uuid4

import attr
import click
import yaml

from renku.core import errors
from renku.core.commands.git import set_git_home
from renku.core.management import RENKU_HOME
from renku.core.management.command_builder.command import Command, inject
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.interface.database_dispatcher import IDatabaseDispatcher
from renku.core.management.interface.database_gateway import IDatabaseGateway
from renku.core.management.migrations.utils import OLD_METADATA_PATH
from renku.core.management.repository import INIT_APPEND_FILES, INIT_KEEP_FILES
from renku.core.metadata.repository import Repository
from renku.core.models.tabulate import tabulate
from renku.core.utils import communication
from renku.core.utils.git import clone_repository
from renku.version import __version__, is_release

try:
    import importlib_resources
except ImportError:
    import importlib.resources as importlib_resources

TEMPLATE_MANIFEST = "manifest.yaml"


def create_template_sentence(templates, describe=False, instructions=False):
    """Create templates choice sentence.

    :ref templates: list of templates coming from manifest file
    :ref instructions: add instructions
    """
    Template = namedtuple("Template", ["index", "id", "description", "variables"])

    def extract_description(template_elem):
        """Extract description from template manifest."""
        if describe:
            return template_elem["description"]
        return None

    def extract_variables(template_elem):
        """Extract variables from template manifest."""
        if describe:
            descriptions = []
            for name, variable in template_elem.get("variables", {}).items():
                variable_type = f', type: {variable["type"]}' if "type" in variable else ""
                enum_values = f', options: {variable["enum"]}' if "enum" in variable else ""
                default_value = f', default: {variable["default_value"]}' if "default_value" in variable else ""
                description = variable["description"]

                descriptions.append(f"{name}: {description}{variable_type}{enum_values}{default_value}")
            return "\n".join(descriptions)

        return ",".join(template_elem.get("variables", {}).keys())

    templates_friendly = [
        Template(
            index=index + 1,
            id=template_elem["folder"],
            description=extract_description(template_elem),
            variables=extract_variables(template_elem),
        )
        for index, template_elem in enumerate(templates)
    ]

    table_headers = OrderedDict((("index", "Index"), ("id", "Id"), ("variables", "Parameters")))

    if describe:
        table_headers["description"] = "Description"

    text = tabulate(templates_friendly, headers=table_headers)

    if not instructions:
        return text
    return "{0}\nPlease choose a template by typing the index".format(text)


def store_directory(value):
    """Store directory as a new Git home."""
    value = Path(value)
    value.mkdir(parents=True, exist_ok=True)
    set_git_home(value)
    return value


def is_path_empty(path):
    """Check if path contains files.

    :ref path: target path
    """
    gen = Path(path).glob("**/*")
    return not any(gen)


def select_template_from_manifest(
    template_manifest, template_id=None, template_index=None, describe=False, prompt=True
):
    """Select a template from a template manifest."""
    repeat = False
    template_data = None
    if template_id:
        if template_index:
            raise errors.ParameterError("Use either --template-id or --template-index, not both", '"--template-index"')
        template_filtered = [
            template_elem for template_elem in template_manifest if template_elem["folder"] == template_id
        ]
        if len(template_filtered) == 1:
            template_data = template_filtered[0]
        else:
            communication.echo(f'The template with id "{template_id}" is not available.')
            repeat = True

    if template_index is not None:
        if 0 < template_index <= len(template_manifest):
            template_data = template_manifest[template_index - 1]
        else:
            communication.echo(f"The template at index {template_index} is not available.")
            repeat = True

    # NOTE: prompt user in case of wrong or missing selection criteria
    if prompt and (repeat or not (template_id or template_index)):
        templates = [template_elem for template_elem in template_manifest]
        if len(templates) == 1:
            if describe:
                communication.echo(create_template_sentence(templates, describe=describe, instructions=False))
            template_data = templates[0]
        else:
            template_index = communication.prompt(
                msg=create_template_sentence(templates, describe=describe, instructions=True),
                type=click.IntRange(1, len(templates)),
                show_default=False,
                show_choices=False,
            )
            template_data = templates[template_index - 1]

        template_id = template_data["folder"]

    return template_data, template_id


def validate_template_variable_value(name, template_variable, value):
    """Validates template values by type."""
    if "type" not in template_variable:
        return True, None, value

    variable_type = template_variable["type"]
    valid = True

    if variable_type == "string":
        if not isinstance(value, str):
            valid = False
    elif variable_type == "number":
        try:
            value = int(value)
            is_int = True
        except ValueError:
            is_int = False

        try:
            value = float(value)
            is_float = True
        except ValueError:
            is_float = False

        if not is_float and not is_int:
            valid = False
    elif variable_type == "boolean":
        truthy = [True, 1, "1", "true", "True"]
        falsy = [False, 0, "0", "false", "False"]
        if value not in truthy and value not in falsy:
            valid = False
        else:
            value = True if value in truthy else False
    elif variable_type == "enum":
        if "enum" not in template_variable:
            raise errors.InvalidTemplateError(
                f'Template contains variable {name} of type enum but does not provide a corresponding "enum" list.'
            )
        possible_values = template_variable["enum"]
        if value not in possible_values:
            return (
                False,
                f"Value '{value}' is not in list of possible values {possible_values} for template parameter {name}.",
                value,
            )
    else:
        raise errors.InvalidTemplateError(
            f"Template contains variable {name} of type {variable_type} which is not supported."
        )

    if not valid:
        return False, f"Value '{value}' is not of type {variable_type} required by {name}.", value

    return True, None, value


def prompt_for_value(name, template_variable):
    """Prompt the user for a template value."""
    valid = False
    while not valid:
        variable_type = f', type: {template_variable["type"]}' if "type" in template_variable else ""
        enum_values = f', options: {template_variable["enum"]}' if "enum" in template_variable else ""
        default_value = template_variable["default_value"] if "default_value" in template_variable else ""

        value = communication.prompt(
            msg=(
                f'The template requires a value for "{name}" ({template_variable["description"]}'
                f"{variable_type}{enum_values})"
            ),
            default=default_value,
            show_default=bool(default_value),
        )

        valid, msg, value = validate_template_variable_value(name, template_variable, value)

        if msg:
            communication.info(msg)

    return value


def verify_template_variables(template_data, metadata):
    """Verifies that template variables are correctly set."""
    template_variables = template_data.get("variables", {})
    template_variables_keys = set(template_variables.keys())
    input_parameters_keys = set(metadata.keys())

    for key in template_variables:
        if "description" not in template_variables[key]:
            raise errors.InvalidTemplateError(f"Template parameter {key} does not contain a description.")

    for key in sorted(template_variables_keys.intersection(input_parameters_keys)):
        valid, msg, metadata[key] = validate_template_variable_value(key, template_variables[key], metadata[key])

        if not valid:
            communication.info(msg)

            metadata[key] = prompt_for_value(key, template_variables[key])

    for key in template_variables_keys - input_parameters_keys:
        metadata[key] = prompt_for_value(key, template_variables[key])

    # ignore internal variables, i.e. __\w__
    internal_keys = re.compile(r"__\w+__$")
    input_parameters_keys = set([i for i in input_parameters_keys if not internal_keys.match(i)])
    unused_variables = input_parameters_keys - template_variables_keys
    if len(unused_variables) > 0:
        communication.info(
            "These parameters are not used by the template and were "
            "ignored:\n\t{}".format("\n\t".join(unused_variables))
        )

    return metadata


@inject.autoparams()
def get_existing_template_files(template_path, metadata, client_dispatcher: IClientDispatcher, force=False):
    """Gets files in the template that already exists in the repository."""
    client = client_dispatcher.current_client

    template_files = list(client.get_template_files(template_path, metadata))

    existing = []

    for rel_path in template_files:
        destination = client.path / rel_path

        if destination.exists():
            existing.append(str(rel_path))

    return existing


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
    template_index,
    template_source,
    template_ref,
    metadata,
    custom_metadata,
    list_templates,
    force,
    describe,
    data_dir,
    initial_branch,
    client_dispatcher: IClientDispatcher,
    database_dispatcher: IDatabaseDispatcher,
):
    """Initialize a renku project."""
    client = client_dispatcher.current_client

    template_manifest, template_folder, template_source, template_version = fetch_template(
        template_source, template_ref
    )

    template_data, template_id = select_template_from_manifest(
        template_manifest, template_id, template_index, describe, prompt=not list_templates
    )

    if list_templates:
        if template_data:
            communication.echo(create_template_sentence([template_data], describe=describe))
        else:
            communication.echo(create_template_sentence(template_manifest, describe=describe))
        return

    metadata = verify_template_variables(template_data, metadata)

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

    # NOTE: supply additional metadata
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

    template_path = template_folder / template_data["folder"]

    existing = get_existing_template_files(template_path=template_path, metadata=metadata, force=force)

    append = list(filter(lambda x: x.lower() in INIT_APPEND_FILES, existing))
    existing = list(filter(lambda x: x.lower() not in INIT_APPEND_FILES + INIT_KEEP_FILES, existing))

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
                template_path=template_path,
                client=client,
                name=name,
                metadata=metadata,
                custom_metadata=custom_metadata,
                template_version=template_version,
                immutable_template_files=template_data.get("immutable_template_files", []),
                automated_update=template_data.get("allow_template_update", False),
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


def fetch_template_from_git(source, ref=None, tempdir=None):
    """Fetch the template from a git repository and checkout the relevant data.

    :param source: url or full path of the templates repository
    :param ref: reference for git checkout - branch, commit or tag
    :param tempdir: temporary work directory path
    :return: tuple of (template folder, template version)
    """
    template_repository = clone_repository(url=source, path=tempdir, checkout_revision=ref, install_lfs=False)

    # checkout the manifest
    try:
        template_repository.checkout(TEMPLATE_MANIFEST)
    except errors.GitCommandError as e:
        raise errors.GitError(f"Cannot checkout manifest file {TEMPLATE_MANIFEST}") from e

    return template_repository.path / TEMPLATE_MANIFEST, template_repository.head.commit.hexsha


def fetch_template(template_source, template_ref):
    """Fetches a local or remote template.

    :param template_source: url or full path of the templates repository
    :param template_ref: reference for git checkout - branch, commit or tag
    :return: tuple of (template manifest, template folder, template source, template version)
    """
    if template_source and template_source != "renku":
        communication.echo("Fetching template from {0}@{1}... ".format(template_source, template_ref or ""))
        template_folder = Path(mkdtemp())
        _, template_version = fetch_template_from_git(template_source, template_ref, template_folder)
        template_manifest = read_template_manifest(template_folder, checkout=True)
        communication.echo("OK")
    else:
        from renku import __version__

        if template_ref and template_ref != "master":
            raise errors.ParameterError("Templates included in renku don't support specifying a template_ref")

        ref = importlib_resources.files("renku") / "templates"
        with importlib_resources.as_file(ref) as folder:
            template_folder = folder
        template_manifest = read_template_manifest(template_folder)
        template_source = "renku"
        template_version = str(__version__)

    return template_manifest, template_folder, template_source, template_version


def validate_template(template_path):
    """Validate a local template.

    :param template_path: path of the template to validate
    """
    # TODO: implement a better check
    required_folders = [RENKU_HOME]
    required_files = ["{0}/renku.ini".format(RENKU_HOME), "Dockerfile"]
    for folder in required_folders:
        if not Path(template_path, folder).is_dir():
            raise errors.InvalidTemplateError("Folder {0} is required for the template to be valid".format(folder))
    for file in required_files:
        if not Path(template_path, file).is_file():
            raise errors.InvalidTemplateError("File {0} is required for the template to be valid".format(file))
    return True


def validate_template_manifest(manifest):
    """Validate manifest content.

    :param manifest: manifest file content
    """
    if not isinstance(manifest, list):
        raise errors.InvalidTemplateError(
            ("The repository doesn't contain a valid", '"{0}" file'.format(TEMPLATE_MANIFEST))
        )
    for template in manifest:
        if not isinstance(template, dict) or "name" not in template:
            raise errors.InvalidTemplateError((f'Every template listed in "{TEMPLATE_MANIFEST}"', " must have a name"))
        for attribute in ["folder", "description"]:
            if attribute not in template:
                raise errors.InvalidTemplateError(
                    ('Template "{0}" doesn\'t have a {1} attribute'.format(template["name"], attribute))
                )

        if "variables" in template:
            for key, variable in template["variables"].items():
                if isinstance(variable, str):
                    # NOTE: Backwards compatibility
                    template["variables"][key] = {"description": variable}
    return True


def read_template_manifest(folder, checkout=False):
    """Extract template metadata from the manifest file.

    :param folder: path where to find the template manifest file
    :param checkout: checkout the template folder from local repo
    """
    folder = Path(folder)
    manifest_path = folder / TEMPLATE_MANIFEST
    try:
        manifest = yaml.safe_load(manifest_path.read_text())
    except FileNotFoundError as e:
        raise errors.InvalidTemplateError('There is no manifest file "{0}"'.format(TEMPLATE_MANIFEST)) from e
    validate_template_manifest(manifest)

    if checkout:
        repository = Repository(folder)
        template_folders = [template["folder"] for template in manifest]
        template_icons = [template["icon"] for template in manifest if "icon" in template]
        if len(template_folders) < 1:
            raise errors.InvalidTemplateError("Cannot find any valid template in manifest file")
        for template_folder in template_folders:
            template_path = folder / template_folder
            try:
                repository.checkout(template_folder)
            except errors.GitCommandError as e:
                raise errors.InvalidTemplateError('Cannot checkout the folder "{0}"'.format(template_folder)) from e
            validate_template(template_path)

        for template_icon in template_icons:
            try:
                repository.checkout(template_icon)
            except errors.GitCommandError as e:
                raise errors.InvalidTemplateError('Cannot checkout the icon "{0}"'.format(template_icon)) from e

    return manifest


def create_from_template(
    template_path,
    client,
    name=None,
    metadata={},
    custom_metadata=None,
    template_version=None,
    immutable_template_files=[],
    automated_update=False,
    force=None,
    data_dir=None,
    user=None,
    commit_message=None,
    description=None,
    keywords=None,
):
    """Initialize a new project from a template."""

    template_files = list(client.get_template_files(template_path, metadata))

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

    with client.commit(commit_message=commit_message, commit_only=commit_only, skip_dirty_checks=True):
        with client.with_metadata(
            name=name, description=description, custom_metadata=custom_metadata, keywords=keywords
        ) as project:
            project.template_source = metadata["__template_source__"]
            project.template_ref = metadata["__template_ref__"]
            project.template_id = metadata["__template_id__"]
            project.template_version = template_version
            project.immutable_template_files = immutable_template_files
            project.automated_update = automated_update
            project.template_metadata = json.dumps(metadata)

            client.import_from_template(template_path, metadata, force)

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
    automated_template_update=False,
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
