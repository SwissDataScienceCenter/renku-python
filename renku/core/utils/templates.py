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
"""Template utilities.

A project has three attributes to specify a template: ``template_source``, ``template_version``, and ``template_ref``.
In projects that use templates that are bundled with Renku, ``template_source`` is "renku" and ``template_version`` is
set to the installed Renku version. ``template_ref`` should not be set for such projects.

For projects that use a template from a Git repository, ``template_source`` is repository's URL and ``template_version``
is set to the current HEAD commit SHA. If a Git referenced was passed when setting the template, then project's
``template_ref`` is the same as the passed reference. In this case, Renku won't update a project's template if the
reference is a fixed value (i.e. a tag or a commit SHA).
"""

import json
import os
import re
import shutil
from collections import OrderedDict, namedtuple
from pathlib import Path
from tempfile import mkdtemp
from typing import Dict, Generator, List, Optional, Tuple, Union

import click
import yaml
from jinja2 import Template
from packaging.version import Version

from renku.core import errors
from renku.core.management import RENKU_HOME
from renku.core.management.command_builder.command import inject
from renku.core.management.interface.client_dispatcher import IClientDispatcher
from renku.core.management.interface.project_gateway import IProjectGateway
from renku.core.metadata.repository import Repository
from renku.core.models.tabulate import tabulate
from renku.core.utils import communication
from renku.core.utils.git import clone_repository
from renku.core.utils.os import hash_file, hash_str

try:
    import importlib_resources
except ImportError:
    import importlib.resources as importlib_resources

TEMPLATE_MANIFEST = "manifest.yaml"
TEMPLATE_KEEP_FILES = ["readme.md", "readme.rst", "readme.txt", "readme"]
TEMPLATE_INIT_APPEND_FILES = [".gitignore"]


def is_renku_template(template_source: Optional[str]) -> bool:
    """Return if template comes from Renku."""
    return not template_source or template_source.lower() == "renku"


def has_template_checksum(client) -> bool:
    """Return if project has a templates checksum file."""
    return os.path.exists(client.template_checksums)


def write_template_checksum(client, checksums: Dict):
    """Write templates checksum file for a project."""
    client.template_checksums.parent.mkdir(parents=True, exist_ok=True)

    with open(client.template_checksums, "w") as checksum_file:
        json.dump(checksums, checksum_file)


def _read_template_checksum(client) -> Optional[Dict]:
    """Read templates checksum file for a project."""
    if has_template_checksum(client):
        with open(client.template_checksums, "r") as checksum_file:
            return json.load(checksum_file)


def _read_renku_version_from_dockerfile(path: Union[Path, str]) -> Optional[str]:
    """Read RENKU_VERSION from the content of path if a valid version is available."""
    path = Path(path)
    if not path.exists():
        return

    docker_content = path.read_text()
    m = re.search(r"^\s*ARG RENKU_VERSION=(.+)$", docker_content, flags=re.MULTILINE)
    if not m:
        return

    try:
        return str(Version(m.group(1)))
    except ValueError:
        return


def get_template_files(template_base: Path, template_metadata) -> Generator[str, None, None]:
    """Return relative paths Gets paths in a rendered renku template."""
    for file in template_base.rglob("*"):
        relative_path = str(file.relative_to(template_base))
        # NOTE: The path could contain template variables, we need to template it
        relative_path = Template(relative_path).render(template_metadata)

        yield relative_path


def check_for_template_update(client) -> Tuple[bool, Optional[str], Optional[str]]:
    """Check if the project can be updated to a newer version of the project template."""
    try:
        project = client.project
    except ValueError:
        # NOTE: Old project, we don't know the status until it is migrated
        return False, None, None

    template_manifest, template_folder, template_source, template_version = fetch_template(
        project.template_source, project.template_ref, validate=False
    )

    update_available = is_template_update_available(
        template_source=template_source, template_version=template_version, project=project
    )

    return update_available, project.template_version, template_version


def is_template_update_available(template_source, template_version, project) -> bool:
    """Return True if template is newer than the project's template."""
    if not project.template_version:
        return False

    if is_renku_template(template_source):
        template_version = Version(template_version)
        try:
            current_version = Version(project.template_version)
        except ValueError:  # NOTE: project.template_version is not a valid SemVer
            return True
        else:
            return current_version < template_version
    else:
        # NOTE: Project's template commit cannot be newer than the remote template's HEAD, so, this check is enough to
        # see if an update is available
        return template_version != project.template_version


def create_template_sentence(templates, describe=False, instructions=False):
    """Create templates choice sentence.

    :ref templates: list of templates coming from manifest file
    :ref instructions: add instructions
    """
    RenkuTemplate = namedtuple("RenkuTemplate", ["index", "id", "description", "variables"])

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
        RenkuTemplate(
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
    return f"{text}\nPlease choose a template by typing the index"


def select_template_from_manifest(
    templates: List[Dict], template_id=None, template_index=None, describe=False, prompt=True
):
    """Select a template from a template manifest."""
    repeat = False
    template_data = None
    if template_id:
        if template_index:
            raise errors.ParameterError("Use either --template-id or --template-index, not both", "--template-index")
        template_filtered = [t for t in templates if t["folder"] == template_id]
        if len(template_filtered) == 1:
            template_data = template_filtered[0]
        else:
            communication.echo(f"The template with id '{template_id}' is not available.")
            repeat = True

    if template_index is not None:
        if 0 < template_index <= len(templates):
            template_data = templates[template_index - 1]
        else:
            communication.echo(f"The template at index {template_index} is not available.")
            repeat = True

    # NOTE: prompt user in case of wrong or missing selection criteria
    if prompt and (repeat or not (template_id or template_index)):
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


def set_template_variables(template_data, template_metadata, interactive=False):
    """Verifies that template variables are correctly set."""
    template_variables = template_data.get("variables", {})
    template_variables_keys = set(template_variables.keys())

    # NOTE: Copy missing variables that have a default value to template metadata
    for key, variable in template_variables.items():
        if key not in template_metadata and "default_value" in variable:
            template_metadata[key] = variable["default_value"]

    input_parameters_keys = set(template_metadata.keys())

    missing_keys = template_variables_keys - input_parameters_keys
    if missing_keys and not interactive:
        missing_keys = ", ".join(missing_keys)
        raise errors.TemplateUpdateError(f"Can't update template, it now requires variable(s): {missing_keys}")

    for key in sorted(template_variables_keys & input_parameters_keys):
        valid, msg, template_metadata[key] = _validate_template_variable(
            name=key, template_variable=template_variables[key], value=template_metadata[key]
        )

        if not valid:
            if not communication.has_prompt():
                raise errors.TemplateUpdateError(f"Invalid value '{template_metadata[key]}' for variable '{key}'")
            communication.info(msg)
            template_metadata[key] = _prompt_for_value(key, template_variables[key])
        elif interactive and communication.has_prompt():
            template_metadata[key] = _prompt_for_value(key, template_variables[key])

    for key in sorted(missing_keys):
        template_metadata[key] = _prompt_for_value(key, template_variables[key])

    # ignore internal variables, i.e. __\w__
    internal_keys = re.compile(r"__\w+__$")
    input_parameters_keys = set([i for i in input_parameters_keys if not internal_keys.match(i)])
    unused_variables = input_parameters_keys - template_variables_keys
    unused_variables -= {"name"}  # NOTE: 'name' is kept for backward-compatibility and might not be used at all
    if len(unused_variables) > 0:
        unused_str = "\n\t".join(unused_variables)
        communication.info(f"These parameters are not used by the template and were ignored:\n\t{unused_str}\n")

    return template_metadata


def _validate_template_variable(name, template_variable, value):
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
                f"Template contains variable '{name}' of type enum but does not provide a corresponding enum list."
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
            f"Template contains variable '{name}' of type '{variable_type}' which is not supported."
        )

    if not valid:
        return False, f"Value '{value}' is not of type {variable_type} required by '{name}'.", value

    return True, None, value


def _prompt_for_value(name, template_variable):
    """Prompt the user for a template value."""
    while True:
        variable_type = f', type: {template_variable["type"]}' if "type" in template_variable else ""
        enum_values = f', options: {template_variable["enum"]}' if "enum" in template_variable else ""
        default_value = template_variable["default_value"] if "default_value" in template_variable else ""
        description = template_variable.get("description", "")

        value = communication.prompt(
            msg=f"The template requires a value for '{name}' ({description}{variable_type}{enum_values})",
            default=default_value,
            show_default=bool(default_value),
        )

        valid, msg, value = _validate_template_variable(name, template_variable, value)

        if msg:
            communication.info(msg)

        if valid:
            return value


def fetch_template(template_source: Optional[str], template_ref: Optional[str], validate: bool = True):
    """Fetches a local or remote template.

    :param template_source: url or full path of the templates repository
    :param template_ref: reference for git checkout - branch, commit or tag
    :return: tuple of (template manifest, template folder, template source, template version)
    """
    if validate and template_ref and not template_source:
        raise errors.ParameterError("Can't use '--template-ref' without specifying '--template-source'")

    if is_renku_template(template_source):
        from renku import __version__

        if validate and template_ref and template_ref != "master":
            raise errors.ParameterError("Templates included in renku don't support specifying a template reference")

        ref = importlib_resources.files("renku") / "templates"
        with importlib_resources.as_file(ref) as folder:
            template_folder = folder
        template_manifest = read_template_manifest(template_folder)
        template_source = "renku"
        template_version = str(__version__)
    else:
        ref = f"@{template_ref}" if template_ref else ""
        communication.echo(f"Fetching template from {template_source}{ref}... ")
        template_folder = Path(mkdtemp())
        _, template_version = fetch_template_from_git(template_source, template_ref, template_folder)
        template_manifest = read_template_manifest(template_folder, checkout=True)
        communication.echo("OK")

    return template_manifest, template_folder, template_source, template_version


def fetch_template_from_git(source, ref=None, tempdir=None) -> Tuple[Path, str]:
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
        raise errors.GitError(f"Cannot checkout manifest file '{TEMPLATE_MANIFEST}'") from e

    return template_repository.path / TEMPLATE_MANIFEST, template_repository.head.commit.hexsha


def validate_template(template_path):
    """Validate a local template.

    :param template_path: path of the template to validate
    """
    # TODO: implement a better check
    required_folders = [RENKU_HOME]
    required_files = [f"{RENKU_HOME}/renku.ini", "Dockerfile"]
    for folder in required_folders:
        if not Path(template_path, folder).is_dir():
            raise errors.InvalidTemplateError(f"Folder '{folder}' is required for the template to be valid")
    for file in required_files:
        if not Path(template_path, file).is_file():
            raise errors.InvalidTemplateError(f"File '{file}' is required for the template to be valid")
    return True


def validate_template_manifest(manifest):
    """Validate manifest content.

    :param manifest: manifest file content
    """
    if not isinstance(manifest, list):
        raise errors.InvalidTemplateError(f"The repository doesn't contain a valid '{TEMPLATE_MANIFEST}' file")
    for template in manifest:
        if not isinstance(template, dict) or "name" not in template:
            raise errors.InvalidTemplateError(f"Every template listed in '{TEMPLATE_MANIFEST}' must have a name")
        name = template["name"]
        for attribute in ["folder", "description"]:
            if attribute not in template:
                raise errors.InvalidTemplateError(f"Template '{name}' does not have a '{attribute}' attribute")

        if "variables" in template:
            for key, variable in template["variables"].items():
                if isinstance(variable, str):
                    # NOTE: Backwards compatibility
                    template["variables"][key] = {"description": variable}
                elif isinstance(variable, dict) and "description" not in variable:
                    raise errors.InvalidTemplateError(f"Template parameter '{key}' does not contain a description.")
                # TODO: Check if default value is valid

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
        raise errors.InvalidTemplateError(f"There is no manifest file '{TEMPLATE_MANIFEST}'") from e
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
                raise errors.InvalidTemplateError(f"Cannot checkout the folder '{template_folder}'") from e
            validate_template(template_path)

        for template_icon in template_icons:
            try:
                repository.checkout(template_icon)
            except errors.GitCommandError as e:
                raise errors.InvalidTemplateError(f"Cannot checkout the icon '{template_icon}'") from e

    return manifest


def create_project_from_template(client, template_path, template_metadata):
    """Render template files from a template directory."""
    rendered_base, checksums = _render_template(
        client=client, action="initialize", template_base=template_path, template_metadata=template_metadata
    )

    _copy_template_files_to_project(client=client, rendered_base=rendered_base, checksums=checksums)


def set_template(client, template_source, template_ref, template_id, force, interactive, parameters):
    """Set template for a project."""
    project = client.project

    if project.template_source and not force:
        raise errors.TemplateUpdateError("Project already has a template: To set a template use '-f/--force' flag")
    if is_renku_template(template_source) and template_ref is not None:
        raise errors.ParameterError("Templates included in renku don't support specifying a template_ref")
    if not has_template_checksum(client) and not interactive:
        raise errors.TemplateUpdateError("Required template metadata doesn't exist: Use '-i/--interactive' flag")

    manifest, template_folder, template_source, template_version = fetch_template(
        template_source, template_ref, validate=False
    )

    template_data, template_id = select_template_from_manifest(manifest, template_id=template_id)

    _set_or_update_project_from_template(
        template_data=template_data,
        template_folder=template_folder,
        template_source=template_source,
        template_ref=template_ref,
        template_id=template_id,
        template_version=template_version,
        interactive=interactive,
        is_update=False,
        parameters=parameters,
    )


def update_template(interactive, client) -> bool:
    """Update project's template if possible. Return True if updated."""
    project = client.project

    if not project.template_source:
        raise errors.TemplateUpdateError("Project doesn't have a template: Use 'renku template set'")
    if not has_template_checksum(client) and not interactive:
        raise errors.TemplateUpdateError("Required template metadata doesn't exist: Use '-i/--interactive' flag")

    template_manifest, template_folder, template_source, template_version = fetch_template(
        project.template_source, project.template_ref, validate=False
    )

    if not is_template_update_available(
        template_source=template_source, template_version=template_version, project=project
    ):
        return False

    templates = [t for t in template_manifest if t["folder"] == project.template_id]
    if len(templates) != 1:
        raise errors.TemplateUpdateError(f"The template with id '{project.template_id}' is not available.")

    template_data = templates[0]

    _set_or_update_project_from_template(
        template_data=template_data,
        template_folder=template_folder,
        template_source=template_source,
        template_ref=project.template_ref,
        template_id=project.template_id,
        template_version=template_version,
        interactive=interactive,
        is_update=True,
    )

    return True


@inject.autoparams("client_dispatcher", "project_gateway")
def _set_or_update_project_from_template(
    template_data,
    template_folder,
    template_source,
    template_ref,
    template_id,
    template_version,
    interactive,
    is_update,
    client_dispatcher: IClientDispatcher,
    project_gateway: IProjectGateway,
    parameters=None,
):
    """Update project files and metadata from a template."""
    if interactive and not communication.has_prompt():
        raise errors.ParameterError("Cannot use '-i/--interactive' with no prompt")

    client = client_dispatcher.current_client
    project = client.project

    template_metadata = _get_template_metadata_from_project(
        client=client,
        parameters=parameters,
        template_source=template_source,
        template_ref=template_ref,
        template_id=template_id,
        template_version=template_version,
    )

    template_metadata = set_template_variables(
        template_data=template_data, template_metadata=template_metadata, interactive=interactive
    )

    template_base = template_folder / template_data["folder"]
    rendered_base, checksums = _render_template(
        client=client,
        action="update" if is_update else "set",
        template_base=template_base,
        template_metadata=template_metadata,
        interactive=interactive,
        checksums=_read_template_checksum(client),
        immutable_template_files=project.immutable_template_files,
    )

    _copy_template_files_to_project(client=client, rendered_base=rendered_base, checksums=checksums)

    update_project_metadata(
        project=project,
        template_metadata=template_metadata,
        immutable_template_files=template_data.get("immutable_template_files", []),
    )

    project_gateway.update_project(project)


def _copy_template_files_to_project(client, rendered_base: Path, checksums: Dict[str, str]):
    """Update project files and metadata from a template."""
    for file in rendered_base.rglob("*"):
        if file.is_dir():
            continue

        relative_path = file.relative_to(rendered_base)
        destination = client.path / relative_path

        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(file, destination, follow_symlinks=False)
        except OSError as e:
            client.repository.clean()

            raise errors.TemplateUpdateError(f"Cannot copy '{file}' to '{destination}'") from e

    write_template_checksum(client, checksums)


def _render_template(
    client,
    action,
    template_base: Path,
    template_metadata: Dict,
    interactive=False,
    checksums: Optional[Dict[str, str]] = None,
    immutable_template_files: Optional[List[str]] = None,
) -> Tuple[Path, Dict[str, str]]:
    """Render template files before copying them to the project."""
    if interactive and not communication.has_prompt():
        raise errors.ParameterError("Cannot use '-i/--interactive' with no prompt")

    checksums = checksums or {}
    immutable_template_files = immutable_template_files or []

    render_base = Path(mkdtemp())

    sorted_files = sorted(template_base.rglob("*"))
    new_checksums = {}

    for file in sorted_files:
        # NOTE: Ignore directories since a project is a git repo
        if file.is_dir():
            continue

        relative_path = str(file.relative_to(template_base))
        # NOTE: The path could contain template variables, we need to template it
        relative_path = Template(relative_path).render(template_metadata)

        path_in_project = client.path / relative_path

        if path_in_project.is_dir():
            raise errors.TemplateUpdateError(
                f"Cannot copy a file '{relative_path}' from template to the directory '{relative_path}'"
            )

        template = Template(file.read_text(), keep_trailing_newline=True)
        rendered_content = template.render(template_metadata)

        render_path = render_base / relative_path
        render_path.parent.mkdir(parents=True, exist_ok=True)

        if action == "initialize":
            _render_file_for_initialize(
                render_path=render_path,
                path_in_project=path_in_project,
                relative_path=relative_path,
                rendered_content=rendered_content,
            )
        elif action == "set":
            _render_file_for_set(
                render_path=render_path,
                path_in_project=path_in_project,
                relative_path=relative_path,
                rendered_content=rendered_content,
                interactive=interactive,
            )
        elif action == "update":
            _render_file_for_update(
                render_path=render_path,
                path_in_project=path_in_project,
                relative_path=relative_path,
                rendered_content=rendered_content,
                checksums=checksums,
                interactive=interactive,
                immutable_template_files=immutable_template_files,
            )
        else:
            raise errors.ParameterError(f"Invalid template render action: {action}")

        checksum = hash_file(render_path) or hash_file(path_in_project)
        if checksum is not None:
            new_checksums[relative_path] = checksum

    return render_base, new_checksums


def _render_file_for_initialize(
    render_path: Path,
    path_in_project: Path,
    relative_path: str,
    rendered_content: str,
):
    def should_append(path: str):
        return path.lower() in TEMPLATE_INIT_APPEND_FILES

    def should_keep(path: str):
        return path.lower() in TEMPLATE_KEEP_FILES

    if not path_in_project.exists():
        communication.echo(f"Initializing {relative_path} ...")
        render_path.write_text(rendered_content)
    elif should_append(relative_path):
        communication.echo(f"Appending to {relative_path} ...")
        render_path.write_text(path_in_project.read_text() + "\n" + rendered_content)
    elif should_keep(relative_path):
        communication.echo(f"Keeping file {relative_path} ...")
    else:
        communication.echo(f"Overwriting {relative_path} ...")
        render_path.write_text(rendered_content)


def _render_file_for_set(
    render_path: Path,
    path_in_project: Path,
    relative_path: str,
    rendered_content: str,
    interactive: bool,
):
    """Decide what to do with files in a project when setting a template."""

    def should_keep(path: str):
        return path.lower() in TEMPLATE_KEEP_FILES

    file_exists = path_in_project.exists()

    if not file_exists:
        communication.echo(f"Initializing {relative_path} ...")
        render_path.write_text(rendered_content)
    elif interactive:
        if communication.confirm(f"Overwrite {relative_path}?", default=True):
            render_path.write_text(rendered_content)
    elif should_keep(relative_path):
        communication.echo(f"Keeping file {relative_path} ...")
    else:
        communication.echo(f"Overwriting {relative_path} ...")
        render_path.write_text(rendered_content)


def _render_file_for_update(
    render_path: Path,
    path_in_project: Path,
    relative_path: str,
    rendered_content: str,
    checksums: Optional[Dict[str, str]],
    interactive: bool,
    immutable_template_files: List[str],
):
    """Decide what to do with files in a project when updating a template."""
    current_hash = hash_file(path_in_project)
    local_changes = current_hash != checksums.get(relative_path)
    new_hash = hash_str(rendered_content)
    remote_changes = new_hash != checksums.get(relative_path)
    file_exists = path_in_project.exists()
    file_deleted = not file_exists and relative_path in checksums

    if not file_exists and not file_deleted:
        communication.echo(f"Initializing {relative_path} ...")
        render_path.write_text(rendered_content)
    elif interactive:
        action = "Recreate deleted" if file_deleted else "Overwrite"
        if communication.confirm(f"{action} {relative_path}?", default=True):
            render_path.write_text(rendered_content)
    elif not remote_changes:
        communication.echo(f"Ignoring unchanged template file {relative_path} ...")
    elif file_deleted or local_changes:
        if relative_path in immutable_template_files:
            # NOTE: There are local changes in a file that should not be changed by users, and the file was
            # updated in the template as well. So the template can't be updated.
            raise errors.TemplateUpdateError(
                f"Can't update template as immutable template file '{relative_path}' has local changes."
            )

        # NOTE: Don't overwrite files that are modified by users
        message = "Ignoring deleted file" if file_deleted else "Keeping"
        communication.echo(f"{message} {relative_path} ...")
    else:
        communication.echo(f"Overwriting {relative_path} ...")
        render_path.write_text(rendered_content)


def update_project_metadata(project, template_metadata, immutable_template_files):
    """Update template-related metadata in a project."""
    project.template_source = template_metadata["__template_source__"]
    project.template_ref = template_metadata["__template_ref__"]
    project.template_id = template_metadata["__template_id__"]
    project.template_version = template_metadata["__template_version__"]
    project.immutable_template_files = immutable_template_files
    project.automated_update = True  # NOTE: This field will be deprecated
    project.template_metadata = json.dumps(template_metadata)


def _get_template_metadata_from_project(
    client,
    parameters: Optional[Dict],
    template_source: str,
    template_ref: Optional[str],
    template_id: str,
    template_version: str,
) -> Dict:
    """Read template-related metadata from a project."""
    parameters = parameters or {}

    metadata = json.loads(client.project.template_metadata) if client.project.template_metadata else {}
    metadata.update(parameters)

    if is_renku_template(template_source):
        template_ref = None

    metadata["__template_source__"] = template_source
    metadata["__template_ref__"] = template_ref
    metadata["__template_id__"] = template_id
    metadata["__template_version__"] = template_version

    # NOTE: Always set __renku_version__ to the value read from the Dockerfile (if available) since setting/updating the
    # template doesn't change project's metadata version and shouldn't update the Renku version either
    renku_version = metadata.get("__renku_version__")
    metadata["__renku_version__"] = _read_renku_version_from_dockerfile(client.docker_path) or renku_version or ""

    return metadata
