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
from typing import Dict, List, Optional, Tuple, Union

import click
import yaml
from jinja2 import Template
from packaging.version import Version

from renku.core import errors
from renku.core.commands.git import set_git_home
from renku.core.management import RENKU_HOME
from renku.core.management.command_builder.command import inject
from renku.core.management.interface.client_dispatcher import IClientDispatcher
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
TEMPLATE_KEEP_FILES = [".gitattributes", "readme.md", "readme.rst", "readme.txt", "readme"]


def read_renku_version_from_dockerfile(path: Union[Path, str]) -> Optional[str]:
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
        pass


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
    templates: List[Dict], template_id=None, template_index=None, describe=False, prompt=True
):
    """Select a template from a template manifest."""
    repeat = False
    template_data = None
    if template_id:
        if template_index:
            raise errors.ParameterError("Use either --template-id or --template-index, not both", '"--template-index"')
        template_filtered = [t for t in templates if t["folder"] == template_id]
        if len(template_filtered) == 1:
            template_data = template_filtered[0]
        else:
            communication.echo(f'The template with id "{template_id}" is not available.')
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

        valid, msg, value = _validate_template_variable(name, template_variable, value)

        if msg:
            communication.info(msg)

    return value


def set_template_variables(template_data, template_metadata, interactive=False):
    """Verifies that template variables are correctly set."""
    template_variables = template_data.get("variables", {})
    template_variables_keys = set(template_variables.keys())
    input_parameters_keys = set(template_metadata.keys())

    for key in template_variables:
        if "description" not in template_variables[key]:
            raise errors.InvalidTemplateError(f"Template parameter {key} does not contain a description.")

    for key in sorted(template_variables_keys.intersection(input_parameters_keys)):
        valid, msg, template_metadata[key] = _validate_template_variable(
            key, template_variables[key], template_metadata[key]
        )

        if not valid:
            communication.info(msg)
            template_metadata[key] = prompt_for_value(key, template_variables[key])
        elif interactive:
            template_metadata[key] = prompt_for_value(key, template_variables[key])

    for key in template_variables_keys - input_parameters_keys:
        template_metadata[key] = prompt_for_value(key, template_variables[key])

    # ignore internal variables, i.e. __\w__
    internal_keys = re.compile(r"__\w+__$")
    input_parameters_keys = set([i for i in input_parameters_keys if not internal_keys.match(i)])
    unused_variables = input_parameters_keys - template_variables_keys
    if len(unused_variables) > 0:
        unused_str = "\n\t".join(unused_variables)
        communication.info(f"These parameters are not used by the template and were ignored:\n\t{unused_str}\n")

    return template_metadata


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
        raise errors.GitError(f"Cannot checkout manifest file {TEMPLATE_MANIFEST}") from e

    return template_repository.path / TEMPLATE_MANIFEST, template_repository.head.commit.hexsha


def fetch_template(template_source: Optional[str], template_ref: Optional[str]):
    """Fetches a local or remote template.

    :param template_source: url or full path of the templates repository
    :param template_ref: reference for git checkout - branch, commit or tag
    :return: tuple of (template manifest, template folder, template source, template version)
    """
    if template_ref and not template_source:
        raise errors.ParameterError("Can't use '--template-ref' without specifying '--template-source'")

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
                    ('Template "{0}" does not have a {1} attribute'.format(template["name"], attribute))
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


@inject.autoparams()
def is_template_update_available(template_source, template_version, client_dispatcher: IClientDispatcher) -> bool:
    """Return True if template is newer than the project's template."""
    client = client_dispatcher.current_client
    project = client.project

    if not project.template_version:
        return True

    if template_source == "renku":
        template_version = Version(template_version)
        try:
            current_version = Version(project.template_version)
        except ValueError:  # NOTE: project.template_version is not a valid SemVer
            return True
        else:
            return current_version < template_version
    else:
        # TODO: We should checkout the template repository and check if the current commit is older than template commit
        return template_version != project.template_version


def update_template(template_data, template_folder, template_source, template_version, client, metadata):
    project = client.project

    communication.echo("Updating project from template...")

    template_path = template_folder / template_data["folder"]

    if not os.path.exists(client.template_checksums):
        raise errors.TemplateUpdateError("Can't update template as there are no template checksums set on the project.")

    with open(client.template_checksums, "r") as checksum_file:
        checksums = json.load(checksum_file)

    updated_files = []

    for file in template_path.rglob("*"):
        relative_path = str(file.relative_to(template_path))
        destination = client.path / relative_path

        # NOTE: the path could contain template variables, we need to template it
        destination = Path(Template(str(destination)).render(metadata))

        try:
            if not destination.exists() and relative_path not in checksums:
                # NOTE: new file in template
                local_changes = False
                remote_changes = True
            elif relative_path not in checksums:
                local_changes = True
                remote_changes = True
            else:
                template = Template(file.read_text(), keep_trailing_newline=True)
                rendered_content = template.render(metadata)
                new_template_hash = hash_str(rendered_content)
                current_hash = hash_file(destination) if destination.exists() else None

                local_changes = current_hash != checksums[relative_path]
                remote_changes = new_template_hash != checksums[relative_path]

            if local_changes:
                if remote_changes and relative_path in project.immutable_template_files:
                    # NOTE: There are local changes in a file that should not be changed by users,
                    # and the file was updated in the template as well. So the template can't be updated.
                    raise errors.TemplateUpdateError(
                        f"Can't update template as immutable template file {relative_path} has local changes."
                    )
                continue
            elif not remote_changes:
                continue

            destination.write_text(rendered_content)
        except IsADirectoryError:
            destination.mkdir(parents=True, exist_ok=True)
        except TypeError:
            shutil.copy(file, destination)

    updated = "\n".join(updated_files)
    communication.echo(f"Updated project from template, updated files:\n{updated}")

    project.template_version = str(template_version)
    project_gateway.update_project(project)

    return True, project.template_version, template_version


@inject.autoparams()
def render_template(
    template_base: Path, template_metadata: Dict, client_dispatcher: IClientDispatcher, interactive=False
) -> Path:
    """Render template files before copying them to the project."""

    def should_confirm(path: str):
        return os.path.basename(path).lower() != ".gitkeep"

    def should_overwrite(path: str):
        return path.lower() not in TEMPLATE_KEEP_FILES

    client = client_dispatcher.current_client
    render_base = Path(mkdtemp())

    for file in template_base.rglob("*"):
        # NOTE: Ignore directories since a project is a git repo
        if file.is_dir():
            continue

        relative_path = str(file.relative_to(template_base))
        # NOTE: The path could contain template variables, we need to template it
        rendered_relative_path = Template(relative_path).render(template_metadata)

        path_in_project = client.path / rendered_relative_path

        if path_in_project.exists():
            if path_in_project.is_dir():
                raise errors.TemplateUpdateError(
                    f"Cannot copy file '{relative_path}' from template to directory '{rendered_relative_path}'"
                )

            if (
                interactive
                and should_confirm(rendered_relative_path)
                and not communication.confirm(f"Overwrite {relative_path}?", default=True)
            ):
                continue
            elif not should_overwrite(rendered_relative_path):
                continue

        template = Template(file.read_text(), keep_trailing_newline=True)
        rendered_content = template.render(template_metadata)

        render_path = render_base / rendered_relative_path
        render_path.parent.mkdir(parents=True, exist_ok=True)
        render_path.write_text(rendered_content)

        # TODO: print a summary of overwritten and ignored files

    return render_base


def set_template(
    template_data: Dict, template_folder: Path, template_version, client, metadata, interactive: bool = False
):
    project = client.project

    template_path = template_folder / template_data["folder"]

    updated_files = []

    for file in template_path.rglob("*"):
        # NOTE: Ignore directories since a project is a git repo
        if file.is_dir():
            continue

        relative_path = str(file.relative_to(template_path))
        destination = client.path / relative_path

        # NOTE: the path could contain template variables, we need to template it
        destination = Path(Template(str(destination)).render(metadata))

        if destination.exists():
            if destination.is_dir():
                raise errors.TemplateUpdateError(
                    f"Cannot copy file '{relative_path}' from template to directory '{destination}'"
                )
        else:
            destination.parent.mkdir(parents=True, exist_ok=True)

        try:
            if not destination.exists() and relative_path not in checksums:
                # NOTE: new file in template
                local_changes = False
                remote_changes = True
            elif relative_path not in checksums:
                local_changes = True
                remote_changes = True
            else:
                template = Template(file.read_text(), keep_trailing_newline=True)
                rendered_content = template.render(metadata)
                new_template_hash = hash_str(rendered_content)
                current_hash = hash_file(destination) if destination.exists() else None

                local_changes = current_hash != checksums[relative_path]
                remote_changes = new_template_hash != checksums[relative_path]

            if local_changes:
                if remote_changes and relative_path in project.immutable_template_files:
                    # NOTE: There are local changes in a file that should not be changed by users,
                    # and the file was updated in the template as well. So the template can't be updated.
                    raise TemplateUpdateError(
                        f"Can't update template as immutable template file {relative_path} has local changes."
                    )
                continue
            elif not remote_changes:
                continue

            destination.write_text(rendered_content)
        except IsADirectoryError:
            destination.mkdir(parents=True, exist_ok=True)
        except TypeError:
            shutil.copy(file, destination)

    updated = "\n".join(updated_files)
    communication.echo(f"Updated project from template, updated files:\n{updated}")

    project.template_version = str(template_version)
    project_gateway.update_project(project)

    return True, project.template_version, template_version


def write_template_checksum(client, checksums: Dict):
    """Write templates checksum files for a project."""
    with open(client.template_checksums, "w") as checksum_file:
        json.dump(checksums, checksum_file)


def update_project_metadata(project, template_metadata, template_version, immutable_template_files, automated_update):
    """Update template-related metadata in a project."""
    project.template_source = template_metadata["__template_source__"]
    project.template_ref = template_metadata["__template_ref__"]
    project.template_id = template_metadata["__template_id__"]
    project.template_version = str(template_version) if template_version else None
    project.immutable_template_files = immutable_template_files
    project.automated_update = automated_update
    project.template_metadata = json.dumps(template_metadata)
