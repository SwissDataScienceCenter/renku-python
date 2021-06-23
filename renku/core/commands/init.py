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
import tempfile
from collections import OrderedDict, namedtuple
from pathlib import Path
from tempfile import mkdtemp
from uuid import uuid4

import attr
import click
import git
import pkg_resources
import yaml

from renku.core import errors
from renku.core.commands.git import set_git_home
from renku.core.incubation.command import Command
from renku.core.management.config import RENKU_HOME
from renku.core.management.repository import INIT_APPEND_FILES, INIT_KEEP_FILES
from renku.core.models.tabulate import tabulate
from renku.core.utils import communication
from renku.version import __version__, is_release

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
        """Extract variables from tempalte manifest."""
        if describe:
            return "\n".join(
                [f"{variable[0]}: {variable[1]}" for variable in template_elem.get("variables", {}).items()]
            )

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
    Path(value).mkdir(parents=True, exist_ok=True)
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
        if template_index > 0 and template_index <= len(template_manifest):
            template_data = template_manifest[template_index - 1]
        else:
            communication.echo(f"The template at index {template_index} is not available.")
            repeat = True

    # NOTE: prompt user in case of wrong or missing selection criteria
    if prompt and (repeat or not (template_id or template_index)):
        templates = [template_elem for template_elem in template_manifest]
        if len(templates) == 1:
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


def verify_template_variables(template_data, metadata):
    """Verifies that template variables are correcly set."""
    template_variables = template_data.get("variables", {})
    template_variables_keys = set(template_variables.keys())
    input_parameters_keys = set(metadata.keys())
    for key in template_variables_keys - input_parameters_keys:
        value = communication.prompt(
            msg=(f'The template requires a value for "{key}" ' f"({template_variables[key]})"),
            default="",
            show_default=False,
        )
        metadata[key] = value

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


def get_existing_template_files(client, template_path, metadata, force=False):
    """Gets files in the template that already exists in the repo."""
    template_files = list(client.get_template_files(template_path, metadata))

    existing = []

    for rel_path in template_files:
        destination = client.path / rel_path

        if destination.exists():
            existing.append(str(rel_path))

    return existing


def create_backup_branch(client, path):
    """Creates a backup branch of the repo."""
    branch_name = None
    if not is_path_empty(path):

        try:
            if client.repo.head.is_valid():
                commit = client.repo.head.commit.commit
                hexsha = commit.hexsha[:7]

                branch_name = f"pre_renku_init_{hexsha}"

                for ref in client.repo.references:
                    if branch_name == ref.name:
                        branch_name = f"pre_renku_init_{hexsha}_{uuid4().hexsha}"
                        break

                with client.worktree(
                    branch_name=branch_name,
                    commit=commit,
                    merge_args=["--no-ff", "-s", "recursive", "-X", "ours", "--allow-unrelated-histories"],
                ):
                    communication.warn("Saving current data in branch {0}".format(branch_name))
        except AttributeError:
            communication.echo("Warning! Overwriting non-empty folder.")
        except git.exc.GitCommandError as e:
            raise errors.GitError(e)

    return branch_name


def _init(
    client,
    ctx,
    external_storage_requested,
    path,
    name,
    template_id,
    template_index,
    template_source,
    template_ref,
    metadata,
    list_templates,
    force,
    describe,
    data_dir,
    initial_branch,
):
    """Initialize a renku project."""
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

    # set local path and storage
    store_directory(path)
    if not client.external_storage_requested:
        external_storage_requested = False
    ctx.obj = client = attr.evolve(
        client, path=path, data_dir=data_dir, external_storage_requested=external_storage_requested
    )

    communication.echo("Initializing Git repository...")
    client.init_repository(force, None, initial_branch=initial_branch)

    # supply additional metadata
    metadata["__template_source__"] = template_source
    metadata["__template_ref__"] = template_ref
    metadata["__template_id__"] = template_id
    metadata["__namespace__"] = ""
    metadata["__sanitized_project_name__"] = ""
    metadata["__repository__"] = ""
    metadata["__project_slug__"] = ""
    if is_release() and "__renku_version__" not in metadata:
        metadata["__renku_version__"] = __version__
    metadata["name"] = name

    template_path = template_folder / template_data["folder"]

    existing = get_existing_template_files(client, template_path, metadata, force)

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

    branch_name = create_backup_branch(client, path)

    # clone the repo
    communication.echo("Initializing new Renku repository... ")
    with client.lock:
        try:
            create_from_template(
                template_path=template_path,
                client=client,
                name=name,
                metadata=metadata,
                template_version=template_version,
                immutable_template_files=template_data.get("immutable_template_files", []),
                automated_update=template_data.get("allow_template_update", False),
                force=force,
                data_dir=data_dir,
            )
        except FileExistsError as e:
            raise errors.InvalidFileOperation(e)
    if branch_name:
        communication.echo(
            "Project initialized.\n"
            f"You can undo this command by running 'git reset --hard {branch_name}'\n"
            f"You can see changes made by running 'git diff {branch_name} {client.repo.head.ref.name}'"
        )
    else:
        communication.echo("Project initialized.")


def init_command():
    """Init command builder."""
    return Command().command(_init)


def fetch_template_from_git(source, ref=None, tempdir=None):
    """Fetch the template from a git repository and checkout the relevant data.

    :param source: url or full path of the templates repository
    :param ref: reference for git checkout - branch, commit or tag
    :param tempdir: temporary work directory path
    :return: tuple of (template folder, template version)
    """
    if tempdir is None:
        tempdir = Path(tempfile.mkdtemp())

    try:
        # clone the repo locally without checking out files
        template_repo = git.Repo.clone_from(source, tempdir, no_checkout=True)
    except git.exc.GitCommandError as e:
        raise errors.GitError("Cannot clone repo from {0}".format(source)) from e

    if ref:
        try:
            # fetch ref and set the HEAD
            template_repo.remotes.origin.fetch()
            try:
                template_repo.head.reset(template_repo.commit(ref))
            except git.exc.BadName:
                ref = "origin/{0}".format(ref)
                template_repo.head.reset(template_repo.commit(ref))
            git_repo = git.Git(str(tempdir))
        except (git.exc.GitCommandError, git.exc.BadName) as e:
            raise errors.GitError("Cannot fetch and checkout reference {0}".format(ref)) from e
    else:
        template_repo.remotes.origin.fetch()
        template_repo.head.reset(template_repo.commit())
        git_repo = git.Git(str(tempdir))

    # checkout the manifest
    try:
        git_repo.checkout(TEMPLATE_MANIFEST)
    except git.exc.GitCommandError as e:
        raise errors.GitError("Cannot checkout manifest file {0}".format(TEMPLATE_MANIFEST)) from e

    return tempdir / TEMPLATE_MANIFEST, template_repo.head.commit.hexsha


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

        if template_ref:
            raise errors.ParameterError("Templates included in renku don't support specifying a template_ref")

        template_folder = Path(pkg_resources.resource_filename("renku", "templates"))
        template_manifest = read_template_manifest(template_folder)
        template_source = "renku"
        template_version = __version__

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
    return True


def read_template_manifest(folder, checkout=False):
    """Extract template metadata from the manifest file.

    :param folder: path where to find the template manifest file
    :param checkout: checkout the template folder from local repo
    """
    manifest_path = folder / TEMPLATE_MANIFEST
    try:
        manifest = yaml.safe_load(manifest_path.read_text())
    except FileNotFoundError as e:
        raise errors.InvalidTemplateError('There is no manifest file "{0}"'.format(TEMPLATE_MANIFEST)) from e
    validate_template_manifest(manifest)

    if checkout:
        git_repo = git.Git(str(folder))
        template_folders = [template["folder"] for template in manifest]
        if len(template_folders) < 1:
            raise errors.InvalidTemplateError("Cannot find any valid template in manifest file")
        for template_folder in template_folders:
            template_path = folder / template_folder
            try:
                git_repo.checkout(template_folder)
            except git.exc.GitCommandError as e:
                raise errors.InvalidTemplateError('Cannot checkout the folder "{0}"'.format(template_folder)) from e
            validate_template(template_path)

    return manifest


def create_from_template(
    template_path,
    client,
    name=None,
    metadata={},
    template_version=None,
    immutable_template_files=[],
    automated_update=False,
    force=None,
    data_dir=None,
    user=None,
    commit_message=None,
):
    """Initialize a new project from a template."""

    template_files = list(client.get_template_files(template_path, metadata))

    commit_only = [f"{RENKU_HOME}/"] + template_files

    if "name" not in metadata:
        metadata["name"] = name

    with client.commit(commit_message=commit_message, commit_only=commit_only, skip_dirty_checks=True):
        with client.with_metadata(name=name) as project:
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
            data_path = client.path / data_dir
            data_path.mkdir(parents=True, exist_ok=True)
            (data_path / ".gitkeep").touch(exist_ok=True)


def _create_from_template_local(
    client,
    template_path,
    name,
    metadata={},
    default_metadata={},
    template_version=None,
    immutable_template_files=[],
    automated_template_update=False,
    user=None,
    source=None,
    ref=None,
    invoked_from=None,
    initial_branch=None,
):
    """Initialize a new project from a template.

    It creates a custom commit message and accepts custom user data.
    """
    command = "renku init" f' -n "{name}"' f' -s "{source}"' f' -r "{ref}"' f' -t "{template_path.name}"'
    parameters = "".join([f' -p "{key}"="{value}"' for key, value in metadata.items()])
    prefix = f"{invoked_from}: " if invoked_from else ""
    commit_message = f"{prefix}{command}{parameters}"

    metadata = {**default_metadata, **metadata}

    client.init_repository(False, user, initial_branch=initial_branch)

    create_from_template(
        template_path=template_path,
        client=client,
        name=name,
        metadata=metadata,
        template_version=template_version,
        immutable_template_files=immutable_template_files,
        automated_update=automated_template_update,
        force=False,
        user=user,
        commit_message=commit_message,
    )


def create_from_template_local_command():
    """Command to initialize a new project from a template."""
    return Command().command(_create_from_template_local)
