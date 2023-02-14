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
"""Template management."""

import json
import os
import re
import shutil
import tempfile
from enum import Enum, IntEnum, auto
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, cast

from packaging.version import Version

from renku.core import errors
from renku.core.util import communication
from renku.core.util.git import clone_repository
from renku.core.util.os import hash_file
from renku.core.util.util import to_semantic_version, to_string
from renku.domain_model.project_context import project_context
from renku.domain_model.template import (
    TEMPLATE_MANIFEST,
    RenderedTemplate,
    Template,
    TemplateMetadata,
    TemplateParameter,
    TemplatesManifest,
    TemplatesSource,
)
from renku.infrastructure.repository import Repository

try:
    import importlib_resources  # type:ignore
except ImportError:
    import importlib.resources as importlib_resources  # type:ignore

from renku.domain_model.project import Project, ProjectTemplateMetadata

TEMPLATE_KEEP_FILES = ["readme.md", "readme.rst", "readme.txt", "readme"]
TEMPLATE_INIT_APPEND_FILES = [".gitignore"]


class TemplateAction(Enum):
    """Types of template rendering."""

    INITIALIZE = auto()
    SET = auto()
    UPDATE = auto()


class FileAction(IntEnum):
    """Types of operation when copying a template to a project."""

    APPEND = 1
    CREATE = 2
    DELETED = 3
    IGNORE_IDENTICAL = 4
    IGNORE_UNCHANGED_REMOTE = 5
    KEEP = 6
    OVERWRITE = 7
    RECREATE = 8


def fetch_templates_source(source: Optional[str], reference: Optional[str]) -> TemplatesSource:
    """Fetch a template."""
    if reference and not source:
        raise errors.ParameterError("Can't use a template reference without specifying a template source")

    return (
        EmbeddedTemplates.fetch(source, reference)
        if is_renku_template(source)
        else RepositoryTemplates.fetch(source, reference)
    )


def is_renku_template(source: Optional[str]) -> bool:
    """Return if template comes from Renku."""
    return not source or source.lower() == "renku"


def write_template_checksum(checksums: Dict):
    """Write templates checksum file for a project."""
    project_context.template_checksums_path.parent.mkdir(parents=True, exist_ok=True)

    with open(project_context.template_checksums_path, "w") as checksum_file:
        json.dump(checksums, checksum_file)


def read_template_checksum() -> Dict[str, str]:
    """Read templates checksum file for a project."""
    if has_template_checksum():
        with open(project_context.template_checksums_path, "r") as checksum_file:
            return json.load(checksum_file)

    return {}


def has_template_checksum() -> bool:
    """Return if project has a templates checksum file."""
    return os.path.exists(project_context.template_checksums_path)


def copy_template_to_project(
    rendered_template: RenderedTemplate, project: "Project", actions: Dict[str, FileAction], cleanup=True
):
    """Update project files and metadata from a template."""

    def copy_template_metadata_to_project():
        """Update template-related metadata in a project."""
        write_template_checksum(rendered_template.checksums)

        project.template_metadata = ProjectTemplateMetadata(
            template_id=rendered_template.template.id,
            template_source=rendered_template.template.source,
            template_ref=rendered_template.template.reference,
            template_version=rendered_template.template.version,
            immutable_template_files=rendered_template.template.immutable_files.copy(),
            metadata=json.dumps(rendered_template.metadata),
        )

    actions_mapping: Dict[FileAction, Tuple[str, str]] = {
        FileAction.APPEND: ("append", "Appending to"),
        FileAction.CREATE: ("copy", "Initializing"),
        FileAction.DELETED: ("", "Ignoring deleted file"),
        FileAction.IGNORE_IDENTICAL: ("", "Ignoring identical file"),
        FileAction.IGNORE_UNCHANGED_REMOTE: ("", "Ignoring unchanged template file"),
        FileAction.KEEP: ("", "Keeping"),
        FileAction.OVERWRITE: ("copy", "Overwriting"),
        FileAction.RECREATE: ("copy", "Recreating deleted file"),
    }

    for relative_path, action in get_sorted_actions(actions=actions).items():
        source = rendered_template.path / relative_path
        destination = project_context.path / relative_path

        operation, message = actions_mapping[action]
        communication.echo(f"{message} {relative_path} ...")

        if not operation:
            continue

        try:
            destination.parent.mkdir(parents=True, exist_ok=True)

            if operation == "copy":
                shutil.copy(source, destination, follow_symlinks=False)
            elif operation == "append":
                destination.write_text(destination.read_text() + "\n" + source.read_text())
        except OSError as e:
            # TODO: Use a general cleanup strategy: https://github.com/SwissDataScienceCenter/renku-python/issues/736
            if cleanup:
                repository = project_context.repository
                repository.reset(hard=True)
                repository.clean()

            raise errors.TemplateUpdateError(f"Cannot write to '{destination}'") from e

    copy_template_metadata_to_project()


def get_sorted_actions(actions: Dict[str, FileAction]) -> Dict[str, FileAction]:
    """Return a sorted actions list."""
    return {k: v for k, v in sorted(actions.items(), key=lambda i: (i[1], i[0]))}


def get_file_actions(
    rendered_template: RenderedTemplate, template_action: TemplateAction, interactive
) -> Dict[str, FileAction]:
    """Render a template regarding files in a project."""
    if interactive and not communication.has_prompt():
        raise errors.ParameterError("Cannot use interactive mode with no prompt")

    old_checksums = read_template_checksum()
    try:
        immutable_files = project_context.project.template_metadata.immutable_template_files or []
    except (AttributeError, ValueError):  # NOTE: Project is not set
        immutable_files = []

    def should_append(path: str):
        return path.lower() in TEMPLATE_INIT_APPEND_FILES

    def should_keep(path: str):
        return path.lower() in TEMPLATE_KEEP_FILES

    def get_action_for_initialize(relative_path: str, destination: Path) -> FileAction:
        if not destination.exists():
            return FileAction.CREATE
        elif should_append(relative_path):
            return FileAction.APPEND
        elif should_keep(relative_path):
            return FileAction.KEEP
        else:
            return FileAction.OVERWRITE

    def get_action_for_set(relative_path: str, destination: Path, new_checksum: Optional[str]) -> FileAction:
        """Decide what to do with a template file."""
        current_checksum = hash_file(destination)

        if not destination.exists():
            return FileAction.CREATE
        if new_checksum == current_checksum:
            return FileAction.IGNORE_IDENTICAL
        elif interactive:
            overwrite = communication.confirm(f"Overwrite {relative_path}?", default=True)
            return FileAction.OVERWRITE if overwrite else FileAction.KEEP
        elif should_keep(relative_path):
            return FileAction.KEEP
        else:
            return FileAction.OVERWRITE

    def get_action_for_update(
        relative_path: str, destination: Path, old_checksum: Optional[str], new_checksum: Optional[str]
    ) -> FileAction:
        """Decide what to do with a template file."""
        current_checksum = hash_file(destination)
        local_changes = current_checksum != old_checksum
        remote_changes = new_checksum != old_checksum
        file_exists = destination.exists()
        file_deleted = not file_exists and old_checksum is not None

        if not file_deleted and new_checksum == current_checksum:
            return FileAction.IGNORE_IDENTICAL
        if not file_exists and not file_deleted:
            return FileAction.CREATE
        elif interactive:
            if file_deleted:
                recreate = communication.confirm(f"Recreate deleted {relative_path}?", default=True)
                return FileAction.RECREATE if recreate else FileAction.DELETED
            else:
                overwrite = communication.confirm(f"Overwrite {relative_path}?", default=True)
                return FileAction.OVERWRITE if overwrite else FileAction.KEEP
        elif not remote_changes:
            return FileAction.IGNORE_UNCHANGED_REMOTE
        elif file_deleted or local_changes:
            if relative_path in immutable_files:
                # NOTE: There are local changes in a file that should not be changed by users, and the file was
                # updated in the template as well. So the template can't be updated.
                raise errors.TemplateUpdateError(
                    f"Can't update template as immutable template file '{relative_path}' has local changes."
                )

            # NOTE: Don't overwrite files that are modified by users
            return FileAction.DELETED if file_deleted else FileAction.KEEP
        else:
            return FileAction.OVERWRITE

    actions: Dict[str, FileAction] = {}

    for relative_path in sorted(rendered_template.get_files()):
        destination = project_context.path / relative_path

        if destination.is_dir():
            raise errors.TemplateUpdateError(
                f"Cannot copy a file '{relative_path}' from template to the directory '{relative_path}'"
            )

        new_checksum = rendered_template.checksums[relative_path]

        if template_action == TemplateAction.INITIALIZE:
            action = get_action_for_initialize(relative_path, destination)
        elif template_action == TemplateAction.SET:
            action = get_action_for_set(relative_path, destination, new_checksum=new_checksum)
        else:
            action = get_action_for_update(
                relative_path,
                destination,
                old_checksum=old_checksums.get(relative_path),
                new_checksum=new_checksum,
            )

        actions[relative_path] = action

    return actions


def set_template_parameters(
    template: Template, template_metadata: TemplateMetadata, input_parameters: Dict[str, str], interactive=False
):
    """Set and verify template parameters' values in the template_metadata."""
    if interactive and not communication.has_prompt():
        raise errors.ParameterError("Cannot use interactive mode with no prompt")

    def validate(var: TemplateParameter, val) -> Tuple[bool, Any]:
        try:
            return True, var.convert(val)
        except ValueError as e:
            communication.info(str(e))
            return False, val

    def read_valid_value(var: TemplateParameter, default_value=None):
        """Prompt the user for a template variable and return a valid value."""
        while True:
            variable_type = f", type: {var.type}" if var.type else ""
            enum_values = f", options: {var.possible_values}" if var.possible_values else ""
            default_value = default_value or to_string(var.default)

            val = communication.prompt(
                f"Enter a value for '{var.name}' ({var.description}{variable_type}{enum_values})",
                default=default_value,
                show_default=var.has_default,
            )

            valid, val = validate(var, val)
            if valid:
                return val

    missing_values = []

    for parameter in sorted(template.parameters, key=lambda v: v.name):
        name = parameter.name
        is_valid = True

        if name in input_parameters:  # NOTE: Inputs override other values. No prompt for them in interactive mode
            is_valid, value = validate(parameter, input_parameters[name])
        elif interactive:
            value = read_valid_value(parameter, default_value=template_metadata.metadata.get(name))
        elif name in template_metadata.metadata:
            is_valid, value = validate(parameter, template_metadata.metadata[name])
        elif parameter.has_default:  # Use default value if no value is available in the metadata
            value = parameter.default
        elif communication.has_prompt():
            value = read_valid_value(parameter)
        else:
            missing_values.append(name)
            continue

        if not is_valid:
            if not communication.has_prompt():
                raise errors.TemplateUpdateError(f"Invalid value '{value}' for variable '{name}'")
            template_metadata.metadata[name] = read_valid_value(parameter)
        else:
            template_metadata.metadata[name] = value

    if missing_values:
        missing_values_str = ", ".join(missing_values)
        raise errors.TemplateUpdateError(f"Can't update template, it now requires variable(s): {missing_values_str}")

    # NOTE: Ignore internal variables, i.e. __\w__
    internal_keys = re.compile(r"^__\w+__$")
    metadata_variables = {v for v in template_metadata.metadata if not internal_keys.match(v)} | set(
        input_parameters.keys()
    )
    template_variables = {v.name for v in template.parameters}
    unused_metadata_variables = metadata_variables - template_variables
    if len(unused_metadata_variables) > 0:
        unused_str = "\n\t".join(unused_metadata_variables)
        communication.info(f"These parameters are not used by the template and were ignored:\n\t{unused_str}\n")


class EmbeddedTemplates(TemplatesSource):
    """Represent templates that are bundled with Renku.

    For embedded templates, ``source`` is "renku". In the old versioning scheme, ``version`` is set to the installed
    Renku version and ``reference`` is not set. In the new scheme, both ``version`` and ``reference`` are set to the
    template version.
    """

    @classmethod
    def fetch(cls, source: Optional[str], reference: Optional[str]) -> "EmbeddedTemplates":
        """Fetch embedded Renku templates."""
        from renku import __template_version__

        template_path = importlib_resources.files("renku") / "templates"
        with importlib_resources.as_file(template_path) as folder:
            path = Path(folder)

        return cls(path=path, source="renku", reference=__template_version__, version=__template_version__)

    def get_all_references(self, id) -> List[str]:
        """Return all available references for a template id."""
        template_exists = any(t.id == id for t in self.templates)
        return [self.reference] if template_exists and self.reference is not None else []

    def get_latest_reference_and_version(
        self, id: str, reference: Optional[str], version: Optional[str]
    ) -> Optional[Tuple[Optional[str], str]]:
        """Return latest reference and version number of a template."""
        if version is None:
            return None
        elif reference is None or reference != version:  # Old versioning scheme
            return self.reference, self.version

        try:
            current_version = Version(version)
        except ValueError:  # NOTE: version is not a valid SemVer
            return self.reference, self.version
        else:
            return (self.reference, self.version) if current_version < Version(self.version) else (reference, version)

    def get_template(self, id, reference: Optional[str]) -> "Template":
        """Return all available versions for a template id."""
        try:
            return next(t for t in self.templates if t.id == id)
        except StopIteration:
            raise errors.TemplateNotFoundError(f"The template with id '{id}' is not available.")


class RepositoryTemplates(TemplatesSource):
    """Represent a local/remote template repository.

    A template repository is checked out at a specific Git reference if one is provided. However, it's still possible to
    get available versions of templates.
    """

    def __init__(self, path, source, reference, version, repository: Repository, skip_validation: bool = False):
        super().__init__(
            path=path, source=source, reference=reference, version=version, skip_validation=skip_validation
        )
        self.repository: Repository = repository

    @classmethod
    def fetch(cls, source: Optional[str], reference: Optional[str]) -> "RepositoryTemplates":
        """Fetch a template repository."""
        ref_str = f"@{reference}" if reference else ""
        communication.echo(f"Fetching template from {source}{ref_str}... ")
        path = Path(tempfile.mkdtemp())

        try:
            repository = clone_repository(url=source, path=path, checkout_revision=reference, install_lfs=False)
        except errors.GitError as e:
            if "Cannot checkout reference" in str(e):
                raise errors.TemplateMissingReferenceError(
                    f"Cannot find the reference '{reference}' in the template repository from {source}"
                ) from e
            raise errors.InvalidTemplateError(f"Cannot clone template repository from {source}") from e

        version = repository.head.commit.hexsha

        return cls(path=path, source=source, reference=reference, version=version, repository=repository)

    def get_all_references(self, id) -> List[str]:
        """Return a list of git tags that are valid SemVer and include a template id."""
        versions = []
        for tag in self.repository.tags:
            tag = str(tag)

            version = to_semantic_version(tag)
            if not version:
                continue

            if self._has_template_at(id, reference=tag):
                versions.append(version)

        return [str(v) for v in sorted(versions)]

    def get_latest_reference_and_version(
        self, id: str, reference: Optional[str], version: Optional[str]
    ) -> Optional[Tuple[Optional[str], str]]:
        """Return latest reference and version number of a template."""
        if version is None:
            return None

        tag = None
        if reference is not None:
            tag = to_semantic_version(reference)

        # NOTE: Assume that a SemVer reference is always a tag
        if tag:
            references = self.get_all_references(id=id)
            return (references[-1], self.version) if len(references) > 0 else None

        # NOTE: Template's reference is a branch or SHA and the latest version is RepositoryTemplates' version
        return reference, self.version

    def _has_template_at(self, id: str, reference: str) -> bool:
        """Return if template id is available at a reference."""
        try:
            content = self.repository.get_content(TEMPLATE_MANIFEST, revision=reference)

            if isinstance(content, bytes):
                return False
            manifest = TemplatesManifest.from_string(cast(str, content))
        except (errors.FileNotFound, errors.InvalidTemplateError):
            return False
        else:
            return any(t.id == id for t in manifest.templates)

    def get_template(self, id, reference: Optional[str]) -> "Template":
        """Return a template at a specific reference."""
        if reference is not None and reference != self.reference:
            try:
                self.repository.checkout(reference=reference)
            except errors.GitError as e:
                raise errors.InvalidTemplateError(f"Cannot find reference '{reference}'") from e
            else:
                self.reference = reference
                self.version = self.repository.head.commit.hexsha

            try:
                manifest = TemplatesManifest.from_path(self.path / TEMPLATE_MANIFEST)
            except errors.InvalidTemplateError as e:
                raise errors.InvalidTemplateError(f"Cannot load template's manifest file at '{reference}'.") from e
            else:
                self.manifest = manifest

        template = next((t for t in self.templates if t.id == id), None)
        if template is None:
            raise errors.TemplateNotFoundError(f"The template with id '{id}' is not available at '{reference}'.")

        return template
