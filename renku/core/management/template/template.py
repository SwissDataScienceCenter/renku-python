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

# TODO Fix all error messages to avoid command line parameters

# TODO Update this comment

# TODO: Should we create a TemplateVersion class to set reference and version

A project has three attributes to specify a template: ``template_source``, ``template_version``, and ``template_ref``.
In projects that use templates that are bundled with Renku, ``template_source`` is "renku" and ``template_version`` is
set to the installed Renku version. ``template_ref`` should not be set for such projects.

For projects that use a template from a Git repository, ``template_source`` is repository's URL and ``template_version``
is set to the current HEAD commit SHA. If a Git referenced was passed when setting the template, then project's
``template_ref`` is the same as the passed reference. In this case, Renku won't update a project's template if the
reference is a fixed value (i.e. a tag or a commit SHA).
"""

import json
import re
import shutil
import tempfile
from enum import Enum, auto
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

import inject
import jinja2
from packaging.version import Version

from renku.core import errors
from renku.core.metadata.repository import Repository
from renku.core.models.template import (
    TEMPLATE_MANIFEST,
    SourceTemplate,
    TemplateParameter,
    TemplatesManifest,
    TemplatesSource,
)
from renku.core.utils import communication
from renku.core.utils.git import clone_repository
from renku.core.utils.metadata import get_renku_version
from renku.core.utils.os import hash_file
from renku.core.utils.util import to_semantic_version, to_string

try:
    import importlib_resources
except ImportError:
    import importlib.resources as importlib_resources

TEMPLATE_KEEP_FILES = ["readme.md", "readme.rst", "readme.txt", "readme"]
TEMPLATE_INIT_APPEND_FILES = [".gitignore"]


class RenderType(Enum):
    """Types of template rendering."""

    INITIALIZE = auto()
    SET = auto()
    UPDATE = auto()


class FileAction(Enum):
    """Types of operation when copying a template to a project."""

    APPEND = auto()
    DELETED = auto()
    IGNORE_IDENTICAL = auto()
    IGNORE_UNCHANGED_REMOTE = auto()
    CREATE = auto()
    KEEP = auto()
    OVERWRITE = auto()
    RECREATE = auto()


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


def write_template_checksum(client, checksums: Dict):
    """Write templates checksum file for a project."""
    client.template_checksums.parent.mkdir(parents=True, exist_ok=True)

    with open(client.template_checksums, "w") as checksum_file:
        json.dump(checksums, checksum_file)


def read_template_checksum(client) -> Dict[str, str]:
    """Read templates checksum file for a project."""
    if client.has_template_checksum():
        with open(client.template_checksums, "r") as checksum_file:
            return json.load(checksum_file)

    return {}


def update_project_metadata(project, template_metadata, immutable_files):
    """Update template-related metadata in a project."""
    project.template_source = template_metadata["__template_source__"]
    project.template_ref = template_metadata["__template_ref__"]
    project.template_id = template_metadata["__template_id__"]
    project.template_version = template_metadata["__template_version__"]
    project.immutable_template_files = immutable_files
    # TODO This is available only for ui-created projects
    project.automated_update = template_metadata.get("__automated_update__", True)
    project.template_metadata = json.dumps(template_metadata)


class RenderedTemplate:
    """A rendered version of a source template."""

    def __init__(self, path: Path, source_template: SourceTemplate, checksums: Dict[str, str]):
        self.path: Path = path
        self.source_template: SourceTemplate = source_template
        self.checksums: Dict[str, str] = checksums
        self._actions: Dict[str, FileAction] = {}

    def get_files(self) -> Generator[str, None, None]:
        """Return all files in a rendered renku template."""
        for subpath in self.path.rglob("*"):
            if not subpath.is_file():
                continue

            relative_path = str(subpath.relative_to(self.path))

            # NOTE: Make sure a checksum entry exists
            if relative_path not in self.checksums:
                self.checksums[relative_path] = hash_file(subpath)

            yield relative_path

    @property
    def actions(self) -> Dict[str, FileAction]:
        """List of available operation for each file."""
        return self._actions

    @actions.setter
    def actions(self, actions: Dict[str, FileAction]):
        """Setter for actions property."""
        self._actions = actions

    def copy_files_to_project(self, client):
        """Update project files and metadata from a template."""
        actions_mapping: Dict[FileAction, Tuple[str, str]] = {
            FileAction.APPEND: ("append", "Appending to"),
            FileAction.CREATE: ("copy", "Initializing"),
            FileAction.OVERWRITE: ("copy", "Overwriting"),
            FileAction.RECREATE: ("copy", "Recreating deleted file"),
            FileAction.DELETED: ("", "Ignoring deleted file"),
            FileAction.IGNORE_IDENTICAL: ("", "Ignoring unchanged file"),
            FileAction.IGNORE_UNCHANGED_REMOTE: ("", "Ignoring unchanged template file"),
            FileAction.KEEP: ("", "Keeping"),
        }

        for relative_path in self.get_files():
            source = self.path / relative_path
            destination = client.path / relative_path

            operation = self.actions.get(relative_path)

            action, message = actions_mapping[operation]
            communication.echo(f"{message} {relative_path} ...")

            if not action:
                continue

            try:
                destination.parent.mkdir(parents=True, exist_ok=True)

                if action == "copy":
                    shutil.copy(source, destination, follow_symlinks=False)
                elif action == "append":
                    destination.write_text(destination.read_text() + "\n" + source.read_text())
            except OSError as e:
                client.repository.clean()  # TODO: We need a general cleanup strategy at Command Builder level

                raise errors.TemplateUpdateError(f"Cannot write to '{destination}'") from e

        write_template_checksum(client, self.checksums)


class TemplateRenderer:
    """Render a template to a project."""

    def __init__(self, source_template: SourceTemplate, metadata: Dict, render_type: RenderType):
        self.source_template: SourceTemplate = source_template
        self.metadata: Dict = metadata
        self.render_type = render_type

    def render(self, client, interactive: bool = False) -> RenderedTemplate:
        """Render a template regarding files in a project."""
        if interactive and not communication.has_prompt():
            raise errors.ParameterError("Cannot use interactive mode with no prompt")

        rendered_template = self._render_source_template()

        old_checksums = read_template_checksum(client)
        try:
            immutable_files = client.project.immutable_template_files or []
        except ValueError:  # NOTE: Project is not set
            immutable_files = []
        except inject.InjectorException:  # TODO Make sure it's ok to do this
            immutable_files = []

        actions: Dict[str, FileAction] = {}

        for relative_path in rendered_template.get_files():
            destination = client.path / relative_path

            if destination.is_dir():
                raise errors.TemplateUpdateError(
                    f"Cannot copy a file '{relative_path}' from template to the directory '{relative_path}'"
                )

            if self.render_type == RenderType.INITIALIZE:
                action = self._get_action_for_initialize(relative_path, destination)
            elif self.render_type == RenderType.SET:
                action = self._get_action_for_set(relative_path, destination, interactive=interactive)
            else:
                action = self._get_action_for_update(
                    relative_path,
                    destination,
                    interactive=interactive,
                    old_checksum=old_checksums.get(relative_path),
                    new_checksum=rendered_template.checksums[relative_path],
                    immutable_files=immutable_files,
                )

            actions[relative_path] = action

        rendered_template.actions = actions

        return rendered_template

    def _render_source_template(self) -> "RenderedTemplate":
        """Render template files."""
        render_base = Path(tempfile.mkdtemp())
        checksums = {}

        for relative_path in self.source_template.get_files():
            # NOTE: The path could contain template variables, we need to template it
            rendered_relative_path = jinja2.Template(relative_path).render(self.metadata)

            rendered_path = render_base / rendered_relative_path
            rendered_path.parent.mkdir(parents=True, exist_ok=True)

            source = self.source_template.path / relative_path

            try:
                content = source.read_text()
            except UnicodeDecodeError:  # NOTE: Binary files
                content = source.read_bytes()
                rendered_path.write_bytes(content)
            else:
                template = jinja2.Template(content, keep_trailing_newline=True)
                rendered_content = template.render(self.metadata)
                rendered_path.write_text(rendered_content)

            checksums[rendered_relative_path] = hash_file(rendered_path)

        return RenderedTemplate(path=render_base, source_template=self.source_template, checksums=checksums)

    @staticmethod
    def _get_action_for_initialize(relative_path: str, destination: Path) -> FileAction:
        def should_append(path: str):
            return path.lower() in TEMPLATE_INIT_APPEND_FILES

        def should_keep(path: str):
            return path.lower() in TEMPLATE_KEEP_FILES

        if not destination.exists():
            return FileAction.CREATE
        elif should_append(relative_path):
            return FileAction.APPEND
        elif should_keep(relative_path):
            return FileAction.KEEP
        else:
            return FileAction.OVERWRITE

    @staticmethod
    def _get_action_for_set(relative_path: str, destination: Path, interactive: bool) -> FileAction:
        """Decide what to do with a template file."""

        def should_keep(path: str):
            return path.lower() in TEMPLATE_KEEP_FILES

        if not destination.exists():
            return FileAction.CREATE
        elif interactive:
            overwrite = communication.confirm(f"Overwrite {relative_path}?", default=True)
            return FileAction.OVERWRITE if overwrite else FileAction.KEEP
        elif should_keep(relative_path):
            return FileAction.KEEP
        else:
            return FileAction.OVERWRITE

    @staticmethod
    def _get_action_for_update(
        relative_path: str,
        destination: Path,
        interactive: bool,
        old_checksum: Optional[str],
        new_checksum: str,
        immutable_files: List[str],
    ) -> FileAction:
        """Decide what to do with a template file."""
        current_hash = hash_file(destination)
        local_changes = current_hash != old_checksum
        remote_changes = new_checksum != old_checksum
        file_exists = destination.exists()
        file_deleted = not file_exists and old_checksum is not None

        if not file_deleted and new_checksum == current_hash:
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
            # TODO: Check to see file is not in the updated template's immutable files

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


class EmbeddedTemplates(TemplatesSource):
    """Represent templates that are bundled with Renku."""

    @classmethod
    def fetch(cls, source: Optional[str], reference: Optional[str]) -> "EmbeddedTemplates":
        """Fetch embedded Renku templates."""
        from renku import __version__

        if reference and reference != "master":
            raise errors.ParameterError("Templates included in renku don't support specifying a template reference")

        path = importlib_resources.files("renku") / "templates"
        with importlib_resources.as_file(path) as folder:  # TODO Is this needed!?
            path = Path(folder)

        return cls(path=path, source="renku", reference=None, version=str(__version__))

    def is_update_available(self, id: str, reference: Optional[str], version: Optional[str]) -> Tuple[bool, str]:
        """Return True if an update is available along with the latest version of a template."""
        latest_version = self.get_latest_version(id=id, reference=reference, version=version)
        update_available = latest_version is not None and latest_version != version

        return update_available, latest_version

    def get_all_versions(self, id) -> List[str]:
        """Return all available versions for a template id."""
        template_exists = any(t.id == id for t in self.templates)
        return [self.version] if template_exists else []

    def get_latest_version(self, id: str, reference: Optional[str], version: Optional[str]) -> Optional[str]:
        """Return True if a newer version of template available."""
        if version is None:
            return

        template_version = Version(self.version)
        try:
            current_version = Version(version)
        except ValueError:  # NOTE: version is not a valid SemVer
            return str(template_version)
        else:
            return str(template_version) if current_version < template_version else version

    def get_template(self, id, reference: Optional[str]) -> Optional["SourceTemplate"]:
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

    def __init__(self, path, source, reference, version, repository: Repository):
        super().__init__(path=path, source=source, reference=reference, version=version)
        self.repository: Repository = repository

    @classmethod
    def fetch(cls, source: Optional[str], reference: Optional[str]) -> "RepositoryTemplates":
        """Fetch a template repository."""
        ref_str = f"@{reference}" if reference else ""
        communication.echo(f"Fetching template from {source}{ref_str}... ")
        path = Path(tempfile.mkdtemp())  # TODO Move this to a cache directory

        try:
            # TODO Template should not have stuff in LFS -> Use a project for this purpose
            repository = clone_repository(url=source, path=path, checkout_revision=reference, install_lfs=False)
        except errors.GitError as e:
            raise errors.InvalidTemplateError("Cannot clone template repository") from e

        version = repository.head.commit.hexsha

        return cls(path=path, source=source, reference=reference, version=version, repository=repository)

    def is_update_available(self, id: str, reference: Optional[str], version: Optional[str]) -> Tuple[bool, str]:
        """Return True if an update is available along with the latest version of a template."""
        latest_version = self.get_latest_version(id=id, reference=reference, version=version)
        update_available = latest_version is not None and latest_version != reference

        return update_available, latest_version

    def get_all_versions(self, id) -> List[str]:
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

    def get_latest_version(self, id: str, reference: Optional[str], version: Optional[str]) -> Optional[str]:
        """Return True if a newer version of template available."""
        if version is None:
            return

        tag = to_semantic_version(reference)

        # NOTE: Assume that a SemVer reference is always a tag
        if tag:
            versions = self.get_all_versions(id=id)
            return versions[-1] if len(versions) > 0 else None

        # NOTE: Template's reference is a branch or SHA and the latest version is RepositoryTemplates' version
        return self.version

    def _has_template_at(self, id: str, reference: str) -> bool:
        """Return if template id is available at a reference."""
        try:
            content = self.repository.get_content(TEMPLATE_MANIFEST, revision=reference)
            manifest = TemplatesManifest.from_string(content)
        except (errors.ExportError, errors.InvalidTemplateError):
            return False
        else:
            return any(t.id == id for t in manifest.templates_ha)

    def get_template(self, id, reference: Optional[str]) -> Optional["SourceTemplate"]:
        """Return a template at a specific reference."""
        if reference is not None:
            try:
                self.repository.checkout(reference=reference)
            except errors.GitError as e:
                raise errors.InvalidTemplateError(f"Cannot find reference '{reference}'") from e
            else:
                self.reference = reference
                self.version = self.repository.head.commit.hexsha

            try:
                manifest = TemplatesManifest.from_path(self.path / TEMPLATE_MANIFEST)
                manifest.validate()
            except errors.InvalidTemplateError as e:
                raise errors.InvalidTemplateError(f"Cannot load template's manifest file at '{reference}'.") from e
            else:
                self.manifest = manifest

        template = next((t for t in self.templates if t.id == id), None)
        if template is None:
            raise errors.TemplateNotFoundError(f"The template with id '{id}' is not available at '{reference}'.")

        return template


class MetadataManager:
    """Metadata required for rendering a template."""

    def __init__(self, metadata: Dict[str, Any], immutable_files: List[str]):
        self.metadata: Dict[str, Any] = metadata or {}
        self.immutable_files: List[str] = immutable_files or []

    @classmethod
    def from_metadata(cls, metadata: Dict[str, Any]) -> "MetadataManager":
        """Return an instance from a metadata dict."""
        return cls(metadata=metadata, immutable_files=[])

    @classmethod
    def from_project(cls, client) -> "MetadataManager":
        """Return an instance from reading template-related metadata from a project."""
        try:
            project = client.project
        except ValueError:
            metadata = {}
            immutable_files = []
        else:
            metadata = json.loads(project.template_metadata) if project.template_metadata else {}

            # NOTE: Make sure project's template metadata is updated
            metadata["__template_source__"] = project.template_source
            metadata["__template_ref__"] = project.template_ref
            metadata["__template_version__"] = project.template_version
            metadata["__template_id__"] = project.template_id
            # NOTE: Ignore Project.automated_update since it's default is False and won't allow any update at all

            immutable_files = project.immutable_template_files

        # NOTE: Always set __renku_version__ to the value read from the Dockerfile (if available) since setting/updating
        # the template doesn't change project's metadata version and shouldn't update the Renku version either
        renku_version = metadata.get("__renku_version__")
        metadata["__renku_version__"] = get_renku_version(client) or renku_version or ""

        return cls(metadata=metadata, immutable_files=immutable_files)

    @property
    def source(self):
        """Template source."""
        return self.metadata.get("__template_source__")

    @property
    def reference(self):
        """Template reference."""
        return self.metadata.get("__template_ref__")

    @property
    def version(self):
        """Template version."""
        return self.metadata.get("__template_version__")

    @property
    def id(self):
        """Template id."""
        return self.metadata.get("__template_id__")

    @property
    def allow_update(self) -> bool:
        """Is template updatable."""
        return self.metadata.get("__automated_update__", True)

    def update_from_template(self, template: SourceTemplate):
        """Update metadata from a template."""
        self.metadata["__template_source__"] = template.source
        self.metadata["__template_ref__"] = template.reference
        self.metadata["__template_version__"] = template.version
        self.metadata["__template_id__"] = template.id
        self.metadata["__automated_update__"] = template.allow_update
        self.immutable_files = template.immutable_files

    def update_project(self, project):
        """Update template-related metadata in a project."""
        project.template_source = self.source
        project.template_ref = self.reference
        project.template_id = self.id
        project.template_version = self.version
        project.immutable_template_files = self.immutable_files.copy()
        project.automated_update = self.allow_update
        project.template_metadata = json.dumps(self.metadata)

    def set_template_variables(self, template: SourceTemplate, input_parameters: Dict[str, str], interactive=False):
        """Verifies that template variables are correctly set."""
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
                value = read_valid_value(parameter, default_value=self.metadata.get(name))
            elif name in self.metadata:
                is_valid, value = validate(parameter, self.metadata[name])
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
                self.metadata[name] = read_valid_value(parameter)
            else:
                self.metadata[name] = value

        if missing_values:
            missing_values_str = ", ".join(missing_values)
            raise errors.TemplateUpdateError(
                f"Can't update template, it now requires variable(s): {missing_values_str}"
            )

        # NOTE: Ignore internal variables, i.e. __\w__
        internal_keys = re.compile(r"__\w+__$")
        metadata_variables = {v for v in self.metadata if not internal_keys.match(v)} | set(input_parameters.keys())
        template_variables = {v.name for v in template.parameters}
        unused_metadata_variables = metadata_variables - template_variables
        if len(unused_metadata_variables) > 0:
            unused_str = "\n\t".join(unused_metadata_variables)
            communication.info(f"These parameters are not used by the template and were ignored:\n\t{unused_str}\n")
