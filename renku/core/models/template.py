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
"""Template models."""

import copy
import os
import tempfile
from abc import abstractmethod
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

import jinja2
import yaml

from renku.core import errors
from renku.core.management import RENKU_HOME

TEMPLATE_MANIFEST = "manifest.yaml"


# TODO: Should we validate the templates in the constructor


class TemplatesSource:
    """Base class for Renku template sources."""

    def __init__(self, path, source, reference, version):
        self.path: Path = Path(path)
        self.source: str = source
        self.reference: Optional[str] = reference
        self.version: str = version
        self.manifest: TemplatesManifest = TemplatesManifest.from_path(path / TEMPLATE_MANIFEST)

    @classmethod
    @abstractmethod
    def fetch(cls, source: Optional[str], reference: Optional[str]) -> "TemplatesSource":
        """Fetch an embedded or remote template."""
        raise NotImplementedError

    @property
    def templates(self) -> List["SourceTemplate"]:
        """Return list of templates."""
        for template in self.manifest.templates_ha:
            template.templates_source = self

        return self.manifest.templates_ha

    @abstractmethod
    def is_update_available(self, id: str, reference: Optional[str], version: Optional[str]) -> Tuple[bool, str]:
        """Return True if an update is available along with the latest version of a template."""
        raise NotImplementedError

    @abstractmethod
    def get_all_versions(self, id) -> List[str]:
        """Return all available versions for a template id."""
        raise NotImplementedError

    @abstractmethod
    def get_latest_version(self, id: str, reference: Optional[str], version: Optional[str]) -> Optional[str]:
        """Return latest version number of a template."""
        raise NotImplementedError

    @abstractmethod
    def get_template(self, id, reference: Optional[str]) -> Optional["SourceTemplate"]:
        """Return a template at a specific reference."""
        raise NotImplementedError


class TemplatesManifest:
    """Manifest file for Renku templates."""

    def __init__(self, content: List[Dict]):
        self._content: List[Dict] = content
        self._templates: Optional[List[SourceTemplate]] = None

    @classmethod
    def from_path(cls, path: Union[Path, str]) -> "TemplatesManifest":
        """Extract template metadata from the manifest file."""
        try:
            return cls.from_string(Path(path).read_text())
        except FileNotFoundError as e:
            raise errors.InvalidTemplateError(f"There is no manifest file '{path}'") from e
        except UnicodeDecodeError as e:
            raise errors.InvalidTemplateError(f"Cannot read manifest file '{path}'") from e

    @classmethod
    def from_string(cls, content: str) -> "TemplatesManifest":
        """Extract template metadata from the manifest file."""
        try:
            manifest = yaml.safe_load(content)
        except yaml.YAMLError as e:
            raise errors.InvalidTemplateError("Cannot parse manifest file") from e
        else:
            manifest = TemplatesManifest(manifest)
            manifest.validate()
            return manifest

    @property
    def templates_ha(self) -> List["SourceTemplate"]:  # TODO
        """Return list of available templates info in the manifest."""
        if self._templates is None:
            self._templates: List[SourceTemplate] = [
                SourceTemplate(
                    id=t.get("id") or t.get("folder"),
                    name=t.get("name"),
                    description=t.get("description"),
                    parameters=t.get("parameters") or t.get("variables"),
                    icon=t.get("icon"),
                    immutable_files=t.get("immutable_template_files", []),
                    allow_update=t.get("allow_template_update", True),
                    source=None,
                    reference=None,
                    version=None,
                    path=None,
                    templates_source=None,
                )
                for t in self._content
            ]

        return self._templates

    def get_raw_content(self) -> List[Dict]:
        """Return raw manifest file content."""
        return copy.deepcopy(self._content)

    def validate(self):
        """Validate manifest content."""
        if not self._content:
            raise errors.InvalidTemplateError("Cannot find any valid template in manifest file")
        elif not isinstance(self._content, list):
            raise errors.InvalidTemplateError(f"Invalid manifest content type: '{type(self._content).__name__}'")

        # NOTE: First check if required fields exists for creating Template instances
        for template in self._content:
            if not isinstance(template, dict):
                raise errors.InvalidTemplateError(f"Invalid template type: '{type(template).__name__}'")

            # TODO: Check if these two values are not the same
            # TODO: Warn about deprecations
            id = template.get("id") or template.get("folder")
            if not id:
                raise errors.InvalidTemplateError(f"Template doesn't have an id: '{template}'")

            parameters = template.get("parameters") or template.get("variables")
            if parameters:
                if not isinstance(parameters, dict):
                    raise errors.InvalidTemplateError(f"Invalid template variable type: '{type(parameters).__name__}'")

                for key, parameter in parameters.items():
                    if isinstance(parameter, str):  # NOTE: Backwards compatibility
                        template["variables"][key] = {"description": parameter}

        for template in self.templates_ha:
            template.validate()


class SourceTemplate:
    """Template files and metadata from a template source."""

    REQUIRED_FILES = (os.path.join(RENKU_HOME, "renku.ini"), "Dockerfile")

    def __init__(
        self,
        id: str,
        name: str,
        description: str,
        parameters: Dict[str, Dict[str, Any]],
        icon: str,
        immutable_files: List[str],
        allow_update: bool,
        source: Optional[str],
        reference: Optional[str],
        version: Optional[str],
        path: Optional[Path],
        templates_source: Optional[TemplatesSource],
    ):
        self.path: Path = path
        self.source = source
        self.reference = reference
        self.version = version
        self.id: str = id
        self.name: str = name
        self.description: str = description
        self.icon = icon
        self.immutable_files: List[str] = immutable_files or []
        self.allow_update: bool = allow_update
        parameters = parameters or {}
        self.parameters: List[TemplateParameter] = [
            TemplateParameter.from_dict(name=k, value=v) for k, v in parameters.items()
        ]

        self._templates_source: Optional[TemplatesSource] = templates_source

    @property
    def templates_source(self) -> TemplatesSource:
        """Return template's source."""
        return self._templates_source

    @templates_source.setter
    def templates_source(self, templates_source: TemplatesSource):
        """Set templates source for this template."""
        self._templates_source = templates_source
        self.source = templates_source.source
        self.reference = templates_source.reference
        self.version = templates_source.version
        self.path = templates_source.path / self.id

    def get_all_versions(self) -> List[str]:
        """Return all available versions for the template."""
        return self.templates_source.get_all_versions(self.id)

    def validate(self, skip_files: bool = True):
        """Validate a template."""
        for attribute in ("name", "description"):
            if not getattr(self, attribute):
                raise errors.InvalidTemplateError(f"Template '{self.id}' does not have a '{attribute}' attribute")

        for parameter in self.parameters:
            parameter.validate()

        if skip_files:
            return

        # TODO: implement a better check
        required_folders = [RENKU_HOME]
        required_files = self.REQUIRED_FILES
        for folder in required_folders:
            if not (self.path / folder).is_dir():
                raise errors.InvalidTemplateError(f"Folder '{folder}' is required for the template to be valid")
        for file in required_files:
            if not (self.path / file).is_file():
                raise errors.InvalidTemplateError(f"File '{file}' is required for the template to be valid")

        # TODO: Validate symlinks resolve to paths in projects for security
        # TODO: Check template folder and icon exist
        # TODO: Warn if there are large files in the template
        """
        template_folders = [template["folder"] for template in manifest]
        template_icons = [template["icon"] for template in manifest if "icon" in template]
        for template_folder in template_folders:
            template_path = path / template_folder
            try:
                repository.checkout(template_folder)
            except errors.GitCommandError as e:
                raise errors.InvalidTemplateError(f"Cannot checkout the folder '{template_folder}'") from e

        for template_icon in template_icons:
            try:
                repository.checkout(template_icon)
            except errors.GitCommandError as e:
                raise errors.InvalidTemplateError(f"Cannot checkout the icon '{template_icon}'") from e
        """

        return True

    def get_files(self) -> Generator[str, None, None]:
        """Return all files in a rendered renku template."""
        for subpath in self.path.rglob("*"):
            if subpath.is_file():
                yield str(subpath.relative_to(self.path))

    def render(self, metadata: Dict[str, Any]) -> "SourceTemplate":
        """Render template files in a new directory."""
        render_base = Path(tempfile.mkdtemp())

        for relative_path in self.get_files():
            # NOTE: The path could contain template variables, we need to template it
            rendered_relative_path = jinja2.Template(relative_path).render(metadata)

            destination = render_base / rendered_relative_path
            destination.parent.mkdir(parents=True, exist_ok=True)

            source = self.path / relative_path

            try:
                content = source.read_text()
            except UnicodeDecodeError:  # NOTE: Binary files
                content = source.read_bytes()
                destination.write_bytes(content)
            else:
                template = jinja2.Template(content, keep_trailing_newline=True)
                rendered_content = template.render(metadata)
                destination.write_text(rendered_content)

        result = SourceTemplate(  # TODO Use a copy constructor or deepcopy
            path=render_base,
            id=self.id,
            name=self.name,
            description=self.description,
            parameters={},
            icon=self.icon,
            immutable_files=self.immutable_files,
            allow_update=self.allow_update,
            source=self.source,
            reference=self.reference,
            version=self.version,
            templates_source=self.templates_source,
        )
        result.parameters = self.parameters

        return result


class TemplateParameter:
    """Represent template variables."""

    VALID_TYPES = ("string", "number", "boolean", "enum")

    def __init__(
        self,
        name: str,
        description: Optional[str],
        type: Optional[str],
        possible_values: Optional[List[Union[int, float, str, bool]]],
        default: Optional[Union[int, float, str, bool]],
    ):
        self.name: str = name
        self.description: str = description or ""
        self.type: Optional[str] = type
        self.possible_values: List[str] = possible_values or []
        self.default = default

    @classmethod
    def from_dict(cls, name: str, value: Dict[str, Any]):
        """Create an instance from a dict."""
        if not name:
            raise errors.InvalidTemplateError(f"No name specified for template parameter '{value}'")
        if not isinstance(value, dict):
            raise errors.InvalidTemplateError(f"Invalid parameter type '{type(value).__name__}' for '{name}'")

        return cls(
            name=name,
            type=value.get("type"),
            description=value.get("description"),
            possible_values=value.get("possible_values") or value.get("enum"),
            default=value.get("default_value"),
        )

    @property
    def has_default(self) -> bool:
        """Return True if a default value is set."""
        # NOTE: ``None`` cannot be used as the default value but it's ok since no variable type accepts it and it's not
        # a valid value anyways
        return self.default is not None

    def validate(self):
        """Validate manifest content."""
        if not self.name:
            raise errors.InvalidTemplateError("Template parameter does not have a name.")

        if self.type and self.type not in self.VALID_TYPES:
            raise errors.InvalidTemplateError(
                f"Template contains variable '{self.name}' of type '{self.type}' which is not supported"
            )

        if self.possible_values and not isinstance(self.possible_values, list):
            raise errors.InvalidTemplateError(
                f"Invalid type for possible values of template variable '{self.name}': '{self.possible_values}'"
            )

        if self.type and self.type == "enum" and not self.possible_values:
            raise errors.InvalidTemplateError(
                f"Template variable '{self.name}' of type enum does not provide a corresponding enum list"
            )

        if self.has_default:
            try:
                self.default = self.convert(self.default)
            except ValueError as e:
                raise errors.InvalidTemplateError(f"Invalid default value for '{self.name}': {e}")

        # TODO: Check list of possible values (bool cannot have it)

    def convert(self, value: Union[int, float, str, bool]) -> Union[int, float, str, bool]:
        """Convert a given value to the proper type and raise if value is not valid."""
        # TODO check for None and raise; always convert value to str and then do the rest
        valid = True

        if not self.type:
            return value
        elif self.type == "string":
            if not isinstance(value, str):
                valid = False
        elif self.type == "number":
            try:
                value = int(str(value))  # NOTE: Convert to str first to avoid converting float to int if value is float
            except ValueError:
                try:
                    value = float(value)
                except ValueError:
                    valid = False
        elif self.type == "boolean":
            true = (True, 1, "1", "true", "True")
            false = (False, 0, "0", "false", "False")
            if value not in true and value not in false:
                valid = False
            else:
                value = True if value in true else False
        elif self.type == "enum":
            if value not in self.possible_values:
                valid = False

        if not valid:
            value = f"{value} (type: {type(value).__name__})"
            kind = f"type: {self.type}" if self.type else ""
            possible_values = f"possible values: {self.possible_values}" if self.possible_values else ""
            separator = ", " if kind and possible_values else ""
            info = f" ({kind}{separator}{possible_values})" if kind or possible_values else ""
            raise ValueError(f"Invalid value '{value}' for template variable '{self.name}{info}'")

        return value
