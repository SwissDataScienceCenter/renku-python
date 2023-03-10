#
# Copyright 2018-2023 - Swiss Data Science Center (SDSC)
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
"""Renku workflow file format parser."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple, Type, Union

from renku.core import errors
from renku.core.interface.workflow_file_parser import IWorkflowFileParser
from renku.core.plugin import hookimpl
from renku.core.util.os import get_absolute_path, get_relative_path, safe_read_yaml
from renku.core.workflow.model.workflow_file import BaseParameterType, Input, Output, Parameter, Step, WorkflowFile
from renku.domain_model.project_context import project_context


class RenkuWorkflowFileParser(IWorkflowFileParser):
    """Parser for Renku workflow files."""

    @hookimpl
    def workflow_file_parser(self) -> Tuple[IWorkflowFileParser, str]:
        """Workflow file parser name."""
        return self, "renku"

    @hookimpl
    def parse(self, path: Union[Path, str]) -> WorkflowFile:
        """Parse a given workflow file using the provider.

        Returns:
            WorkflowFile: A ``WorkflowFile`` generated by the provider.
        """
        data = safe_read_yaml(path=path)

        return convert_to_workflow_file(data=data, path=path)


class Attribute:
    """Defines an attribute of a class and include its validation properties."""

    def __init__(
        self,
        name: str,
        *types: Type,
        required: bool = False,
        list_subtype: Optional[Union[Type, Tuple[Type, ...]]] = None,
    ):
        self.name: str = name
        self.types: Tuple[Type, ...] = types
        self.required: bool = required
        self.list_subtype: Optional[Union[Type, Tuple[Type, ...]]] = list_subtype

    def validate(self, value: Any, kind: str):
        """Validate a given value to have correct type."""
        if value is None:
            if not self.required:
                return
            raise errors.ParseError(f"Attribute '{self.name}' in {kind} is required and cannot be None")

        if not isinstance(value, self.types):
            valid_type_names: Union[str, List[str]] = [t.__name__ for t in self.types]
            if len(valid_type_names) == 1:
                valid_type_names = valid_type_names[0]

            raise errors.ParseError(
                f"Invalid type for attribute '{self.name}' in {kind}: Required '{valid_type_names}' but got "
                f"'{type(value).__name__}': {value}"
            )
        elif self.list_subtype:
            subtype = self.list_subtype
            if not isinstance(value, (list, tuple)):
                raise errors.ParseError(
                    f"Expected List[{subtype}] for attribute '{self.name}' in {kind} but got "
                    f"'{type(value).__name__}': {value}"
                )
            for element in value:
                if not isinstance(element, subtype):
                    if isinstance(subtype, tuple):
                        subtype_name = ", ".join(s.__name__ for s in subtype)
                        subtype_name = f"({subtype_name})"
                    else:
                        subtype_name = subtype.__name__
                    raise errors.ParseError(
                        f"Expected '{subtype_name}' for elements of attribute '{self.name}' in {kind} but "
                        f"got '{type(element).__name__}': {element}"
                    )


def convert_to_workflow_file(data: Dict[str, Any], path: Union[Path, str]) -> WorkflowFile:
    """Create an instance of ``WorkflowFile``."""
    if not isinstance(data, dict):
        raise errors.ParseError("Workflow file must be a YAML dictionary")

    validate_attributes(
        kind="workflow file",
        name=None,
        data=data,
        attributes=[
            Attribute("description", str),
            Attribute("keywords", list, list_subtype=(str, int, float)),
            Attribute("name", str, required=True),
            Attribute("steps", list, dict, required=True),
        ],
    )

    if len(data["steps"]) == 0:
        raise errors.ParseError("Workflow file must have at least one step")

    try:
        path = get_absolute_path(path=path)
        relative_path = get_relative_path(path=path, base=project_context.path)
    except errors.ProjectContextError:
        relative_path = str(path)

    # NOTE: We always store the relative path (if available) in the WorkflowFile
    path = relative_path or path
    name: str = data.get("name")  # type: ignore

    return WorkflowFile(
        path=path,
        name=name,
        description=data.get("description"),
        steps=[convert_to_step(data=e, path=path, workflow_file_name=name) for e in get_elements(data["steps"])],
        keywords=data.get("keywords"),
    )


def convert_to_step(data: Union[Dict[str, Any], str], path: Union[Path, str], workflow_file_name: str) -> Step:
    """Create an instance of a workflow file ``Step``."""
    data = convert_element(
        kind="step",
        data=data,
        attributes=[
            Attribute("command", str, required=True),
            Attribute("keywords", list, list_subtype=(str, int, float)),
            Attribute("description", str),
            Attribute("inputs", dict, list),
            Attribute("outputs", dict, list),
            Attribute("parameters", dict, list),
            Attribute("success_codes", list, list_subtype=int),
        ],
        allow_single_value=False,
    )

    if not isinstance(data, dict):
        raise errors.ParseError(
            f"Expected Dict[str, Any] when processing steps, but found '{type(data).__name__}': {data}"
        )
    name = data["name"]

    return Step(
        command=data["command"],
        description=data.get("description"),
        inputs=[convert_to_base_parameter(Input, data=e) for e in get_elements(data.get("inputs", []))],
        keywords=data.get("keywords"),
        name=name,
        outputs=[convert_to_base_parameter(Output, data=e) for e in get_elements(data.get("outputs", []))],
        parameters=[convert_to_base_parameter(Parameter, data=e) for e in get_elements(data.get("parameters", []))],
        path=path,
        success_codes=data.get("success_codes"),
        workflow_file_name=workflow_file_name,
    )


def convert_to_base_parameter(cls: Type[BaseParameterType], data: Union[Dict[str, Any], str]) -> BaseParameterType:
    """Create an instance of a workflow file ``Input``, ``Output``, or ``Parameter``."""
    attributes = [
        Attribute("description", str),
        Attribute("prefix", str),
        Attribute("implicit", bool),
    ]

    if cls == Parameter:
        attributes.append(Attribute("value", str, int, float, bool, required=True))
    else:
        attributes.extend(
            [
                Attribute("path", str, int, float, required=True),
                Attribute("persist", bool),
            ]
        )

    data = convert_element(kind=cls.__name__.lower(), data=data, attributes=attributes)

    if not isinstance(data, dict):
        data = {"value": data} if cls == Parameter else {"path": str(data)}
    elif cls in [Input, Output]:
        # NOTE: Path might be an int/float if user enters something like 123 as the path
        data["path"] = str(data.pop("path"))

    return cls(**data)


def get_elements(data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Generator[Dict[str, Any], None, None]:
    """Get elements of a list of dict as a dict."""
    if isinstance(data, list):
        for element in data:
            if isinstance(element, list):  # type: ignore[unreachable]
                raise errors.ParseError(f"Data cannot be a list: {element}")
            yield element
    elif not isinstance(data, dict):
        raise errors.ParseError(f"Expected Dict[str, Any] but found '{type(data).__name__}': {data}")
    else:
        for key, value in data.items():
            yield {key: value}


def convert_element(
    kind: str,
    data: Union[Dict[str, Any], str],
    attributes: List[Attribute],
    allow_single_value: bool = True,
) -> Union[Dict[str, Any], str]:
    """Convert possible element definition formats to a dict format."""
    if isinstance(data, dict) and len(data) == 1:  # NOTE: <name>: {<properties>} format
        name, value = list(data.items())[0]

        if not isinstance(value, dict):
            kind = f"{kind} '{name}'" if name else kind
            raise errors.ParseError(
                f"Expected Dict[str, Any] when processing {kind}, but found '{type(value).__name__}': {value}"
            )

        validate_attributes(kind=kind, name=name, data=value, attributes=attributes)
        data = {"name": name}
        data.update(value)
        return data
    else:  # NOTE: List of inputs/outputs/parameters
        if not allow_single_value:
            raise errors.ParseError(f"Expected Dict[str, Any] with one value but got: '{data}'")
        if not isinstance(data, (str, int, float)):
            raise errors.ParseError(f"Expected string, integer, or floating-point type but got '{type(data)}': {data}")
        return str(data)


def validate_attributes(*, kind: str, name: Optional[str], data: Dict[str, Any], attributes: List[Attribute]):
    """Validate that the loaded yaml data is valid."""
    kind = f"{kind} '{name}'" if name else kind

    if attributes:
        valid_attributes = {a.name for a in attributes}
        invalid_attributes = set(data.keys()) - valid_attributes
        if invalid_attributes:
            invalid_attributes_str = ", ".join(sorted(invalid_attributes))
            raise errors.ParseError(f"Invalid attributes for {kind}: {invalid_attributes_str}")

        for attribute in attributes:
            if attribute.name in data:
                attribute.validate(value=data[attribute.name], kind=kind)
            elif attribute.required:
                raise errors.ParseError(f"Required attribute '{attribute.name}' isn't set for {kind}: {data}")
