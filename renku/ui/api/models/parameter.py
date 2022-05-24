# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
r"""Renku API Workflow Models.

Input and Output classes can be used to define inputs and outputs of a script
within the same script. Paths defined with these classes are added to explicit
inputs and outputs in the workflow's metadata. For example, the following
mark a ``data/data.csv`` as an input with name ``my-input`` to the script:

.. code-block:: python

    from renku.api import Input

    with open(Input("my-input", "data/data.csv")) as input_data:
        for line in input_data:
            print(line)


Users can track parameters' values in a workflow by defining them using
``Parameter`` function.

.. code-block:: python

    from renku.api import Parameter

    nc = Parameter(name="n_components", value=10)

    print(nc.value)  # 10

Once a Parameter is tracked like this, it can be set normally in commands like
``renku workflow execute`` with the ``--set`` option to override the value.

"""

from os import PathLike, environ
from pathlib import Path
from typing import Any, List, Optional, Union

from renku.core import errors
from renku.core.plugin.provider import RENKU_ENV_PREFIX
from renku.core.workflow.plan_factory import (
    add_indirect_parameter,
    add_to_files_list,
    get_indirect_inputs_path,
    get_indirect_outputs_path,
)
from renku.domain_model.workflow import parameter as core_parameter
from renku.domain_model.workflow import plan as core_plan
from renku.ui.api.util import ensure_project_context


def _validate_name(name: str):
    if not name:
        raise errors.ParameterError("'name' must be set.")

    core_plan.Plan.validate_name(name)


class _PathBase(PathLike):
    """Base class of API input/output parameters."""

    @ensure_project_context
    def __init__(self, name: str, path: Union[str, Path], project=None, skip_addition: bool = False):
        env_value = environ.get(f"{RENKU_ENV_PREFIX}{name}", None)
        if env_value:
            self._path = Path(env_value)
        else:
            self._path = Path(path)

            if not skip_addition:
                indirect_list_path = self._get_indirect_list_path(project.path)
                add_to_files_list(indirect_list_path, name, self._path)

    @staticmethod
    def _get_indirect_list_path(project_path):
        raise NotImplementedError

    def __fspath__(self):
        """Abstract method of PathLike."""
        return str(self._path)

    @property
    def path(self) -> Path:
        """Return path of file."""
        return self._path


class Parameter:
    """API Parameter model."""

    @ensure_project_context
    def __init__(self, name: str, value: Union[Path, str, bool, int, float], project, skip_addition: bool = False):
        _validate_name(name)

        value = self._get_parameter_value(name=name, value=value)

        self.name: str = name
        self.value: Union[Path, str, bool, int, float] = value
        self.default_value: Union[Path, str, bool, int, float] = value
        self.description: Optional[str] = None
        self.position: Optional[int] = None
        self.prefix: Optional[str] = None

        if not skip_addition:
            add_indirect_parameter(project.path, name=name, value=value)

    @classmethod
    def from_parameter(cls, parameter: core_parameter.CommandParameter) -> "Parameter":
        """Create an instance from a core CommandParameterBase."""
        value = parameter.actual_value
        default_value = parameter.default_value
        value = value if value is not None else default_value

        self = cls(name=parameter.name, value=value, skip_addition=True)
        self.default_value = default_value if default_value is not None else value
        self.description = parameter.description
        self.position = parameter.position
        self.prefix = parameter.prefix

        return self

    @staticmethod
    def _get_parameter_value(name: str, value: Any) -> Union[str, bool, int, float]:
        """Get parameter's actual value from env variables.

        Args:
            name (str): The name of the parameter.
            value (Any): The value of the parameter.

        Returns:
            The supplied value or a value set on workflow execution.
        """
        env_value = environ.get(f"{RENKU_ENV_PREFIX}{name}", None)

        if env_value:
            if isinstance(value, str):
                value = env_value
            elif isinstance(value, bool):
                value = bool(env_value)
            elif isinstance(value, int):
                value = int(env_value)
            elif isinstance(value, float):
                value = float(env_value)
            else:
                raise errors.ParameterError(
                    f"Can't convert value '{env_value}' to type '{type(value)}' for parameter '{name}'. Only "
                    "str, bool, int and float are supported."
                )

        return value

    def __repr__(self):
        return (
            f"<Parameter '{self.name}': {self.value} (default: {self.default_value}, prefix: {self.prefix}, "
            f"position: {self.position})>"
        )


class Input(_PathBase, Parameter):
    """API Input model."""

    @staticmethod
    def _get_indirect_list_path(project_path):
        return get_indirect_inputs_path(project_path)

    def __init__(self, name: str, path: Union[str, Path], skip_addition: bool = False):
        _PathBase.__init__(self, name=name, path=path, skip_addition=skip_addition)
        Parameter.__init__(self, name=name, value=path, skip_addition=True)

        self.mapped_stream: Optional[str] = None

    @classmethod
    def from_parameter(cls, input: core_parameter.CommandInput) -> "Input":  # type: ignore
        """Create an instance from a CommandInput."""
        assert isinstance(input, core_parameter.CommandInput)

        self = cls(name=input.name, path=input.default_value, skip_addition=True)
        self.description = input.description
        self.prefix = input.prefix
        self.position = input.position
        self.mapped_stream = input.mapped_to.stream_type if input.mapped_to else None

        return self

    def __repr__(self):
        return f"<Input '{self.name}'={self.path}>"


class Output(_PathBase, Parameter):
    """API Output model."""

    @staticmethod
    def _get_indirect_list_path(project_path):
        return get_indirect_outputs_path(project_path)

    def __init__(self, name: str, path: Union[str, Path], skip_addition: bool = False):
        _PathBase.__init__(self, name=name, path=path, skip_addition=skip_addition)
        Parameter.__init__(self, name=name, value=path, skip_addition=True)

        self.mapped_stream: Optional[str] = None

    @classmethod
    def from_parameter(cls, output: core_parameter.CommandOutput) -> "Output":  # type: ignore
        """Create an instance from a CommandOutput."""
        assert isinstance(output, core_parameter.CommandOutput)

        self = cls(name=output.name, path=output.default_value, skip_addition=True)
        self.description = output.description
        self.prefix = output.prefix
        self.position = output.position
        self.mapped_stream = output.mapped_to.stream_type if output.mapped_to else None

        return self

    def __repr__(self):
        return f"<Output '{self.name}'={self.path}>"


class Link:
    """Parameter Link API model."""

    def __init__(self, source: Parameter, sinks: List[Parameter]):
        self.source: Parameter = source
        self.sinks: List[Parameter] = sinks

    @classmethod
    def from_link(cls, link: core_parameter.ParameterLink) -> "Link":
        """Create an instance from a ParameterLink."""
        return cls(source=convert_parameter(link.source), sinks=[convert_parameter(p) for p in link.sinks])


class Mapping(Parameter):
    """Parameter Mapping API model."""

    def __init__(
        self,
        name: str,
        value: Union[Path, str, bool, int, float],
        default_value: Union[Path, str, bool, int, float] = None,
        description: Optional[str] = None,
        parameters: List[Parameter] = None,
    ):
        super().__init__(name=name, value=value)
        self.default_value: Union[Path, str, bool, int, float] = default_value if default_value is not None else value
        self.description: Optional[str] = description
        self.parameters: List[Parameter] = parameters or []

    @classmethod
    def from_parameter(cls, mapping: core_parameter.CommandParameterBase) -> "Mapping":
        """Create an instance from a ParameterMapping."""
        assert isinstance(mapping, core_parameter.ParameterMapping)

        return cls(
            name=mapping.name,
            value=mapping.actual_value,
            default_value=mapping.default_value,
            description=mapping.description,
            parameters=[convert_parameter(p) for p in mapping.mapped_parameters],
        )


def convert_parameter(
    parameter: Union[core_parameter.CommandParameterBase],
) -> Union[Input, Output, Parameter, Mapping]:
    """Convert a core CommandParameterBase subclass to its equivalent API class."""
    if isinstance(parameter, core_parameter.CommandInput):
        return Input.from_parameter(parameter)
    elif isinstance(parameter, core_parameter.CommandOutput):
        return Output.from_parameter(parameter)
    elif isinstance(parameter, core_parameter.CommandParameter):
        return Parameter.from_parameter(parameter)
    elif isinstance(parameter, core_parameter.ParameterMapping):
        return Mapping.from_parameter(parameter)

    raise errors.ParameterError(f"Invalid parameter type: '{type(parameter)}'")
