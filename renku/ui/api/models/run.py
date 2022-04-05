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

    from renku.ui.api import Input

    with open(Input("my-input", "data/data.csv")) as input_data:
        for line in input_data:
            print(line)


Users can track parameters' values in a workflow by defining them using
``Parameter`` function.

.. code-block:: python

    from renku.ui.api import Parameter

    nc = Parameter(name="n_components", value=10)

Once a Parameter is tracked like this, it can be set normally in commands like
``renku workflow execute`` with the ``--set`` option to override the value.

"""

import re
from os import PathLike, environ
from pathlib import Path
from typing import Union

from renku.core import errors
from renku.core.plugin.provider import RENKU_ENV_PREFIX
from renku.core.workflow.plan_factory import (
    add_indirect_parameter,
    add_to_files_list,
    get_indirect_inputs_path,
    get_indirect_outputs_path,
)
from renku.ui.api.models.project import ensure_project_context

name_pattern = re.compile("[a-zA-Z0-9-_]+")


class _PathBase(PathLike):
    @ensure_project_context
    def __init__(self, name: str, path: Union[str, Path], project=None):
        if not name:
            raise errors.ParameterError("'name' must be set.")

        if name and not name_pattern.match(name):
            raise errors.ParameterError(
                f"Name {name} contains illegal characters. Only characters, numbers, _ and - are allowed."
            )
        self.name = name

        env_value = environ.get(f"{RENKU_ENV_PREFIX}{name}", None)

        if env_value:
            self._path = Path(env_value)
        else:
            self._path = Path(path)

            indirect_list_path = self._get_indirect_list_path(project.path)

            add_to_files_list(indirect_list_path, name, self._path)

    @staticmethod
    def _get_indirect_list_path(project_path):
        raise NotImplementedError

    def __fspath__(self):
        """Abstract method of PathLike."""
        return str(self._path)

    @property
    def path(self):
        """Return path of file."""
        return self._path


class Input(_PathBase):
    """API Input model."""

    @staticmethod
    def _get_indirect_list_path(project_path):
        return get_indirect_inputs_path(project_path)


class Output(_PathBase):
    """API Output model."""

    @staticmethod
    def _get_indirect_list_path(project_path):
        return get_indirect_outputs_path(project_path)


@ensure_project_context
def parameter(name, value, project):
    """Store parameter's name and value.

    Args:
        name (str): The name of the parameter.
        value (Any): The value of the parameter.
        project: The current project.

    Returns:
        The supplied value or a value set on workflow execution.
    """
    if not name:
        raise errors.ParameterError("'name' must be set.")

    if not name_pattern.match(name):
        raise errors.ParameterError(
            f"Name {name} contains illegal characters. Only characters, numbers, _ and - are allowed."
        )
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

    add_indirect_parameter(project.path, name=name, value=value)

    return value


Parameter = parameter
