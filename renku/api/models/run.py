# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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
mark a ``data/data.csv`` as an input to the script:

.. code-block:: python

    from renku.api import Input

    with open(Input("data/data.csv")) as input_data:
        for line in input_data:
            print(line)


Users can track parameters' values in a workflow by defining them using
``Parameter`` function.

.. code-block:: python

    from renku.api import Parameter

    nc = Parameter(name="n_components", value=10)

"""

from os import PathLike
from pathlib import Path

from renku.api.models.project import ensure_project_context
from renku.core.models.cwl.command_line_tool import (
    add_indirect_parameter,
    get_indirect_inputs_path,
    get_indirect_outputs_path,
)


class _PathBase(PathLike):
    @ensure_project_context
    def __init__(self, path, project):
        self._path = Path(path)

        indirect_list_path = self._get_indirect_list_path(project.path)
        indirect_list_path.parent.mkdir(exist_ok=True, parents=True)
        with open(indirect_list_path, "a") as file_:
            file_.write(str(path))
            file_.write("\n")

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
    """Store parameter's name and value."""
    add_indirect_parameter(project.path, name=name, value=value)

    return value


Parameter = parameter
