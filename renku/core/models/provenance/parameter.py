# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 - Swiss Data Science Center (SDSC)
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
"""Classes for tracking parameter values in provenance."""

from itertools import chain
from typing import Any
from uuid import uuid4

from renku.core import errors
from renku.core.models.workflow.plan import Plan


class ParameterValue:
    """Value for a parameter in provenance."""

    def __init__(self, *, id: str, parameter_id: str, value: Any):
        self.id = id
        self.parameter_id: str = parameter_id
        self.value: Any = value

    @staticmethod
    def generate_id(activity_id: str) -> str:
        """Generate a default id."""
        return f"{activity_id}/parameter-value/{uuid4().hex}"

    def apply_value_to_parameter(self, plan: Plan) -> None:
        """Apply the current value as actual_value on the plan's parameter."""
        for parameter in chain(plan.inputs, plan.outputs, plan.parameters):
            if parameter.id == self.parameter_id:
                parameter.actual_value = self.value
                return

        raise errors.ParameterError(f"Parameter {self.parameter_id} not found on plan {plan.id}.")
