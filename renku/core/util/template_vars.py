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
"""Template variable utility methods."""

import datetime
from string import Formatter
from typing import Dict, Iterable

from renku.core.models.workflow.parameter import CommandParameterBase


class TemplateVariableFormatter(Formatter):
    """Template variable formatter for `CommandParameterBase`."""

    def __init__(self):
        super(TemplateVariableFormatter, self).__init__()

    def apply(self, param: str, parameters: Dict[str, str] = None) -> str:
        """Renders the parameter template into its final value."""
        return super().vformat(param, args=[datetime.datetime.now()], kwargs=parameters)

    @staticmethod
    def to_map(parameters: Iterable[CommandParameterBase]) -> Dict[str, str]:
        """Converts a list of `CommandParameterBase` into parameter name-value dictionary."""
        return dict(map(lambda x: (x.name, x.actual_value), parameters))
