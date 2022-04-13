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
from typing import Any, Iterable, Mapping, Tuple, Union

from renku.core.errors import ParameterError
from renku.domain_model.workflow.parameter import CommandParameterBase


class TemplateVariableFormatter(Formatter):
    """Template variable formatter for `CommandParameterBase`."""

    RESERVED_KEYS = ["iter_index"]

    def __init__(self):
        super(TemplateVariableFormatter, self).__init__()

    def apply(self, param: str, parameters: Mapping[str, Any] = {}) -> str:
        """Renders the parameter template into its final value."""
        try:
            return super().vformat(param, args=[datetime.datetime.now()], kwargs=parameters)
        except KeyError as e:
            raise ParameterError(f"Could not resolve the variable {str(e)}")

    def get_value(self, key, args, kwargs):
        """Ignore some special keys when formatting the variable."""
        if key in self.RESERVED_KEYS:
            return key
        return super().get_value(key, args, kwargs)

    @staticmethod
    def to_map(parameters: Iterable[Union[CommandParameterBase, Tuple[str, str]]]) -> Mapping[str, str]:
        """Converts a list of `CommandParameterBase` into parameter name-value dictionary."""
        return dict(
            map(
                lambda x: (x.name, x.actual_value) if isinstance(x, CommandParameterBase) else (x[1], str(x[0])),
                parameters,
            )
        )
