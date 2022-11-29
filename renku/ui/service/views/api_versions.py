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
"""Renku service api versions."""
from typing import Any, Callable, List, Optional

from flask import Blueprint


class ApiVersion:
    """Represents a Blueprint API version."""

    def __init__(self, name: str, is_base_version: bool = False):
        self.name = name
        self.is_base_version = is_base_version


class VersionedBlueprint(Blueprint):
    """A Blueprint that supports versioning."""

    def add_url_rule(
        self,
        rule: str,
        endpoint: Optional[str] = None,
        view_func: Optional[Callable] = None,
        provide_automatic_options: Optional[bool] = None,
        versions: List[ApiVersion] = None,
        **options: Any,
    ) -> None:
        """Overwrite Blueprint add_url_rule to support versioning."""
        if versions is None:
            return

        for version in versions:
            if version.is_base_version:
                super().add_url_rule(
                    rule, endpoint, view_func, provide_automatic_options=provide_automatic_options, **options
                )

            version_rule = f"/{version.name}{rule}"
            super().add_url_rule(
                version_rule, endpoint, view_func, provide_automatic_options=provide_automatic_options, **options
            )


V0_9 = ApiVersion("0.9")
V1_0 = ApiVersion("1.0")
V1_1 = ApiVersion("1.1")
V1_2 = ApiVersion("1.2")
V1_3 = ApiVersion("1.3")
V1_4 = ApiVersion("1.4")
V1_5 = ApiVersion("1.5", is_base_version=True)

VERSIONS_FROM_V1_4 = [V1_4, V1_5]
VERSIONS_FROM_V1_1 = [V1_1, V1_2, V1_3, V1_4, V1_5]
VERSIONS_FROM_V1_0 = [V1_0] + VERSIONS_FROM_V1_1
ALL_VERSIONS = [V0_9] + VERSIONS_FROM_V1_0

MINIMUM_VERSION = V0_9
MAXIMUM_VERSION = V1_5
