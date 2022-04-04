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
"""Renku project gateway interface."""

from abc import ABC
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from renku.domain_model.project import Project


class IProjectGateway(ABC):
    """Interface for the ProjectGateway."""

    def get_project(self) -> "Project":
        """Get project metadata."""
        raise NotImplementedError

    def update_project(self, project: "Project"):
        """Update project metadata."""
        raise NotImplementedError
