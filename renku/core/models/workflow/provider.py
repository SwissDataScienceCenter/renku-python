# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
"""Workflow executor provider."""

from __future__ import annotations

from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import Optional, Tuple

from renku.core.models.workflow.plan import AbstractPlan


class IWorkflowProvider(metaclass=ABCMeta):
    """Abstract class for executing ``Plan``."""

    @abstractmethod
    def workflow_provider(self) -> Tuple[IWorkflowProvider, str]:
        """Supported workflow description formats."""
        pass

    @abstractmethod
    def workflow_execute(self, workflow: AbstractPlan, basedir: Path, config_file: Optional[str]):
        """Executes a given ``Plan`` using the provider."""
        pass
