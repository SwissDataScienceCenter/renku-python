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
"""Models to represent a workflow file run templates."""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path
from typing import List, Optional, Union

from renku.domain_model.workflow.composite_plan import CompositePlan
from renku.domain_model.workflow.plan import Plan, validate_plan_name


class WorkflowFileCompositePlan(CompositePlan):
    """A workflow file composite plan."""

    plans: List[WorkflowFilePlan]

    def __init__(self, *, path: Union[Path, str], **kwargs):
        super().__init__(**kwargs)

        self.path: str = str(path)

    @staticmethod
    def generate_id(path: Union[Path, str] = None, sequence: Optional[int] = None, **_) -> str:
        """Generate an identifier for Plan."""
        assert path, "Path is needed to generate id for WorkflowFileCompositePlan"

        # NOTE: Workflow file's root composite plan's ID is generated only based on the file's path. The ID might be
        # changed later if the plan is a derivative
        key = f"{path}" if sequence is None else f"{path}::{sequence}"
        key_bytes = key.encode("utf-8")
        return CompositePlan.generate_id(uuid=hashlib.md5(key_bytes).hexdigest()[:32])

    def assign_new_id(self, *, sequence: Optional[int] = None, **_) -> str:
        """Assign a new UUID or a deterministic."""
        new_id = uuid.uuid4().hex if sequence is None else self.generate_id(path=self.path, sequence=sequence)
        return super().assign_new_id(uuid=new_id)

    def is_equal_to(self, other: WorkflowFileCompositePlan) -> bool:
        """Return true if plan hasn't changed from the other plan."""
        return self.path == other.path and super().is_equal_to(other=other)


class WorkflowFilePlan(Plan):
    """Represent a Plan that is converted from a workflow file."""

    def __init__(self, *, path: Union[Path, str], **kwargs):
        super().__init__(**kwargs)

        self.path: str = str(path)

    @staticmethod
    def generate_id(path: Union[Path, str] = None, name: str = None, sequence: Optional[int] = None, **_) -> str:
        """Generate an identifier for Plan."""
        assert path, "Path is needed to generate id for WorkflowFilePlan"
        assert name, "Name is needed to generate id for WorkflowFilePlan"

        key = f"{path}::{name}" if sequence is None else f"{path}::{name}::{sequence}"
        key_bytes = key.encode("utf-8")
        return Plan.generate_id(uuid=hashlib.md5(key_bytes).hexdigest()[:32])

    @staticmethod
    def validate_name(name: str):
        """Check a name for invalid characters."""
        validate_plan_name(name=name, extra_valid_characters="._-")

    @property
    def unqualified_name(self) -> str:
        """Name of the plan as appears in the workflow file definition."""
        return self.name.rsplit(".", maxsplit=1)[-1]

    def assign_new_id(self, *, sequence: Optional[int] = None, **_) -> str:
        """Assign a new UUID or a deterministic."""
        new_id = (
            uuid.uuid4().hex
            if sequence is None
            else self.generate_id(path=self.path, name=self.name, sequence=sequence)
        )
        return super().assign_new_id(uuid=new_id)

    def is_equal_to(self, other: WorkflowFilePlan) -> bool:
        """Return true if plan hasn't changed from the other plan."""
        return self.path == other.path and super().is_equal_to(other=other)

    def set_parameters_from_strings(self, params_strings: List[str]) -> None:
        """Set parameters by parsing parameters strings."""
        raise NotImplementedError
