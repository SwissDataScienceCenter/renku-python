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
"""Define repository checks for :program:`renku doctor`."""

from .datasets import check_dataset_metadata, check_missing_files
from .external import check_missing_external_files
from .githooks import check_git_hooks_installed
from .migration import check_migration
from .references import check_missing_references
from .storage import check_lfs_info
from .validate_shacl import check_datasets_structure, check_project_structure

# Checks will be executed in the order as they are listed in __all__.
# They are mostly used in ``doctor`` command to inspect broken things.
__all__ = (
    "check_migration",
    "check_git_hooks_installed",
    "check_dataset_metadata",
    "check_missing_files",
    "check_missing_references",
    "check_project_structure",
    "check_datasets_structure",
    "check_missing_external_files",
    "check_lfs_info",
)
