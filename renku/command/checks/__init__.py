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

from .activities import check_migrated_activity_ids
from .datasets import (
    check_dataset_files_outside_datadir,
    check_dataset_old_metadata_location,
    check_invalid_datasets_derivation,
    check_missing_files,
)
from .external import check_missing_external_files
from .githooks import check_git_hooks_installed
from .migration import check_migration
from .project import check_project_id_group
from .storage import check_lfs_info
from .validate_shacl import check_datasets_structure, check_project_structure
from .workflow import check_activity_catalog

# Checks will be executed in the order as they are listed in __all__. They are mostly used in ``doctor`` command to
# inspect broken things. The order of operations matters when fixing issues, so, don't sort this list.
__all__ = (
    "check_activity_catalog",
    "check_dataset_files_outside_datadir",
    "check_dataset_old_metadata_location",
    "check_datasets_structure",
    "check_git_hooks_installed",
    "check_invalid_datasets_derivation",
    "check_lfs_info",
    "check_migrated_activity_ids",
    "check_migration",
    "check_missing_external_files",
    "check_missing_files",
    "check_project_id_group",
    "check_project_structure",
)
