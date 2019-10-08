# -*- coding: utf-8 -*-
#
# Copyright 2019 - Swiss Data Science Center (SDSC)
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

from .migration import check_dataset_metadata, check_missing_files
from .references import check_missing_references

# Checks will be executed in the order as they are listed in __all__.
# They are mostly used in ``doctor`` command to inspect broken things.
__all__ = (
    'check_dataset_metadata',
    'check_missing_files',
    'check_missing_references',
)
