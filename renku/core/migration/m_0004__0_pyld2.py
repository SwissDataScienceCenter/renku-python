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
"""Migrate datasets with type scoped contexts for support with pyld 2.0.

Since pyld now doesn't allow ':' in type scoped contexts and this is not
backwards compatible, we need to add/duplicate this migrations as a sort of
back-dated migration.
"""

from .m_0005__1_pyld2 import migrate_datasets_for_pyld2


def migrate(_):
    """Migration function."""
    migrate_datasets_for_pyld2()
