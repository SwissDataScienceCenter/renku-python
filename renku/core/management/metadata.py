# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 - Swiss Data Science Center (SDSC)
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
"""Metadata management functions."""

from renku.core.metadata.database import Database
from renku.core.models.dataset import Dataset
from renku.core.models.provenance.activity import Activity
from renku.core.models.workflow.plan import Plan


def initialize_database(database: Database):
    """Initialize Database."""
    database.clear()

    database.add_index(name="activities", object_type=Activity, attribute="id")
    database.add_index(name="plans", object_type=Plan, attribute="id")
    database.add_index(name="datasets", object_type=Dataset, attribute="name")
    database.add_index(name="datasets-provenance-tails", object_type=Dataset, attribute="id")

    database.commit()
