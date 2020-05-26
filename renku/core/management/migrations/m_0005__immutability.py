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
"""Migrations for dataset immutability."""

import attr


def migrate(client):
    """Migration function."""
    _migrate_blank_node_ids(client)
    _fix_tags(client)


def _migrate_blank_node_ids(client):
    """Fix IDs of blank nodes to contain only alphanumerics."""
    for dataset in client.datasets.values():
        if dataset.same_as is not None:
            dataset.same_as._id = dataset.same_as.default_id()

        tags = [attr.evolve(tag, id=tag.default_id()) for tag in dataset.tags]
        dataset.tags = tags

        dataset.to_yaml()


def _fix_tags(client):
    """Use dataset's short_name in tag instead of its name."""
    for dataset in client.datasets.values():
        tags = [
            attr.evolve(tag, dataset=dataset.short_name)
            for tag in dataset.tags
        ]
        dataset.tags = tags

        dataset.to_yaml()
