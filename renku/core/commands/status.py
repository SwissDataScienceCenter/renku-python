# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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
"""Renku show command."""

import os
from collections import defaultdict
from typing import List, Tuple

from git import GitCommandError

from renku.core.management import LocalClient
from renku.core.management.command_builder import inject
from renku.core.management.command_builder.command import Command
from renku.core.management.interface.activity_gateway import IActivityGateway
from renku.core.models.provenance.activity import Activity, Usage
from renku.core.utils import communication
from renku.core.utils.contexts import measure


def _get_relative_path(client, path):
    """Get a relative path to current working directory."""
    return str((client.path / path).resolve().relative_to(os.getcwd()))


def get_status():
    """Show a status of the repository."""
    return Command().command(_get_status).require_migration().require_clean().with_database(write=False)


@inject.autoparams()
def _get_status(client: LocalClient, activity_gateway: IActivityGateway):
    with measure("BUILD AND QUERY GRAPH"):
        latest_activities = activity_gateway.get_latest_activity_per_plan().values()

    if client.has_external_files():
        communication.warn(
            "Changes in external files are not detected automatically. To update external files run "
            "`renku dataset update -e`."
        )

    try:
        communication.echo("On branch {0}".format(client.repo.active_branch))
    except TypeError:
        communication.warn("Git HEAD is detached!\n Please move back to your working branch to use renku\n")

    with measure("CALCULATE MODIFIED"):
        modified, deleted = _get_modified_paths(activities=latest_activities)

    if not modified and not deleted:
        return None, None, None

    stales = defaultdict(set)

    with measure("CALCULATE UPDATES"):
        for activity, usage in modified:
            usage_path = _get_relative_path(client, usage.entity.path)
            for generation in activity.generations:
                generation_path = _get_relative_path(client, generation.entity.path)
                stales[generation_path].add(usage_path)
            downstream_activities = activity_gateway.get_downstream_activities(activity)
            paths = [_get_relative_path(client, g.entity.path) for a in downstream_activities for g in a.generations]
            for p in paths:
                stales[p].add(usage_path)

    modified = {_get_relative_path(client, v[1].entity.path) for v in modified}

    deleted = {_get_relative_path(client, d) for d in deleted}

    return stales, modified, deleted


@inject.autoparams()
def _get_modified_paths(
    activities: List[Activity], client: LocalClient
) -> Tuple[List[Tuple[Activity, Usage]], List[Tuple[Activity, Usage]]]:
    """Get modified and deleted usages/inputs of a plan."""
    modified = set()
    deleted = set()
    for activity in activities:
        for usage in activity.usages:
            try:
                current_checksum = client.repo.git.rev_parse(f"HEAD:{str(usage.entity.path)}")
            except GitCommandError:
                deleted.add(usage.entity.path)
            else:
                if current_checksum != usage.entity.checksum:
                    modified.add((activity, usage))

    return modified, deleted
