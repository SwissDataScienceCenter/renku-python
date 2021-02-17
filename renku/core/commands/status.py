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

from renku.core.commands.graph import Graph
from renku.core.incubation.command import Command
from renku.core.utils import communication


def get_status():
    """Show a status of the repository."""
    return Command().command(_get_status).require_migration().require_clean()


def _get_status(client, revision, no_output, path):
    graph = Graph(client)
    # TODO filter only paths = {graph.normalize_path(p) for p in path}
    status = graph.build_status(revision=revision, can_be_cwl=no_output)

    if client.has_external_files():
        communication.echo(
            "Changes in external files are not detected automatically. To "
            'update external files run "renku dataset update -e".'
        )

    try:
        communication.echo("On branch {0}".format(client.repo.active_branch))
    except TypeError:
        communication.error("Git HEAD is detached!\n" " Please move back to your working branch to use renku\n")

    return graph, status
