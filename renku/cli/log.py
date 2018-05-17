# -*- coding: utf-8 -*-
#
# Copyright 2018 - Swiss Data Science Center (SDSC)
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
"""Show provenance of data created by executing programs."""

import click

from ._ascii import DAG
from ._client import pass_local_client
from ._echo import echo_via_pager
from ._graph import Graph


@click.command()
@click.option('--revision', default='HEAD')
@click.argument('path', type=click.Path(exists=True, dir_okay=False), nargs=-1)
@pass_local_client
def log(client, revision, path):
    """Show logs for a file."""
    graph = Graph(client)
    for p in path:
        graph.add_file(p, revision=revision)

    echo_via_pager(DAG(graph=graph))
