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
"""PoC command for testing the new graph design."""

import click

from renku.cli.utils.callback import ClickCallback
from renku.cli.utils.click import CaseInsensitiveChoice
from renku.core.incubation.graph import FORMATS, export_graph, remove_workflow


@click.group(hidden=True)
def graph():
    """Proof-of-Concept command for testing the new graph design."""


# @graph.command()
# @click.argument("path", type=click.Path(exists=False, dir_okay=False))
# def save(path):
#     r"""Save dependency graph as PNG."""
#     with measure("CREATE DEPENDENCY GRAPH"):

#         @inject.autoparams()
#         def _to_png(path, database: Database):
#             DependencyGraph.from_database(database).to_png(path=path)

#         Command().command(_to_png).build().execute(path=path)


@graph.command()
@click.option("--format", type=CaseInsensitiveChoice(FORMATS), default="json-ld", help="Choose an output format.")
@click.option("--strict", is_flag=True, default=False, help="Validate triples before output.")
@click.option("--workflows-only", is_flag=True, help="Exclude datasets metadata from export.")
def export(format, strict, workflows_only):
    r"""Equivalent of `renku log --format json-ld`."""
    communicator = ClickCallback()
    export_graph().with_communicator(communicator).build().execute(
        format=format, workflows_only=workflows_only, strict=strict
    )


@graph.group()
def workflow():
    """Proof-of-Concept command for workflow operations using new metadata."""


@workflow.command()
@click.argument("name", metavar="<name or uuid>")
@click.option("-f", "--force", is_flag=True, help="Force remove (don't prompt user to confirm).")
def remove(name, force):
    """Remove the workflow named <name>."""
    communicator = ClickCallback()
    remove_workflow().with_communicator(communicator).build().execute(name=name, force=force)
