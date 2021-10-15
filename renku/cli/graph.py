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
"""Renku CLI commands for handling of Knowledge Graph data.

Exporting Knowledge Graph data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can export part or all of the Renku Knowledge Graph metadata for the
current project using the ``renku graph export`` command.

By default, this will export the metadata created in the last commit in the
project.
If that commit was not a ``renku`` command that creates metadata, it will
produce no output.

.. code-block:: console

   $ renku dataset create my-dataset
   OK
   $ renku graph export
    [
        {
            "@id": "https://localhost/datasets/850e74d6c0204e8c923457a1b9ce52d8",
            "@type": [
            "http://schema.org/Dataset",
            "http://www.w3.org/ns/prov#Entity"
            ],

            [... many more lines ...]

        }
    ]

Here we created a new dataset and then ``renku graph export`` exported the
created metadata as JSON-LD, the default format.

If you want the Knowledge Graph data for the whole project, you can use
``renku graph export --full``. Alternatively, you can get data for a single
commit by using ``renku graph export --revision <git commit sha>`` or by
specifying a range of commits like ``renku graph export --revision sha1..sha2``.

``renku graph export`` currently supports various formats for export, such as
``json-ld``, ``rdf``, ``nt`` (for triples) and ``dot`` (for GraphViz graphs),
which can be specified using the ``--format`` option. For instance,

.. code-block:: console

   $ renku graph export --full --format dot | dot -Tpng -o my_graph.png

would produce a PNG image of the whole Knowledge Graph for the project.

To run validation on the generated output, you can pass the ``--strict``
option, which will check that all the nodes and properties in the graph are
correct and that there isn't anything missing.

"""

import click

from renku.cli.utils.click import CaseInsensitiveChoice
from renku.core.commands.format.graph import GRAPH_FORMATS


@click.group(hidden=True)
def graph():
    """Graph commands."""


@graph.command()
@click.option("--format", type=CaseInsensitiveChoice(GRAPH_FORMATS), default="json-ld", help="Choose an output format.")
@click.option(
    "--revision",
    type=str,
    default="HEAD",
    help="Limit graph to changes done in revision (or range of revisions like 'A..B').",
)
@click.option("--full", is_flag=True, help="Generate full graph for project. Overrides --revision.")
@click.option("--strict", is_flag=True, default=False, help="Validate triples before output.")
def export(format, revision, full, strict):
    r"""Export Renku graph metadata for project."""
    from renku.cli.utils.callback import ClickCallback
    from renku.core.commands.graph import export_graph_command

    if full:
        revision = None

    communicator = ClickCallback()
    export_graph_command().with_communicator(communicator).build().execute(
        format=format, strict=strict, revision_or_range=revision
    )
