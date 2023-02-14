# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""Renku RDF Graph API.

The ``RDFGraph`` class allows for the quick creation of a searchable graph object
based on the project's metadata.

To create the graph and query it:

.. code-block:: python

    from renku.ui.api import RDFGraph

    g = RDFGraph()
    # get a list of contributors to the project
    list(g.subjects(object=URIRef("http://schema.org/Person")))

For more information on querying the graph, see the `RDFLib
documentation <https://rdflib.readthedocs.io/en/stable/intro_to_graphs.html>`_.

"""

import json

from rdflib import Graph

from renku.command.graph import export_graph_command


class RDFGraph(Graph):
    """RDF Graph of the project's metadata."""

    def __init__(self, revision_or_range=None):
        """Instantiate the RDFGraph class.

        Args:
            revision_or_range(None): Revision or range to generate the graph from. Defaults to ``None``

        """
        super().__init__()
        self.revision_or_range = revision_or_range
        self._build()
        self.bind_(self)

    def _build(self):
        """Construct the RDF graph representing this Renku project."""
        import pyld

        data = json.dumps(
            pyld.jsonld.expand(
                export_graph_command().build().execute(revision_or_range=self.revision_or_range).output._graph
            )
        )
        self.parse(data=data, format="json-ld")

    @staticmethod
    def bind_(graph):
        """Bind all the usual namespaces."""
        graph.bind("prov", "http://www.w3.org/ns/prov#")
        graph.bind("oa", "http://www.w3.org/ns/oa#")
        graph.bind("schema", "http://schema.org/")
        graph.bind("renku", "https://swissdatasciencecenter.github.io/renku-ontology#")
        graph.bind("foaf", "http://xmlns.com/foaf/0.1/")
