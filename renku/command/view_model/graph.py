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
"""Graph view model."""

import collections
import html
import io
import json
from enum import Enum
from typing import Dict, List, Optional

import pyld
import rdflib
from rdflib import ConjunctiveGraph, Graph
from rdflib.tools.rdf2dot import LABEL_PROPERTIES, NODECOLOR, rdf2dot


class DotFormat(Enum):
    """Enum of valid Dot output formats."""

    FULL = 1
    FULL_LANDSCAPE = 2
    DEBUG = 3


class GraphViewModel:
    """ViewModel for a JSON-LD knowledge graph.

    Args:
        graph(List[Dict]): The flattened JSON-LD data as a list of Python dictionaries.
    """

    def __init__(self, graph: List[dict]) -> None:
        self._graph = graph

    def as_jsonld(self) -> List[dict]:
        """Get the JSON-LD representation as a list of dictionaries.

        Returns:
            List[Dict]: The Python list of dictionary representation of the graph.
        """
        return self._graph

    def as_jsonld_string(self, indentation: Optional[int] = 2) -> str:
        """Get the JSON-LD representation as a string.

        Args:
            indentation(int, optional): The indentation to use for pretty-printing,
                set to `None` for no pretty printing (Default value = 2).
        Returns:
            str: The flattened JSON-LD string representation of the graph.
        """

        output = pyld.jsonld.flatten(self._graph)
        return json.dumps(output, indent=indentation)

    def as_rdflib_graph(self) -> Graph:
        """Get the graph as an RDFLib graph.

        Returns:
            Graph: The RDFLib graph.
        """
        data = json.dumps(pyld.jsonld.expand(self._graph))
        graph = ConjunctiveGraph().parse(data=data, format="json-ld")
        return graph

    def as_nt_string(self) -> str:
        """Get the graph as a string in nt format.

        Returns:
            str: The nt string representation of the graph.
        """
        return self.as_rdflib_graph().serialize(format="nt")

    def as_rdf_string(self) -> str:
        """Get the graph as a string in rdf+xml format.

        Returns:
            str: The rdf string representation of the graph.
        """
        return self.as_rdflib_graph().serialize(format="application/rdf+xml")

    def as_dot_string(self, format: DotFormat = DotFormat.FULL) -> str:
        """Get the graph as a Graphviz Dot string.

        Args:
            format(DotFormat): The format to use (Default value = `DotFormat.SIMPLE`).
        Returns:
            str: The Dot string representation of the graph.
        """
        graph = self.as_rdflib_graph()

        graph.bind("prov", "http://www.w3.org/ns/prov#")
        graph.bind("foaf", "http://xmlns.com/foaf/0.1/")
        graph.bind("schema", "http://schema.org/")
        graph.bind("renku", "https://swissdatasciencecenter.github.io/renku-ontology/")

        output = io.StringIO()

        try:
            if format == DotFormat.DEBUG:
                rdf2dot(graph, output)
                return output.getvalue()

            output.write('digraph { \n node [ fontname="DejaVu Sans" ] ; \n ')

            if format == DotFormat.FULL_LANDSCAPE:
                output.write('rankdir="LR" \n')

            self._rdfdot_full(graph, output)

            return output.getvalue()
        finally:
            output.close()

    def _rdfdot_full(self, graph: Graph, output: io.TextIOBase) -> None:
        """Write a full Dot graph representation to output.

        Args:
            graph(Graph): RDFLib graph.
            output(io.TextIOBase): The output stream to write to.
        """
        types = collections.defaultdict(set)
        fields = collections.defaultdict(set)
        nodes: Dict[str, str] = {}

        def node(x):
            """Return a name of the given node."""
            return nodes.setdefault(x, "node{0}".format(len(nodes)))

        def label(x, g):
            """Generate a label for the node."""
            for labelProp in LABEL_PROPERTIES:
                label_ = g.value(x, labelProp)
                if label_:
                    return label_

            try:
                return g.namespace_manager.compute_qname(x)[2]
            except Exception:
                return x

        def formatliteral(literal, g):
            """Format and escape literal."""
            v = html.escape(literal)
            if literal.datatype:
                return "&quot;%s&quot;^^%s" % (v, qname(literal.datatype, g))
            elif literal.language:
                return "&quot;%s&quot;@%s" % (v, literal.language)
            return "&quot;%s&quot;" % v

        def qname(x, g):
            """Compute qname."""
            try:
                q = g.compute_qname(x)
                return q[0] + ":" + q[2]
            except Exception:
                return x

        def color(p):
            """Choose node color."""
            return "BLACK"

        # filter out nodes and edges created for directories
        sparql = """
        SELECT ?s ?p ?o
        WHERE {
            ?s ?p ?o
            MINUS {
                ?s rdf:type prov:Collection.
            }
            MINUS {
                VALUES ?exclude { prov:wasInformedBy prov:influenced rdf:label }
                ?s ?exclude ?o.
            }
        }
        """

        for s, p, o in graph.query(sparql):
            sn = node(s)
            if p == rdflib.RDFS.label:
                continue
            # inject the type predicate into the node itself
            if p == rdflib.RDF.type:
                types[sn].add((qname(p, graph), html.escape(o)))
                continue
            # add the project membership to the node
            if p == rdflib.term.URIRef("schema:isPartOf"):
                fields[sn].add((qname(p, graph), html.escape(o)))
                continue

            if isinstance(o, (rdflib.URIRef, rdflib.BNode)):
                on = node(o)
                opstr = '\t%s -> %s [ color=%s, label=< <font point-size="12" ' 'color="#336633">%s</font> > ] ;\n'
                output.write(opstr % (sn, on, color(p), qname(p, graph)))
            else:
                fields[sn].add((qname(p, graph), formatliteral(o, graph)))

        for u, n in nodes.items():
            output.write("# %s %s\n" % (u, n))
            f = [
                '<tr><td align="left"><b>%s</b></td><td align="left">' "<b>%s</b></td></tr>" % x
                for x in sorted(types[n])
            ]
            f += ['<tr><td align="left">%s</td><td align="left">%s</td></tr>' % x for x in sorted(fields[n])]
            opstr = (
                '%s [ shape=none, color=%s label=< <table color="#666666"'
                ' cellborder="0" cellspacing="0" border="1"><tr>'
                '<td colspan="2" bgcolor="grey"><B>%s</B></td></tr><tr>'
                '<td href="%s" bgcolor="#eeeeee" colspan="2">'
                '<font point-size="12" color="#6666ff">%s</font></td>'
                "</tr>%s</table> > ] \n"
            )
            output.write(opstr % (n, NODECOLOR, label(u, graph), u, u, "".join(f)))

        output.write("}\n")
