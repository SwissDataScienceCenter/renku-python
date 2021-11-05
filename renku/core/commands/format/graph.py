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
"""Serializers for graph data."""

import functools

import click

from renku.core.errors import OperationError, SHACLValidationError


def ascii(graph, strict=False):
    """Format graph as an ASCII art."""
    from ..ascii import DAG
    from ..echo import echo_via_pager

    if strict:
        raise SHACLValidationError("--strict not supported for ascii")

    echo_via_pager(str(DAG(graph)))


def _jsonld(graph, format, *args, **kwargs):
    """Return formatted graph in JSON-LD ``format`` function."""
    import json

    import pyld

    output = getattr(pyld.jsonld, format)(graph)
    return json.dumps(output, indent=2)


def _conjunctive_graph(graph):
    """Convert a renku ``Graph`` to an rdflib ``ConjunctiveGraph``."""
    from rdflib import ConjunctiveGraph

    return ConjunctiveGraph().parse(data=_jsonld(graph, "expand"), format="json-ld")


def dot(graph, simple=True, debug=False, landscape=False, strict=False):
    """Format graph as a dot file."""
    import sys

    from rdflib.tools.rdf2dot import rdf2dot

    if strict:
        raise SHACLValidationError("--strict not supported for dot")

    g = _conjunctive_graph(graph)

    g.bind("prov", "http://www.w3.org/ns/prov#")
    g.bind("foaf", "http://xmlns.com/foaf/0.1/")
    g.bind("schema", "http://schema.org/")
    g.bind("renku", "https://swissdatasciencecenter.github.io/renku-ontology/")

    if debug:
        rdf2dot(g, sys.stdout)
        return

    sys.stdout.write('digraph { \n node [ fontname="DejaVu Sans" ] ; \n ')
    if landscape:
        sys.stdout.write('rankdir="LR" \n')
    if simple:
        _rdf2dot_simple(g, sys.stdout, graph=graph)
        return
    _rdf2dot_reduced(g, sys.stdout)


# define the various dot options
dot_full = functools.partial(dot, simple=False, landscape=False)
dot_landscape = functools.partial(dot, simple=True, landscape=True)
dot_full_landscape = functools.partial(dot, simple=False, landscape=True)
dot_debug = functools.partial(dot, debug=True)


def _rdf2dot_simple(g, stream, graph=None):
    """Create a simple graph of processes and artifacts."""
    import re
    from itertools import chain

    path_re = re.compile(
        r"(?P<prefix>https://[\w\.]+/\w+/|https://[\w\.]+/){0,1}(?P<type>[a-zA-Z]+)/(?P<commit>[a-f0-9]+)(?P<path>.+)?"
    )

    inputs = g.query(
        """
        SELECT ?input ?activity
        WHERE {
            ?activity (prov:qualifiedUsage/prov:entity) ?input .
            ?activity prov:qualifiedUsage ?qual .
            ?qual prov:entity ?input .
            ?qual rdf:type ?type .
            ?activity rdf:type prov:Activity .
        }
        """
    )
    outputs = g.query(
        """
        SELECT ?activity ?output
        WHERE {
            ?output (prov:qualifiedGeneration/prov:activity) ?activity .
            ?output prov:qualifiedGeneration ?qual .
            ?qual prov:activity ?activity .
            ?qual rdf:type ?type .
            ?activity rdf:type prov:Activity ;
        }
        """
    )

    activity_nodes = {}
    artifact_nodes = {}
    for source, target in chain(inputs, outputs):
        # extract the pieces of the process URI
        src_match = path_re.match(source)
        tgt_match = path_re.match(target)

        if not src_match:
            raise OperationError(f"Could not extract path from id {source}")

        if not tgt_match:
            raise OperationError(f"Could not extract path from id {target}")

        src_path = src_match.groupdict()
        tgt_path = tgt_match.groupdict()

        # write the edge
        stream.write(
            '\t"{src_commit}:{src_path}" -> '
            '"{tgt_commit}:{tgt_path}" \n'.format(
                src_commit=src_path["commit"][:5],
                src_path=src_path.get("path") or "",
                tgt_commit=tgt_path["commit"][:5],
                tgt_path=tgt_path.get("path") or "",
            )
        )
        if src_path.get("type") == "commit":
            activity_nodes.setdefault(source, {})
            artifact_nodes.setdefault(target, {})
        if tgt_path.get("type") == "commit":
            activity_nodes.setdefault(target, {})
            artifact_nodes.setdefault(source, {})

    # customize the nodes
    for node, content in activity_nodes.items():
        node_path = path_re.match(node).groupdict()
        comment = content["comment"]
        if graph:
            activity = None
            for a in graph.activities.values():
                if a._id == str(node):
                    activity = a
                elif hasattr(a, "_processes"):
                    activity = next((p for p in a._processes if p._id == str(node)), None)
                else:
                    continue
                if not activity:
                    continue

                break

            if activity:
                plan = activity.association.plan

                comment = "{}: {} {}".format(
                    comment.split(":", 1)[0], " ".join(plan.to_argv()), " ".join(plan.to_stream_repr())
                )
        path = node_path.get("path")
        if path:
            path = ":{}".format(path)
        else:
            path = ""
        stream.write(
            '\t"{commit}:{path}" '
            '[shape=box label="#{commit}{path}:{comment}"] \n'.format(
                comment=comment, commit=node_path["commit"][:5], path=path
            )
        )
    for node, content in artifact_nodes.items():
        node_path = path_re.match(node).groupdict()
        stream.write(
            '\t"{commit}:{path}" '
            '[label="#{commit}:{path}"] \n'.format(commit=node_path["commit"][:5], path=node_path.get("path") or "")
        )
    stream.write("}\n")


def _rdf2dot_reduced(g, stream):
    """
    A reduced dot graph.

    Adapted from original source:
    https://rdflib.readthedocs.io/en/stable/_modules/rdflib/tools/rdf2dot.html
    """
    import collections
    import html

    import rdflib
    from rdflib.tools.rdf2dot import LABEL_PROPERTIES, NODECOLOR

    types = collections.defaultdict(set)
    fields = collections.defaultdict(set)
    nodes = {}

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

    for s, p, o in g.query(sparql):
        sn = node(s)
        if p == rdflib.RDFS.label:
            continue
        # inject the type predicate into the node itself
        if p == rdflib.RDF.type:
            types[sn].add((qname(p, g), html.escape(o)))
            continue
        # add the project membership to the node
        if p == rdflib.term.URIRef("schema:isPartOf"):
            fields[sn].add((qname(p, g), html.escape(o)))
            continue

        if isinstance(o, (rdflib.URIRef, rdflib.BNode)):
            on = node(o)
            opstr = '\t%s -> %s [ color=%s, label=< <font point-size="12" ' 'color="#336633">%s</font> > ] ;\n'
            stream.write(opstr % (sn, on, color(p), qname(p, g)))
        else:
            fields[sn].add((qname(p, g), formatliteral(o, g)))

    for u, n in nodes.items():
        stream.write("# %s %s\n" % (u, n))
        f = ['<tr><td align="left"><b>%s</b></td><td align="left">' "<b>%s</b></td></tr>" % x for x in sorted(types[n])]
        f += ['<tr><td align="left">%s</td><td align="left">%s</td></tr>' % x for x in sorted(fields[n])]
        opstr = (
            '%s [ shape=none, color=%s label=< <table color="#666666"'
            ' cellborder="0" cellspacing="0" border="1"><tr>'
            '<td colspan="2" bgcolor="grey"><B>%s</B></td></tr><tr>'
            '<td href="%s" bgcolor="#eeeeee" colspan="2">'
            '<font point-size="12" color="#6666ff">%s</font></td>'
            "</tr>%s</table> > ] \n"
        )
        stream.write(opstr % (n, NODECOLOR, label(u, g), u, u, "".join(f)))

    stream.write("}\n")


def makefile(graph, strict=False):
    """Format graph as Makefile."""
    from renku.core.models.provenance.activity import Activity

    if strict:
        raise SHACLValidationError("--strict not supported for makefile")

    for activity in graph:
        if not isinstance(activity, Activity):
            continue

        plan = activity.association.plan
        inputs = [i.default_value for i in plan.inputs]
        outputs = [o.default_value for o in plan.outputs]
        click.echo(" ".join(outputs) + ": " + " ".join(inputs))
        click.echo("\t@" + " ".join(plan.to_argv()) + " " + " ".join(plan.to_stream_repr()))


def jsonld(graph, strict=False, to_stdout=True):
    """Format graph as JSON-LD file."""
    from renku.core.utils.shacl import validate_graph

    ld = _jsonld(graph, "flatten")

    if strict:
        r, _, t = validate_graph(ld, format="json-ld")

        if not r:
            raise SHACLValidationError("{}\nCouldn't get log: Invalid Knowledge Graph data".format(t))

    if to_stdout:
        click.echo(ld)
        return

    return ld


def nt(graph, strict=False):
    """Format graph as n-tuples."""
    from renku.core.utils.shacl import validate_graph

    nt = _conjunctive_graph(graph).serialize(format="nt")
    if strict:
        r, _, t = validate_graph(nt, format="nt")

        if not r:
            raise SHACLValidationError("{}\nCouldn't get log: Invalid Knowledge Graph data".format(t))

    click.echo(nt)


def rdf(graph, strict=False):
    """Output the graph as RDF."""
    from renku.core.utils.shacl import validate_graph

    xml = _conjunctive_graph(graph).serialize(format="application/rdf+xml")
    if strict:
        r, _, t = validate_graph(xml, format="xml")

        if not r:
            raise SHACLValidationError("{}\nCouldn't get log: Invalid Knowledge Graph data".format(t))

    click.echo(xml)


FORMATS = {
    "ascii": ascii,
    "makefile": makefile,
    "dot": dot,
    "dot-landscape": dot_landscape,
}
"""Valid formatting options."""

GRAPH_FORMATS = {
    "jsonld": jsonld,
    "json-ld": jsonld,
    "nt": nt,
    "rdf": rdf,
    "dot": dot_full,
    "dot-landscape": dot_full_landscape,
    "dot-debug": dot_debug,
}
"""Valid graph formatting options."""
