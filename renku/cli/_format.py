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
"""Serializers for graph data."""

import click


def ascii(graph):
    """Format graph as an ASCII art."""
    from ._ascii import DAG
    from ._echo import echo_via_pager

    echo_via_pager(str(DAG(graph)))


def _jsonld(graph, format, *args, **kwargs):
    """Return formatted graph in JSON-LD ``format`` function."""
    import json

    from pyld import jsonld
    from renku.models._jsonld import asjsonld

    output = getattr(jsonld, format)([
        asjsonld(action) for action in graph.commits.values()
    ])
    return json.dumps(output, indent=2)


def dot_old(graph):
    """Format graph as a dot file."""
    import sys

    from rdflib import ConjunctiveGraph
    from rdflib.plugin import register, Parser
    from rdflib.tools.rdf2dot import rdf2dot

    register('json-ld', Parser, 'rdflib_jsonld.parser', 'JsonLDParser')

    g = ConjunctiveGraph().parse(
        data=_jsonld(graph, 'expand'),
        format='json-ld',
    )
    rdf2dot(g, sys.stdout)


def dot(graph):
    """A reduced dot graph."""
    import cgi
    import collections
    import sys

    import rdflib

    from rdflib import ConjunctiveGraph
    from rdflib.tools.rdf2dot import LABEL_PROPERTIES, NODECOLOR

    g = ConjunctiveGraph().parse(
        data=_jsonld(graph, 'expand'),
        format='json-ld',
    )
    g.bind('prov', 'http://www.w3.org/ns/prov#')
    g.bind('wfdesc', 'http://purl.org/wf4ever/wfdesc#')
    g.bind('wf', 'http://www.w3.org/2005/01/wf/flow#')
    g.bind('wfprov', 'http://purl.org/wf4ever/wfprov#')

    def rdf2dot(g, stream, opts={}):
        """Convert the RDF graph to DOT.

        source: https://rdflib.readthedocs.io/en/stable/_modules/\
                rdflib/tools/rdf2dot.html
        """
        types = collections.defaultdict(set)
        fields = collections.defaultdict(set)
        nodes = {}

        def node(x):
            """Return a name of the given node."""
            return nodes.setdefault(x, 'node{0}'.format(len(nodes)))

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

        def formatliteral(l, g):
            """Format and escape literal."""
            v = cgi.escape(l)
            if l.datatype:
                return '&quot;%s&quot;^^%s' % (v, qname(l.datatype, g))
            elif l.language:
                return '&quot;%s&quot;@%s' % (v, l.language)
            return '&quot;%s&quot;' % v

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

        stream.write(
            'digraph { \n node [ fontname="DejaVu Sans" ] ; \n '
            'rankdir="LR" \n'
        )

        for s, p, o in g:
            # import ipdb; ipdb.set_trace()
            sn = node(s)
            if p == rdflib.RDFS.label:
                continue

            # inject the type predicate into the node itself
            if p == rdflib.RDF.type:
                types[sn].add((qname(p, g), cgi.escape(o)))
                continue
            if p == rdflib.term.URIRef('http://www.w3.org/ns/prov#atLocation'):
                fields[sn].add((qname(p, g), cgi.escape(o)))
                continue
            if p == rdflib.term.URIRef(
                'http://www.w3.org/ns/prov#wasInformedBy'
            ):
                continue

            if isinstance(o, (rdflib.URIRef, rdflib.BNode)):
                on = node(o)
                opstr = (
                    '\t%s -> %s [ color=%s, label=< <font point-size="12" '
                    'color="#336633">%s</font> > ] ;\n'
                )
                stream.write(opstr % (sn, on, color(p), qname(p, g)))
            else:
                fields[sn].add((qname(p, g), formatliteral(o, g)))

        for u, n in nodes.items():
            stream.write(u"# %s %s\n" % (u, n))
            f = [
                '<tr><td align="left"><b>%s</b></td><td align="left">'
                '<b>%s</b></td></tr>' % x for x in sorted(types[n])
            ]
            f += [
                '<tr><td align="left">%s</td><td align="left">%s</td></tr>' % x
                for x in sorted(fields[n])
            ]
            opstr = (
                '%s [ shape=none, color=%s label=< <table color="#666666"'
                ' cellborder="0" cellspacing="0" border="1"><tr>'
                '<td colspan="2" bgcolor="grey"><B>%s</B></td></tr><tr>'
                '<td href="%s" bgcolor="#eeeeee" colspan="2">'
                '<font point-size="12" color="#6666ff">%s</font></td>'
                '</tr>%s</table> > ] \n'
            )
            stream.write(opstr % (n, NODECOLOR, label(u, g), u, u, ''.join(f)))

        stream.write('}\n')

    rdf2dot(g, sys.stdout)


def jsonld(graph):
    """Format graph as JSON-LD file."""
    click.echo(_jsonld(graph, 'expand'))


def jsonld_graph(graph):
    """Format graph as JSON-LD graph file."""
    click.echo(_jsonld(graph, 'flatten'))


def nt(graph):
    """Format graph as n-tuples."""
    from rdflib import ConjunctiveGraph
    from rdflib.plugin import register, Parser

    register('json-ld', Parser, 'rdflib_jsonld.parser', 'JsonLDParser')

    click.echo(
        ConjunctiveGraph().parse(
            data=_jsonld(graph, 'expand'),
            format='json-ld',
        ).serialize(format='nt')
    )


def rdf(graph):
    """Output the graph as RDF."""
    from rdflib import ConjunctiveGraph
    from rdflib.plugin import register, Parser

    register('json-ld', Parser, 'rdflib_jsonld.parser', 'JsonLDParser')

    click.echo(
        ConjunctiveGraph().parse(
            data=_jsonld(graph, 'expand'),
            format='json-ld',
        ).serialize(format='application/rdf+xml')
    )


FORMATS = {
    'ascii': ascii,
    'dot': dot,
    'json-ld': jsonld,
    'json-ld-graph': jsonld_graph,
    'nt': nt,
    'rdf': rdf,
}
"""Valid formatting options."""
