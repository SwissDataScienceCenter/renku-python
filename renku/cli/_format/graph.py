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

import functools

import click


def ascii(graph):
    """Format graph as an ASCII art."""
    from .._ascii import DAG
    from .._echo import echo_via_pager

    echo_via_pager(str(DAG(graph)))


def _jsonld(graph, format, *args, **kwargs):
    """Return formatted graph in JSON-LD ``format`` function."""
    import json

    from pyld import jsonld
    from renku.models._jsonld import asjsonld

    output = getattr(jsonld, format)([
        asjsonld(action) for action in graph.activities.values()
    ])
    return json.dumps(output, indent=2)


def dot(graph, simple=True, debug=False, landscape=False):
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

    g.bind('prov', 'http://www.w3.org/ns/prov#')
    g.bind('wfdesc', 'http://purl.org/wf4ever/wfdesc#')
    g.bind('wf', 'http://www.w3.org/2005/01/wf/flow#')
    g.bind('wfprov', 'http://purl.org/wf4ever/wfprov#')

    if debug:
        rdf2dot(g, sys.stdout)
        return

    sys.stdout.write('digraph { \n node [ fontname="DejaVu Sans" ] ; \n ')
    if landscape:
        sys.stdout.write('rankdir="LR" \n')
    if simple:
        _rdf2dot_simple(g, sys.stdout)
        return
    _rdf2dot_reduced(g, sys.stdout)


# define the various dot options
dot_full = functools.partial(dot, simple=False, landscape=False)
dot_landscape = functools.partial(dot, simple=True, landscape=True)
dot_full_landscape = functools.partial(dot, simple=False, landscape=True)
dot_debug = functools.partial(dot, debug=True)


def _rdf2dot_simple(g, stream):
    """Create a simple graph of processes and artifacts."""
    from itertools import chain

    import re

    path_re = re.compile(
        r'file:///(?P<type>[a-zA-Z]+)/'
        r'(?P<commit>\w+)'
        r'(?P<path>.+)?'
    )

    inputs = g.query(
        """
        SELECT ?input ?role ?activity ?comment
        WHERE {
            ?activity (prov:qualifiedUsage/prov:entity) ?input .
            ?activity prov:qualifiedUsage ?qual .
            ?qual prov:hadRole ?role .
            ?qual prov:entity ?input .
            ?qual rdf:type ?type .
            ?activity rdf:type wfprov:ProcessRun .
            ?activity rdfs:comment ?comment .
            FILTER NOT EXISTS {?activity rdf:type wfprov:WorkflowRun}
        }
        """
    )
    outputs = g.query(
        """
        SELECT ?activity ?role ?output ?comment
        WHERE {
            ?output (prov:qualifiedGeneration/prov:activity) ?activity .
            ?output prov:qualifiedGeneration ?qual .
            ?qual prov:hadRole ?role .
            ?qual prov:activity ?activity .
            ?qual rdf:type ?type .
            ?activity rdf:type wfprov:ProcessRun ;
                      rdfs:comment ?comment .
            FILTER NOT EXISTS {?activity rdf:type wfprov:WorkflowRun}
        }
        """
    )

    activity_nodes = {}
    artifact_nodes = {}
    for source, role, target, comment, in chain(inputs, outputs):
        # extract the pieces of the process URI
        src_path = path_re.match(source).groupdict()
        tgt_path = path_re.match(target).groupdict()

        # write the edge
        stream.write(
            '\t"{src_commit}:{src_path}" -> '
            '"{tgt_commit}:{tgt_path}" '
            '[label={role}] \n'.format(
                src_commit=src_path['commit'][:5],
                src_path=src_path.get('path') or '',
                tgt_commit=tgt_path['commit'][:5],
                tgt_path=tgt_path.get('path') or '',
                role=role
            )
        )
        if src_path.get('type') == 'commit':
            activity_nodes.setdefault(source, {'comment': comment})
            artifact_nodes.setdefault(target, {})
        if tgt_path.get('type') == 'commit':
            activity_nodes.setdefault(target, {'comment': comment})
            artifact_nodes.setdefault(source, {})

    # customize the nodes
    for node, content in activity_nodes.items():
        node_path = path_re.match(node).groupdict()
        stream.write(
            '\t"{commit}:{path}" '
            '[shape=box label="#{commit}:{path}:{comment}"] \n'.format(
                comment=content['comment'],
                commit=node_path['commit'][:5],
                path=node_path.get('path') or ''
            )
        )
    for node, content in artifact_nodes.items():
        node_path = path_re.match(node).groupdict()
        stream.write(
            '\t"{commit}:{path}" '
            '[label="#{commit}:{path}"] \n'.format(
                commit=node_path['commit'][:5],
                path=node_path.get('path') or ''
            )
        )
    stream.write('}\n')


def _rdf2dot_reduced(g, stream):
    """
    A reduced dot graph.

    Adapted from original source:
    https://rdflib.readthedocs.io/en/stable/_modules/rdflib/tools/rdf2dot.html
    """
    import cgi
    import collections

    import rdflib

    from rdflib.tools.rdf2dot import LABEL_PROPERTIES, NODECOLOR

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
            return q[0] + ':' + q[2]
        except Exception:
            return x

    def color(p):
        """Choose node color."""
        return 'BLACK'

    for s, p, o in g:
        sn = node(s)
        if p == rdflib.RDFS.label:
            continue

        # inject the type predicate into the node itself
        if p == rdflib.RDF.type:
            types[sn].add((qname(p, g), cgi.escape(o)))
            continue
        if p == rdflib.term.URIRef('http://purl.org/dc/terms/isPartOf'):
            fields[sn].add((qname(p, g), cgi.escape(o)))
            continue
        if p == rdflib.term.URIRef('http://www.w3.org/ns/prov#wasInformedBy'):
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


def makefile(graph):
    """Format graph as Makefile."""
    from renku.models.provenance.activities import ProcessRun, WorkflowRun

    for activity in graph.activities.values():
        if not isinstance(activity, ProcessRun):
            continue
        elif isinstance(activity, WorkflowRun):
            steps = activity.subprocesses.values()
        else:
            steps = [activity]

        for step in steps:
            click.echo(' '.join(step.outputs) + ': ' + ' '.join(step.inputs))
            tool = step.process
            click.echo(
                '\t@' + ' '.join(tool.to_argv()) + ' ' + ' '.join(
                    tool.STD_STREAMS_REPR[key] + ' ' + str(path)
                    for key, path in tool._std_streams().items()
                )
            )


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
    'dot-full': dot_full,
    'dot-landscape': dot_landscape,
    'dot-full-landscape': dot_full_landscape,
    'dot-debug': dot_debug,
    'json-ld': jsonld,
    'json-ld-graph': jsonld_graph,
    'Makefile': makefile,
    'nt': nt,
    'rdf': rdf,
}
"""Valid formatting options."""
