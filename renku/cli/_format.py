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


def ascii(graph, **kwargs):
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


def dot(graph, simple=False):
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

    if simple:
        rdf2dot_simple(g, sys.stdout)
        return
    rdf2dot(g, sys.stdout)


def rdf2dot_simple(g, stream):
    """Create a simple graph of processes and artifacts."""
    stream.write(
        'digraph { \n node [ fontname="DejaVu Sans" ] ; \n '
        'rankdir="LR" \n'
    )

    import re

    path_re = re.compile(
        r'file:///(?P<type>[a-zA-Z]+)/'
        r'(?P<commit>\w+)'
        r'(?P<path>.+)?'
    )

    processes = g.query(
        """
        SELECT ?process
        WHERE {
            ?process rdf:type wfprov:ProcessRun
            FILTER NOT EXISTS {?process rdf:type wfprov:WorkflowRun}
        }
        """
    )
    for process, in processes:
        inputs = g.query(
            """
            SELECT ?role ?dependency
            WHERE {{
                <{process}> (prov:qualifiedUsage/prov:entity) ?dependency .
                <{process}> prov:qualifiedUsage ?qual .
                ?qual prov:hadRole ?role .
                ?qual prov:entity ?dependency .
            }}
            """.format(process=process)
        )
        outputs = g.query(
            """
        SELECT ?role ?dependent
        WHERE {{
            ?dependent (prov:qualifiedGeneration/prov:activity) <{process}>  .
            ?dependent prov:qualifiedGeneration ?qual .
            ?qual prov:hadRole ?role .
            ?qual prov:activity <{process}>
        }}
        """.format(process=process)
        )
        comment = list(
            g.query(
                """
            SELECT ?comment
            WHERE {{
                <{process}> rdfs:comment ?comment .
            }}
            """.format(process=process)
            )
        )[0]

        proc_path = path_re.match(process)

        # write the process node
        stream.write(
            '"{proc_commit}:{proc_path}"'
            '[shape=box label=<{comment}<br/>'
            '#{proc_commit}<br/>{proc_path}>] \n'.format(
                proc_commit=proc_path.group('commit')[:7],
                proc_path=proc_path.group('path') or '',
                comment=str(comment[0])
            )
        )

        # write the input nodes
        for role, dependency in inputs:
            dep_path = path_re.match(dependency)
            stream.write(
                '"{dep_commit}:{dep_path}"'
                '[label=<{dep_path}<br/>#{dep_commit}> \n]'
                '\t"{dep_commit}:{dep_path}" -> "{proc_commit}:{proc_path}" '
                '[label = "used as {role}"];\n'.format(
                    proc_commit=proc_path.group('commit')[:7],
                    proc_path=proc_path.group('path') or '',
                    dep_path=dep_path.group('path'),
                    dep_commit=dep_path.group('commit')[:7],
                    role=str(role)
                )
            )

        # write the output nodes
        for role, dependent in outputs:
            dep_path = path_re.match(dependent)
            stream.write(
                '"{dep_commit}:{dep_path}"'
                '[label=<{dep_path}<br/>#{dep_commit}> \n]'
                '\t"{proc_commit}:{proc_path}" -> "{dep_commit}:{dep_path}"'
                '[label="generated as {role}"];\n'.format(
                    proc_path=proc_path.group('path') or '',
                    proc_commit=proc_path.group('commit')[:7],
                    dep_path=dep_path.group('path'),
                    dep_commit=dep_path.group('commit')[:7],
                    role=str(role)
                )
            )
    stream.write('}\n')


def jsonld(graph, **kwargs):
    """Format graph as JSON-LD file."""
    click.echo(_jsonld(graph, 'expand'))


def jsonld_graph(graph, **kwargs):
    """Format graph as JSON-LD graph file."""
    click.echo(_jsonld(graph, 'flatten'))


def nt(graph, **kwargs):
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


def rdf(graph, **kwargs):
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
