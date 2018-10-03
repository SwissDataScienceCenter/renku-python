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

    echo_via_pager(DAG(graph))


def _jsonld(graph, format, *args, **kwargs):
    """Return formatted graph in JSON-LD ``format`` function."""
    import json

    from pyld import jsonld
    from renku.models._jsonld import asjsonld

    output = getattr(jsonld, format)([
        asjsonld(action) for action in graph.commits.values()
    ])
    return json.dumps(output, indent=2)


def dot(graph):
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
