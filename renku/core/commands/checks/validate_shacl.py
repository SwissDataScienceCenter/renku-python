# -*- coding: utf-8 -*-
#
# Copyright 2019 - Swiss Data Science Center (SDSC)
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
"""Check KG structure using SHACL."""
import yaml
from pyld import jsonld as ld
from rdflib.namespace import Namespace
from rdflib.term import BNode

from renku.core.utils.shacl import validate_graph
from renku.core.models.jsonld import NoDatesSafeLoader
from renku.core.commands.echo import WARNING


def _shacl_graph_to_string(graph):
    """Converts a shacl validation graph into human readable format."""
    sh = Namespace('http://www.w3.org/ns/shacl#')

    problems = []

    for _, result in graph.subject_objects(sh.result):
        path = graph.value(result, sh.resultPath)
        res = graph.value(result, sh.resultMessage)

        if res:
            message = '{}: {}'.format(path, res)
        else:
            kind = graph.value(result, sh.sourceConstraintComponent)
            focusNode = graph.value(result, sh.focusNode)

            if isinstance(focusNode, BNode):
                focusNode = '<Anonymous>'

            message = '{}: Type: {}, Node ID: {}'.format(path, kind, focusNode)

        problems.append(message)

    return '\n\t'.join(problems)


def check_project_structure(client):
    """Validate project metadata against SHACL."""
    project_path = client.renku_metadata_path

    conform, graph, _ = check_shacl_structure(project_path)

    if conform:
        return True, None

    problems = (
        WARNING + 'Invalid structure of project metadata\n\t' +
        _shacl_graph_to_string(graph)
    )

    return False, problems


def check_datasets_structure(client):
    """Validate dataset metadata against SHACL."""
    ok = True

    problems = WARNING + 'Invalid structure of dataset metadata\n'

    for path in client.renku_datasets_path.rglob(client.METADATA):
        try:
            conform, graph, _ = check_shacl_structure(path)
        except (Exception, BaseException) as e:
            problems += 'Couldn\'t validate {}: {}\n\n'.format(path, e)
            continue

        if conform:
            continue

        ok = False

        problems += str(path) + '\n\t' + _shacl_graph_to_string(graph) + '\n\n'

    if ok:
        return True, None

    return False, problems


def check_shacl_structure(path):
    """Validates all metadata aginst the SHACL schema."""
    with path.open(mode='r') as fp:
        source = yaml.load(fp, Loader=NoDatesSafeLoader) or {}

    rdf = ld.to_rdf(
        source,
        options={
            'format': 'application/n-quads',
            'produceGeneralizedRdf': True
        }
    )

    return validate_graph(rdf)
