# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
from rdflib.namespace import Namespace
from rdflib.term import BNode

from renku.core.commands.echo import WARNING
from renku.core.compat import pyld
from renku.core.models.jsonld import NoDatesSafeLoader
from renku.core.utils.shacl import validate_graph


def _shacl_graph_to_string(graph):
    """Converts a shacl validation graph into human readable format."""
    sh = Namespace('http://www.w3.org/ns/shacl#')

    problems = []

    for _, result in graph.subject_objects(sh.result):
        path = graph.value(result, sh.resultPath)
        res = graph.value(result, sh.resultMessage)

        if res:
            message = '{0}: {1}'.format(path, res)
        else:
            kind = graph.value(result, sh.sourceConstraintComponent)
            focusNode = graph.value(result, sh.focusNode)

            if isinstance(focusNode, BNode):
                focusNode = '<Anonymous>'

            message = '{0}: Type: {1}, Node ID: {2}'.format(
                path, kind, focusNode
            )

        problems.append(message)

    return '\n\t'.join(problems)


def check_project_structure(client):
    """Validate project metadata against SHACL."""
    project_path = client.renku_metadata_path

    conform, graph, t = check_shacl_structure(project_path)

    if conform:
        return True, None

    problems = '{0}Invalid structure of project metadata\n\t{1}'.format(
        WARNING, _shacl_graph_to_string(graph)
    )

    return False, problems


def check_datasets_structure(client):
    """Validate dataset metadata against SHACL."""
    ok = True

    problems = ['{0}Invalid structure of dataset metadata'.format(WARNING)]

    for path in client.renku_datasets_path.rglob(client.METADATA):
        try:
            relative_path = path.relative_to(client.path)
            conform, graph, t = check_shacl_structure(path)
        except (Exception, BaseException) as e:
            problems.append(f'Couldn\'t validate {relative_path}: {e}\n\n')
            continue

        if conform:
            continue

        ok = False

        problems.append(
            '{0}\n\t{1}\n'.format(
                relative_path, _shacl_graph_to_string(graph)
            )
        )

    if ok:
        return True, None

    return False, '\n'.join(problems)


def check_shacl_structure(path):
    """Validates all metadata aginst the SHACL schema."""
    with path.open(mode='r') as fp:
        source = yaml.load(fp, Loader=NoDatesSafeLoader) or {}

    rdf = pyld.jsonld.to_rdf(
        source,
        options={
            'format': 'application/n-quads',
            'produceGeneralizedRdf': True
        }
    )

    return validate_graph(rdf)
