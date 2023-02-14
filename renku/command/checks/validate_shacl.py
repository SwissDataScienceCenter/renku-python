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

import pyld
import yaml

from renku.command.command_builder import inject
from renku.command.schema.dataset import dump_dataset_as_jsonld
from renku.command.schema.project import ProjectSchema
from renku.command.util import WARNING
from renku.core.interface.dataset_gateway import IDatasetGateway
from renku.core.util.shacl import validate_graph
from renku.core.util.yaml import NoDatesSafeLoader
from renku.domain_model.project_context import project_context


def _shacl_graph_to_string(graph):
    """Converts a shacl validation graph into human readable format.

    Args:
        graph:  SHACL validation output graph.

    Returns:
        Text describing problems found in validation.
    """
    from rdflib.namespace import Namespace
    from rdflib.term import BNode

    sh = Namespace("http://www.w3.org/ns/shacl#")

    problems = []

    for _, result in graph.subject_objects(sh.result):
        path = graph.value(result, sh.resultPath)
        res = graph.value(result, sh.resultMessage)

        if res:
            message = "{0}: {1}".format(path, res)
        else:
            kind = graph.value(result, sh.sourceConstraintComponent)
            focus_node = graph.value(result, sh.focusNode)

            if isinstance(focus_node, BNode):
                focus_node = "<Anonymous>"

            message = "{0}: Type: {1}, Node ID: {2}".format(path, kind, focus_node)

        problems.append(message)

    return "\n\t".join(problems)


def check_project_structure(**_):
    """Validate project metadata against SHACL.

    Args:
        _: keyword arguments.

    Returns:
        Tuple of whether project structure is valid and string of found problems.
    """
    data = ProjectSchema().dump(project_context.project)

    conform, graph, t = _check_shacl_structure(data)

    if conform:
        return True, None

    problems = "{0}Invalid structure of project metadata\n\t{1}".format(WARNING, _shacl_graph_to_string(graph))

    return False, problems


@inject.autoparams("dataset_gateway")
def check_datasets_structure(dataset_gateway: IDatasetGateway, **_):
    """Validate dataset metadata against SHACL.

    Args:
        dataset_gateway(IDatasetGateway): The injected dataset gateway.
        _: keyword arguments.

    Returns:
        Tuple[bool, str]: Tuple of whether structure is valid and of problems that might have been found.
    """
    ok = True

    problems = [f"{WARNING}Invalid structure of dataset metadata"]

    for dataset in dataset_gateway.get_all_active_datasets():
        data = dump_dataset_as_jsonld(dataset)
        try:
            conform, graph, t = _check_shacl_structure(data)
        except (Exception, BaseException) as e:
            problems.append(f"Couldn't validate dataset '{dataset.name}': {e}\n\n")
            continue

        if conform:
            continue

        ok = False

        problems.append(f"{dataset.name}\n\t{_shacl_graph_to_string(graph)}\n")

    if ok:
        return True, None

    return False, "\n".join(problems)


def _check_shacl_structure_for_path(path):
    with path.open(mode="r") as fp:
        data = yaml.load(fp, Loader=NoDatesSafeLoader) or {}

    return _check_shacl_structure(data)


def _check_shacl_structure(data):
    """Validates all metadata against the SHACL schema.

    Args:
        data: JSON-LD data to validate.

    Returns:
        Validation result.
    """
    rdf = pyld.jsonld.to_rdf(data, options={"format": "application/n-quads", "produceGeneralizedRdf": True})

    return validate_graph(rdf)
