# -*- coding: utf-8 -*-
#
# Copyright 2018-2019- Swiss Data Science Center (SDSC)
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
"""JSON-LD SHACL validation."""

import json

import yaml
from pkg_resources import resource_string
from pyshacl import validate


def validate_graph(graph, shacl_path=None, format='nquads'):
    """Validate the current graph with a SHACL schema.

    uses default schema if not supplied.
    """
    if not shacl_path:
        shacl_path = resource_string('renku', 'data/shacl_shape.yml')

    shacl = json.dumps(yaml.safe_load(shacl_path))

    return validate(
        graph,
        shacl_graph=shacl,
        inference='rdfs',
        meta_shacl=True,
        debug=False,
        data_graph_format=format,
        shacl_graph_format='json-ld',
        advanced=True
    )
