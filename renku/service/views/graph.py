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
"""Renku graph endpoints."""
from flask import request

from renku.service.config import SERVICE_PREFIX
from renku.service.controllers.graph_export import GraphExportCtrl
from renku.service.views.api_versions import V1_0, V1_1, VersionedBlueprint
from renku.service.views.decorators import accepts_json, handle_common_except, optional_identity, requires_cache

GRAPH_BLUEPRINT_TAG = "graph"
graph_blueprint = VersionedBlueprint(GRAPH_BLUEPRINT_TAG, __name__, url_prefix=SERVICE_PREFIX)


@graph_blueprint.route("/graph.export", methods=["GET"], provide_automatic_options=False, versions=[V1_0, V1_1])
@handle_common_except
@requires_cache
@accepts_json
@optional_identity
def graph_build_view(user_data, cache):
    """
    Graph export view.

    ---
    post:
      description: Build a graph for a given repository and revision.
      requestBody:
        content:
          application/json:
            schema: GraphExportRequest
      responses:
        200:
          description: "Status of the graph building"
          content:
            application/json:
              schema: GraphExportResponseRPC
      tags:
        - graph
    """
    return GraphExportCtrl(cache, user_data, dict(request.json)).to_response()
