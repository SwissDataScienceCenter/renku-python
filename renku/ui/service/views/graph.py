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

from renku.ui.service.config import SERVICE_PREFIX
from renku.ui.service.controllers.graph_export import GraphExportCtrl
from renku.ui.service.views.api_versions import VERSIONS_FROM_V1_0, VersionedBlueprint
from renku.ui.service.views.decorators import accepts_json, optional_identity, requires_cache
from renku.ui.service.views.error_handlers import handle_common_except, handle_graph_errors

GRAPH_BLUEPRINT_TAG = "graph"
graph_blueprint = VersionedBlueprint(GRAPH_BLUEPRINT_TAG, __name__, url_prefix=SERVICE_PREFIX)


@graph_blueprint.route("/graph.export", methods=["GET"], provide_automatic_options=False, versions=VERSIONS_FROM_V1_0)
@handle_common_except
@handle_graph_errors
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
    return GraphExportCtrl(cache, user_data, dict(request.json)).to_response()  # type: ignore
