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
"""Renku service views."""
from flask import current_app

from renku.service.serializers.rpc import JsonRPCResponse


def result_response(serializer, data):
    """Construct flask response."""
    return current_app.response_class(
        response=serializer.dumps({'result': data}),
        mimetype='application/json'
    )


def error_response(code, reason):
    """Construct error response."""
    return current_app.response_class(
        response=JsonRPCResponse().dumps({
            'error': {
                'code': code,
                'reason': reason
            }
        }),
        mimetype='application/json'
    )
