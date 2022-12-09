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
from flask import jsonify
from marshmallow import Schema

from renku.ui.service.config import SVC_ERROR_GENERIC
from renku.ui.service.serializers.rpc import JsonRPCResponse


def result_response(serializer: Schema, data):
    """Construct flask response."""
    return jsonify(serializer.dump({"result": data}))


def error_response(serviceError):
    """Construct error response."""
    error = {}
    error["code"] = getattr(serviceError, "code", SVC_ERROR_GENERIC)
    error["userMessage"] = getattr(serviceError, "userMessage", "Unexpected exception")
    error["devMessage"] = getattr(serviceError, "devMessage", error["userMessage"])
    if hasattr(serviceError, "userReference"):
        error["userReference"] = serviceError.userReference
    if hasattr(serviceError, "devReference"):
        error["devReference"] = serviceError.devReference
    if hasattr(serviceError, "sentry"):
        error["sentry"] = serviceError.sentry

    return jsonify(JsonRPCResponse().dump({"error": error}))
