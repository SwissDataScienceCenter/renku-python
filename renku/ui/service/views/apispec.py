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
"""Renku service apispec views."""
from apispec import APISpec, yaml_utils
from apispec.ext.marshmallow import MarshmallowPlugin
from apispec_webframeworks.flask import FlaskPlugin
from flask import Blueprint, current_app, jsonify
from flask.views import MethodView

from renku.ui.service.config import (
    API_VERSION,
    OIDC_URL,
    OPENAPI_VERSION,
    SERVICE_API_BASE_PATH,
    SERVICE_NAME,
    SERVICE_PREFIX,
)

apispec_blueprint = Blueprint("apispec", __name__, url_prefix=SERVICE_PREFIX)

# security schemes
oidc_scheme = {"type": "openIdConnect", "openIdConnectUrl": OIDC_URL}
jwt_scheme = {"type": "apiKey", "name": "Renku-User", "in": "header"}
gitlab_token_scheme = {"type": "apiKey", "name": "Authorization", "in": "header"}

TOP_LEVEL_DESCRIPTION = """
This is the API specification of the renku core service.

The basic API is low-level and requires that the client handles project
(repository) state in the service cache by invoking the `cache.project_clone`
method. This returns a `project_id` that is required for many of the other API
calls. Note that the `project_id` identifies a combination of `git_url` and
`ref` - i.e. each combination of `git_url` and `ref` receives a different
`project_id`.

## Higher-level interface

Some API methods allow the client to defer repository management to the service.
In these cases, the API documentation will include `project_id` _and_
`git_url`+`ref` in the spec. Note that for such methods, _either_ `project_id`
_or_ `git_url` (and optionally `ref`) should be passed in the request body.

## Responses

Loosely following the JSON-RPC 2.0 Specification, the methods all return with
HTTP code 200 and include a [response
object](https://www.jsonrpc.org/specification#response_object) may contain
either a `result` or an `error` object. If the call succeeds, the returned
`result` follows the schema documented in the individual methods. In the case of
an error, the [`error`
object](https://www.jsonrpc.org/specification#error_object), contains a code and
two messages describing the nature of the error. One targets the user, while the
other targets the developers. Instead of following the [standard JSON-RPC
response codes](https://www.jsonrpc.org/specification#error_object), we define
our own application-specific codes. Those can be found in the definition of the
error service classes.
"""


class MultiURLFlaskPlugin(FlaskPlugin):
    """FlaskPlugin extension that supports multiple URLs per endpoint."""

    def path_helper(self, path, operations, *, view, app=None, **kwargs):
        """Path helper that allows passing a Flask view function."""
        rule = self._rule_for_view(view, app=app)
        operations.update(yaml_utils.load_operations_from_docstring(view.__doc__))
        if hasattr(view, "view_class") and issubclass(view.view_class, MethodView):
            for method in view.methods:
                if method in rule.methods:
                    method_name = method.lower()
                    method = getattr(view.view_class, method_name)
                    operations[method_name] = yaml_utils.load_yaml_from_docstring(method.__doc__)
        return self.flaskpath2openapi(path)


spec = APISpec(
    title=SERVICE_NAME,
    openapi_version=OPENAPI_VERSION,
    version=API_VERSION,
    plugins=[MultiURLFlaskPlugin(), MarshmallowPlugin()],
    servers=[{"url": SERVICE_API_BASE_PATH}],
    security=[{"oidc": []}, {"JWT": [], "gitlab-token": []}],
    info={"description": TOP_LEVEL_DESCRIPTION},
)

spec.components.security_scheme("oidc", oidc_scheme)
spec.components.security_scheme("jwt", jwt_scheme)
spec.components.security_scheme("gitlab-token", gitlab_token_scheme)


@apispec_blueprint.route("/spec.json")
def openapi():
    """Return the OpenAPI spec for this service."""
    return jsonify(get_apispec(current_app).to_dict())


def get_apispec(app):
    """Return the apispec."""
    for rule in current_app.url_map.iter_rules():
        spec.path(path=rule.rule, view=app.view_functions[rule.endpoint])
    return spec
