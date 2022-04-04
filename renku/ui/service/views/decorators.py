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
"""Renku service view decorators."""
from functools import wraps

from flask import request
from marshmallow import ValidationError

from renku.ui.service.cache import cache
from renku.ui.service.errors import ProgramContentTypeError, UserAnonymousError
from renku.ui.service.serializers.headers import OptionalIdentityHeaders, RequiredIdentityHeaders
from renku.ui.service.views.error_handlers import handle_redis_except


def requires_identity(f):
    """Wrapper which indicates that route requires user identification."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kws):
        """Represents decorated function."""
        try:
            user_identity = RequiredIdentityHeaders().load(request.headers)
        except (ValidationError, KeyError) as e:
            raise UserAnonymousError(e)

        return f(user_identity, *args, **kws)

    return decorated_function


def optional_identity(f):
    """Wrapper which indicates partial dependency on user identification."""

    @wraps(f)
    def decorated_function(*args, **kws):
        """Represents decorated function."""
        user_identity = OptionalIdentityHeaders().load(request.headers)
        return f(user_identity, *args, **kws)

    return decorated_function


@handle_redis_except
def requires_cache(f):
    """Wrapper which injects cache object into view."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        return f(cache, *args, **kwargs)

    return decorated_function


def accepts_json(f):
    """Wrapper which ensures only JSON payload can be in request."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        content_type = "application/json"
        wrong_type = False
        if "Content-Type" not in request.headers:
            wrong_type = True
        else:
            header_check = request.headers["Content-Type"] == content_type
            if not request.is_json or not header_check:
                wrong_type = True

        if wrong_type:
            raise ProgramContentTypeError(request.headers.get("Content-Type"), content_type)

        return f(*args, **kwargs)

    return decorated_function
