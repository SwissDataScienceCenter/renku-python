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

from flask import jsonify, request
from flask_apispec import doc
from git import GitCommandError
from marshmallow import ValidationError
from redis import RedisError
from werkzeug.exceptions import HTTPException

from renku.core.errors import RenkuException
from renku.service.cache import cache
from renku.service.config import GIT_ACCESS_DENIED_ERROR_CODE, \
    GIT_UNKNOWN_ERROR_CODE, INTERNAL_FAILURE_ERROR_CODE, \
    INVALID_HEADERS_ERROR_CODE, INVALID_PARAMS_ERROR_CODE, \
    REDIS_EXCEPTION_ERROR_CODE, RENKU_EXCEPTION_ERROR_CODE
from renku.service.serializers.headers import UserIdentityHeaders
from renku.service.views import error_response


def requires_identity(f):
    """Wrapper which indicates that route requires user identification."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kws):
        """Represents decorated function."""
        try:
            user = UserIdentityHeaders().load(request.headers)
        except (ValidationError, KeyError):
            err_message = 'user identification is incorrect or missing'
            return jsonify(
                error={
                    'code': INVALID_HEADERS_ERROR_CODE,
                    'reason': err_message
                }
            )

        return f(user, *args, **kws)

    return decorated_function


def handle_redis_except(f):
    """Wrapper which handles Redis exceptions."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)
        except (RedisError, OSError) as e:
            error_code = REDIS_EXCEPTION_ERROR_CODE

            return jsonify(error={
                'code': error_code,
                'reason': e.messages,
            })

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


def handle_validation_except(f):
    """Wrapper which handles marshmallow `ValidationError`."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)
        except ValidationError as e:
            return jsonify(
                error={
                    'code': INVALID_PARAMS_ERROR_CODE,
                    'reason': e.messages,
                }
            )

    return decorated_function


def handle_renku_except(f):
    """Wrapper which handles `RenkuException`."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)
        except RenkuException as e:
            return jsonify(
                error={
                    'code': RENKU_EXCEPTION_ERROR_CODE,
                    'reason': str(e),
                }
            )

    return decorated_function


def handle_git_except(f):
    """Wrapper which handles `RenkuException`."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)
        except GitCommandError as e:

            error_code = GIT_ACCESS_DENIED_ERROR_CODE \
                if 'Access denied' in e.stderr else GIT_UNKNOWN_ERROR_CODE

            return jsonify(
                error={
                    'code': error_code,
                    'reason':
                        'git error: {0}'.
                        format(' '.join(e.stderr.strip().split('\n'))),
                }
            )

    return decorated_function


def accepts_json(f):
    """Wrapper which ensures only JSON payload can be in request."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        if 'Content-Type' not in request.headers:
            return jsonify(
                error={
                    'code': INVALID_HEADERS_ERROR_CODE,
                    'reason': 'invalid request headers'
                }
            )

        header_check = request.headers['Content-Type'] == 'application/json'

        if not request.is_json or not header_check:
            return jsonify(
                error={
                    'code': INVALID_HEADERS_ERROR_CODE,
                    'reason': 'invalid request payload'
                }
            )

        return f(*args, **kwargs)

    return decorated_function


def handle_base_except(f):
    """Wrapper which handles base exceptions."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)
        except HTTPException as e:  # handle general werkzeug exception
            return error_response(e.code, e.description)

        except (Exception, BaseException, OSError, IOError) as e:
            internal_error = 'internal error'
            if hasattr(e, 'stderr'):
                internal_error += ': {0}'.format(
                    ' '.join(e.stderr.strip().split('\n'))
                )
            return error_response(INTERNAL_FAILURE_ERROR_CODE, internal_error)

    return decorated_function


def header_doc(description, tags=()):
    """Wrap additional OpenAPI header description for an endpoint."""
    return doc(
        description=description,
        params={
            'Authorization': {
                'description': (
                    'Used for users git oauth2 access. '
                    'For example: '
                    '```Authorization: Bearer asdf-qwer-zxcv```'
                ),
                'in': 'header',
                'type': 'string',
                'required': True
            },
            'Renku-User-Id': {
                'description': (
                    'Used for identification of the users. '
                    'For example: '
                    '```Renku-User-Id: sasdsa-sadsd-gsdsdh-gfdgdsd```'
                ),
                'in': 'header',
                'type': 'string',
                'required': True
            },
            'Renku-User-FullName': {
                'description': (
                    'Used for commit author signature. '
                    'For example: '
                    '```Renku-User-FullName: Rok Roskar```'
                ),
                'in': 'header',
                'type': 'string',
                'required': True
            },
            'Renku-User-Email': {
                'description': (
                    'Used for commit author signature. '
                    'For example: '
                    '```Renku-User-Email: dev@renkulab.io```'
                ),
                'in': 'header',
                'type': 'string',
                'required': True
            },
        },
        tags=list(tags),
    )
