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
import os
import re
from functools import wraps

from flask import jsonify, request
from flask_apispec import doc
from git import GitCommandError, GitError
from jwt import ExpiredSignatureError, ImmatureSignatureError, InvalidIssuedAtError
from marshmallow import ValidationError
from redis import RedisError
from sentry_sdk import capture_exception, set_context
from werkzeug.exceptions import HTTPException

from renku.core.errors import (
    DockerfileUpdateError,
    MigrationError,
    MigrationRequired,
    RenkuException,
    TemplateUpdateError,
    UninitializedProject,
)
from renku.service.cache import cache
from renku.service.config import (
    GIT_ACCESS_DENIED_ERROR_CODE,
    GIT_UNKNOWN_ERROR_CODE,
    INTERNAL_FAILURE_ERROR_CODE,
    INVALID_HEADERS_ERROR_CODE,
    INVALID_PARAMS_ERROR_CODE,
    REDIS_EXCEPTION_ERROR_CODE,
    RENKU_EXCEPTION_ERROR_CODE,
)
from renku.service.serializers.headers import UserIdentityHeaders
from renku.service.utils.squash import squash
from renku.service.views import error_response


def requires_identity(f):
    """Wrapper which indicates that route requires user identification."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kws):
        """Represents decorated function."""
        try:
            user_identity = UserIdentityHeaders().load(request.headers)
        except (ValidationError, KeyError) as e:
            capture_exception(e)

            err_message = "user identification is incorrect or missing"
            return jsonify(error={"code": INVALID_HEADERS_ERROR_CODE, "reason": err_message})

        return f(user_identity, *args, **kws)

    return decorated_function


def optional_identity(f):
    """Wrapper which indicates partial dependency on user identification."""

    @wraps(f)
    def decorated_function(*args, **kws):
        """Represents decorated function."""
        try:
            user_identity = UserIdentityHeaders().load(request.headers)
        except (ValidationError, KeyError):
            return f(None, *args, **kws)

        return f(user_identity, *args, **kws)

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
            try:
                set_context("pwd", os.readlink(f"/proc/{os.getpid()}/cwd"))
            except (Exception, BaseException):
                pass

            capture_exception(e)

            return jsonify(error={"code": REDIS_EXCEPTION_ERROR_CODE, "reason": e.messages})

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


def handle_schema_except(f):
    """Wrapper which handles Schema.load exceptions."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)
        except KeyError as e:
            capture_exception(e)

            if e.args and len(e.args) > 0:
                return jsonify(error={"code": INVALID_PARAMS_ERROR_CODE, "reason": f'missing parameter "{e.args[0]}"'})

            raise

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
            capture_exception(e)

            reasons = [f"`{key}` - {', '.join(value)}" for key, value in squash(e.messages).items()]
            error_message = f"Validation error: {'; '.join(reasons)}"

            return error_response(INVALID_PARAMS_ERROR_CODE, error_message)

    return decorated_function


def handle_jwt_except(f):
    """Wrapper which handles invalid JWT."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)
        except (ExpiredSignatureError, ImmatureSignatureError, InvalidIssuedAtError) as e:
            capture_exception(e)

            error_message = "invalid web token"
            return error_response(INVALID_HEADERS_ERROR_CODE, error_message)

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
            try:
                set_context("pwd", os.readlink(f"/proc/{os.getpid()}/cwd"))
            except (Exception, BaseException):
                pass
            capture_exception(e)

            err_response = {
                "code": RENKU_EXCEPTION_ERROR_CODE,
                "reason": str(e),
            }

            if isinstance(e, UninitializedProject):
                err_response["project_initialization_required"] = True

            if isinstance(e, MigrationRequired):
                err_response["migration_required"] = True

            return jsonify(error=err_response)

    return decorated_function


def handle_migration_except(f):
    """Wrapper which handles exceptions during migrations."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)
        except (TemplateUpdateError, DockerfileUpdateError, MigrationError) as e:
            err_response = {
                "code": RENKU_EXCEPTION_ERROR_CODE,
                "reason": str(e),
                "template_update_failed": isinstance(e, TemplateUpdateError),
                "dockerfile_update_failed": isinstance(e, DockerfileUpdateError),
                "migrations_failed": isinstance(e, MigrationError),
            }

            return jsonify(error=err_response)

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
            try:
                set_context("pwd", os.readlink(f"/proc/{os.getpid()}/cwd"))
            except (Exception, BaseException):
                pass
            capture_exception(e)

            error_code = GIT_ACCESS_DENIED_ERROR_CODE if "Access denied" in e.stderr else GIT_UNKNOWN_ERROR_CODE

            if "is this a git repository?" in e.stderr:
                error_reason = "Repository could not be found"
            elif "Access denied" in e.stderr:
                error_reason = "Repository could not be accessed - Do you have access rights?"
            else:
                error_reason = format(" ".join(e.stderr.strip().split("\n")))

                # strip oauth tokens
                error_reason_safe = re.sub("^(.+oauth2:)[^@]+(@.+)$", r"\1<token-hidden>\2", error_reason)
                error_reason = f"git error: {error_reason_safe}"

            return error_response(error_code, error_reason)

    return decorated_function


def accepts_json(f):
    """Wrapper which ensures only JSON payload can be in request."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        if "Content-Type" not in request.headers:
            return jsonify(error={"code": INVALID_HEADERS_ERROR_CODE, "reason": "invalid request headers"})

        header_check = request.headers["Content-Type"] == "application/json"

        if not request.is_json or not header_check:
            return jsonify(error={"code": INVALID_HEADERS_ERROR_CODE, "reason": "invalid request payload"})

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
            capture_exception(e)

            error_message = f"Failed to contact external service. Received response {e.code} ({e.description})."
            return error_response(INTERNAL_FAILURE_ERROR_CODE, error_message)

        except GitError as e:
            try:
                set_context("pwd", os.readlink(f"/proc/{os.getpid()}/cwd"))
            except (Exception, BaseException):
                pass
            capture_exception(e)

            error_message = "Failed to execute git operation."
            return error_response(INTERNAL_FAILURE_ERROR_CODE, error_message)

        except (Exception, BaseException, OSError, IOError) as e:
            try:
                set_context("pwd", os.readlink(f"/proc/{os.getpid()}/cwd"))
            except (Exception, BaseException):
                pass
            capture_exception(e)

            internal_error = "internal error"
            if hasattr(e, "stderr"):
                internal_error += ": {0}".format(" ".join(e.stderr.strip().split("\n")))

            return error_response(INTERNAL_FAILURE_ERROR_CODE, internal_error)

    return decorated_function


def header_doc(description, tags=()):
    """Wrap additional OpenAPI header description for an endpoint."""
    return doc(
        description=description,
        params={
            "Authorization": {
                "description": "Used for users git oauth2 access. For example: ```Bearer asdf-qwer-zxcv```",
                "in": "header",
                "type": "string",
            },
            "Renku-User": {
                "description": (
                    "JWT used for identification of the users. "
                    "For example: "
                    "```a9bd31fb.bfad4899b8bdf.d0908fab19d```"
                ),
                "in": "header",
                "type": "string",
            },
        },
        tags=list(tags),
    )


def handle_common_except(f):
    """Handle common exceptions."""
    # noqa
    @wraps(f)
    def dec(*args, **kwargs):
        """Decorated function."""

        @handle_base_except
        @handle_validation_except
        @handle_renku_except
        @handle_git_except
        @handle_jwt_except
        def _wrapped(*args_, **kwargs_):
            return f(*args_, **kwargs_)

        return _wrapped(*args, **kwargs)

    return dec
