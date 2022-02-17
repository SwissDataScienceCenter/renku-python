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
from jwt import ExpiredSignatureError, ImmatureSignatureError, InvalidIssuedAtError
from marshmallow import ValidationError
from redis import RedisError
from sentry_sdk import capture_exception, set_context

from renku.core import errors
from renku.core.errors import (
    DockerfileUpdateError,
    MigrationError,
    MigrationRequired,
    RenkuException,
    TemplateUpdateError,
)
from renku.service.cache import cache
from renku.service.config import (
    INVALID_HEADERS_ERROR_CODE,
    INVALID_PARAMS_ERROR_CODE,
    REDIS_EXCEPTION_ERROR_CODE,
    RENKU_EXCEPTION_ERROR_CODE,
)
from renku.service.errors import (
    IntermittentProjectIdError,
    ProgramContentTypeError,
    ProgramGitError,
    ProgramInternalError,
    ProgramInvalidGenericFieldsError,
    ProgramRenkuError,
    ProgramRepoUnknownError,
    ServiceError,
    UserAnonymousError,
    UserInvalidGenericFieldsError,
    UserOutdatedProjectError,
    UserRepoNoAccessError,
    UserRepoUrlInvalidError,
)
from renku.service.serializers.headers import OptionalIdentityHeaders, RequiredIdentityHeaders
from renku.service.utils.squash import squash
from renku.service.views import error_response, error_response_new


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


def handle_redis_except(f):
    """Wrapper which handles Redis exceptions."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)
        except (RedisError, OSError) as e:
            # NOTE: Trap the process working directory when internal error occurs.
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
            items = squash(e.messages).items()
            reasons = []
            for key, value in items:
                if key == "project_id":
                    raise IntermittentProjectIdError(e)
                reasons.append(f"'{key}': {', '.join(value)}")

            error_message = f"{'; '.join(reasons)}"
            if "Unknown field" in error_message:
                raise ProgramInvalidGenericFieldsError(e, error_message)
            raise UserInvalidGenericFieldsError(e, error_message)

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
        except MigrationRequired as e:
            raise UserOutdatedProjectError(e)
        except RenkuException as e:
            try:
                set_context("pwd", os.readlink(f"/proc/{os.getpid()}/cwd"))
            except (Exception, BaseException):
                pass

            raise ProgramRenkuError(e)

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
        except errors.GitCommandError as e:
            try:
                set_context("pwd", os.readlink(f"/proc/{os.getpid()}/cwd"))
            except (Exception, BaseException):
                pass

            error_message = e.stderr.lower() if e.stderr else ""
            error_message_safe = format(" ".join(error_message.strip().split("\n")))
            error_message_safe = re.sub("^(.+oauth2:)[^@]+(@.+)$", r"\1<token-hidden>\2", error_message_safe)
            if "access denied" in error_message:
                raise UserRepoNoAccessError(e, error_message_safe)
            elif "is this a git repository?" in error_message or "not found" in error_message:
                raise UserRepoUrlInvalidError(e, error_message_safe)
            else:
                raise ProgramRepoUnknownError(e, error_message_safe)

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


def handle_base_except(f):
    """Wrapper which handles base exceptions."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)

        # NOTE: HTTPException are now handled in the entrypoint
        except ServiceError as e:
            return error_response_new(e)
        except errors.GitError as e:
            try:
                set_context("pwd", os.readlink(f"/proc/{os.getpid()}/cwd"))
            except (Exception, BaseException):
                pass

            return error_response_new(ProgramGitError(e, e.message if e.message else None))

        except (Exception, BaseException, OSError, IOError) as e:
            try:
                set_context("pwd", os.readlink(f"/proc/{os.getpid()}/cwd"))
            except (Exception, BaseException):
                pass
            if hasattr(e, "stderr"):
                error_message = " ".join(e.stderr.strip().split("\n"))
            else:
                error_message = str(e)

            return error_response_new(ProgramInternalError(e, error_message))

    return decorated_function


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
