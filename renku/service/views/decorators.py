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

from flask import request
from jwt import ExpiredSignatureError, ImmatureSignatureError, InvalidIssuedAtError
from marshmallow import ValidationError
from redis import RedisError
from sentry_sdk import set_context

from renku.core.errors import GitCommandError, GitError, MigrationRequired, RenkuException, UninitializedProject
from renku.service.cache import cache
from renku.service.errors import (
    IntermittentAuthenticationError,
    IntermittentProjectIdError,
    IntermittentRedisError,
    IntermittentTimeoutError,
    ProgramContentTypeError,
    ProgramGitError,
    ProgramInternalError,
    ProgramInvalidGenericFieldsError,
    ProgramRenkuError,
    ProgramRepoUnknownError,
    ServiceError,
    UserAnonymousError,
    UserInvalidGenericFieldsError,
    UserNonRenkuProjectError,
    UserOutdatedProjectError,
    UserRepoNoAccessError,
    UserRepoUrlInvalidError,
)
from renku.service.serializers.headers import OptionalIdentityHeaders, RequiredIdentityHeaders
from renku.service.utils.squash import squash
from renku.service.views import error_response


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

            raise IntermittentRedisError(e)

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
            items = squash(e.messages).items()
            reasons = []
            for key, value in items:
                if key == "project_id":
                    raise IntermittentProjectIdError(e)
                reasons.append(f"'{key}': {', '.join(value)}")

            error_message = f"{'; '.join(reasons)}"
            if "Invalid `git_url`" in error_message:
                raise UserRepoUrlInvalidError(e, error_message)
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
            raise IntermittentAuthenticationError(e)

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
        except UninitializedProject as e:
            raise UserNonRenkuProjectError(e)
        except RenkuException as e:
            try:
                set_context("pwd", os.readlink(f"/proc/{os.getpid()}/cwd"))
            except (Exception, BaseException):
                pass

            raise ProgramRenkuError(e)

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

            error_message = e.stderr.lower() if e.stderr else ""
            error_message_safe = format(" ".join(error_message.strip().split("\n")))
            error_message_safe = re.sub("^(.+oauth2:)[^@]+(@.+)$", r"\1<token-hidden>\2", error_message_safe)
            if "access denied" in error_message:
                raise UserRepoNoAccessError(e, error_message_safe)
            elif "is this a git repository?" in error_message or "not found" in error_message:
                raise UserRepoUrlInvalidError(e, error_message_safe)
            elif "connection timed out" in error_message:
                raise IntermittentTimeoutError(e)
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
            return error_response(e)
        except GitError as e:
            try:
                set_context("pwd", os.readlink(f"/proc/{os.getpid()}/cwd"))
            except (Exception, BaseException):
                pass

            return error_response(ProgramGitError(e, e.message if e.message else None))

        except (Exception, BaseException, OSError, IOError) as e:
            try:
                set_context("pwd", os.readlink(f"/proc/{os.getpid()}/cwd"))
            except (Exception, BaseException):
                pass
            if hasattr(e, "stderr"):
                error_message = " ".join(e.stderr.strip().split("\n"))
            else:
                error_message = str(e)

            return error_response(ProgramInternalError(e, error_message))

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
