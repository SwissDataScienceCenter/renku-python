# -*- coding: utf-8 -*-
#
# Copyright 2022 - Swiss Data Science Center (SDSC)
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
"""Renku service error handlers."""

import re
from functools import wraps
from typing import cast

from jwt import ExpiredSignatureError, ImmatureSignatureError, InvalidIssuedAtError
from marshmallow import ValidationError
from redis import RedisError
from requests import RequestException

from renku.core.errors import (
    AuthenticationError,
    DatasetExistsError,
    DatasetImageError,
    DockerfileUpdateError,
    GitCommandError,
    GitError,
    InvalidTemplateError,
    MigrationError,
    MigrationRequired,
    MinimumVersionError,
    ParameterError,
    ProjectNotFound,
    RenkuException,
    TemplateMissingReferenceError,
    TemplateUpdateError,
    UninitializedProject,
    WorkflowNotFoundError,
)
from renku.ui.service.errors import (
    IntermittentAuthenticationError,
    IntermittentDatasetExistsError,
    IntermittentFileNotExistsError,
    IntermittentProjectIdError,
    IntermittentProjectTemplateUnavailable,
    IntermittentRedisError,
    IntermittentSettingExistsError,
    IntermittentTimeoutError,
    IntermittentWorkflowNotFound,
    ProgramGitError,
    ProgramGraphCorruptError,
    ProgramInternalError,
    ProgramInvalidGenericFieldsError,
    ProgramProjectCorruptError,
    ProgramProjectCreationError,
    ProgramRenkuError,
    ProgramRepoUnknownError,
    ProgramUpdateProjectError,
    ServiceError,
    UserDatasetsMultipleImagesError,
    UserDatasetsUnlinkError,
    UserDatasetsUnreachableImageError,
    UserInvalidGenericFieldsError,
    UserMissingFieldError,
    UserNewerRenkuProjectError,
    UserNonRenkuProjectError,
    UserOutdatedProjectError,
    UserProjectCreationError,
    UserProjectTemplateReferenceError,
    UserRepoBranchInvalidError,
    UserRepoNoAccessError,
    UserRepoUrlInvalidError,
    UserTemplateInvalidError,
)
from renku.ui.service.utils.squash import squash
from renku.ui.service.views import error_response


def handle_redis_except(f):
    """Wrapper which handles Redis exceptions."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)
        except (RedisError, OSError) as e:
            raise IntermittentRedisError(e)

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
        except (AuthenticationError, ExpiredSignatureError, ImmatureSignatureError, InvalidIssuedAtError) as e:
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
        except MinimumVersionError as e:
            raise UserNewerRenkuProjectError(e, minimum_version=e.minimum_version, current_version=e.current_version)
        except RenkuException as e:
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
        # NOTE: GitError here may not be necessary anymore
        except GitError as e:
            return error_response(ProgramGitError(e, cast(str, e.message) if hasattr(e, "message") else ""))
        except (Exception, BaseException, OSError, IOError) as e:
            if hasattr(e, "stderr") and e.stderr:
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


def handle_templates_read_errors(f):
    """Wrapper which handles reading templates errors."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)
        except InvalidTemplateError as e:
            raise UserTemplateInvalidError(e)
        except GitError as e:
            error_message = str(e)
            if "Cannot clone repo from" in error_message:
                raise UserRepoUrlInvalidError(e)
            elif "Cannot checkout reference" in error_message:
                raise UserRepoBranchInvalidError(e)
            elif "Cannot checkout manifest file" in error_message:
                raise UserTemplateInvalidError(e)
            raise

    return decorated_function


@handle_templates_read_errors
def handle_templates_create_errors(f):
    """Wrapper which handles template creating projects errors."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""

        def get_schema_error_message(e):
            if isinstance(getattr(e, "messages", None), dict) and e.messages.get("_schema"):
                message = (
                    "; ".join(e.messages.get("_schema"))
                    if isinstance(e.messages.get("_schema"), list)
                    else str(e.messages.get("_schema"))
                )
                return message
            return None

        try:
            return f(*args, **kwargs)
        except ValidationError as e:
            if getattr(e, "field_name", None) == "_schema":
                error_message = get_schema_error_message(e)
                if error_message:
                    raise UserProjectCreationError(e, error_message)
                else:
                    raise ProgramProjectCreationError(e, str(e))
            else:
                raise ProgramProjectCreationError(e)

    return decorated_function


def handle_project_write_errors(f):
    """Wrapper which handles writing project metadata errors."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)
        except UserInvalidGenericFieldsError as e:
            if "Unknown field" in str(e):
                raise ProgramInvalidGenericFieldsError(e)
            raise

    return decorated_function


def handle_config_read_errors(f):
    """Wrapper which handles reading config errors."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)
        except Exception as e:
            if str(e).startswith("Source contains parsing errors"):
                raise ProgramProjectCorruptError(e)
            raise

    return decorated_function


@handle_config_read_errors
def handle_config_write_errors(f):
    """Wrapper which handles setting config errors."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)
        except ParameterError as e:
            if "Invalid parameter value" in str(e):
                parameter = None
                match = re.search('Key "(.*)" not found', str(e))
                if match is not None and len(match.groups()) > 0:
                    parameter = match.group(1)
                raise IntermittentSettingExistsError(e, parameter)
            raise

    return decorated_function


def handle_datasets_write_errors(f):
    """Wrapper which handles datasets write errors."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)
        except DatasetImageError as e:
            error_message = str(e)
            if "Duplicate dataset image" in error_message:
                raise UserDatasetsMultipleImagesError(e)
            elif "couldn't be mirrored" in error_message:
                raise UserDatasetsUnreachableImageError(e)
            raise
        except ValidationError as e:
            items = squash(e.messages).items()
            for key, value in items:
                if "".join(value) == "Field may not be null.":
                    raise UserMissingFieldError(e, key)
            raise
        except DatasetExistsError as e:
            raise IntermittentDatasetExistsError(e)
        except RenkuException as e:
            if str(e).startswith("invalid file reference"):
                raise IntermittentFileNotExistsError(e)
            raise

    return decorated_function


def handle_workflow_errors(f):
    """Wrapper which handles workflow errors."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)
        except WorkflowNotFoundError as e:
            raise IntermittentWorkflowNotFound(e, name_or_id=e.name_or_id)
        except RenkuException:
            raise

    return decorated_function


def handle_datasets_unlink_errors(f):
    """Wrapper which handles datasets unlink errors."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)
        except RenkuException as e:
            if str(e).startswith("Invalid parameter") and "No records found" in str(e):
                raise UserDatasetsUnlinkError(e)
            raise
        except ValueError as e:
            if "one of the filters must be specified" in str(e):
                raise UserDatasetsUnlinkError(e)
            raise

    return decorated_function


def handle_migration_read_errors(f):
    """Wrapper which handles migrations read exceptions."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)
        except TemplateMissingReferenceError as e:
            raise UserProjectTemplateReferenceError(e)
        except (InvalidTemplateError, TemplateUpdateError) as e:
            raise IntermittentProjectTemplateUnavailable(e)
        except ProjectNotFound as e:
            raise UserRepoUrlInvalidError(e)

    return decorated_function


@handle_migration_read_errors
def handle_migration_write_errors(f):
    """Wrapper which handles migrations write exceptions."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)
        except (TemplateUpdateError, DockerfileUpdateError, MigrationError) as e:
            raise ProgramUpdateProjectError(e)

    return decorated_function


def handle_graph_errors(f):
    """Wrapper which handles graph exceptions."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)
        # TODO: handle user and program errors separately
        except (MemoryError, RenkuException, RequestException) as e:
            raise ProgramGraphCorruptError(e)

    return decorated_function
