# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
"""Renku service exceptions."""
import os

from renku.core.errors import RenkuException
from renku.service.config import (
    DOCS_URL_BASE,
    DOCS_URL_ERRORS,
    ERROR_NOT_AVAILABLE,
    SVC_ERROR_PROGRAMMING,
    SVC_ERROR_USER,
)
from renku.service.serializers.headers import OptionalIdentityHeaders

HAS_SENTRY = True if os.getenv("SENTRY_DSN") else False
SENTRY_URL = os.getenv("SENTRY_URL", None)


class ServiceError(Exception):
    """Error handler for service errors.

    Set the documentation reference for developers willing to explore the base code.
    Handle Sentry, adding user data (when available) and error details.
    """

    devReference = f"{DOCS_URL_BASE}{DOCS_URL_ERRORS}" + "{class_name}"

    def __init__(
        self,
        code=None,
        userMessage=None,
        devMessage=None,
        userReference=None,
        devReference=None,
        exception=None,
    ):
        """Initialize the service error."""
        super().__init__()
        self.exception = exception
        self.devReference = f"{DOCS_URL_BASE}{DOCS_URL_ERRORS}{self.__class__.__name__}"
        if exception:
            self.__cause__ = exception

        if code:
            self.code = code
        if userMessage:
            self.userMessage = userMessage
        if devMessage:
            self.devMessage = devMessage
        if userReference:
            self.userReference = userReference
        if devReference:
            self.devReference = devReference

        if HAS_SENTRY:
            self._handle_sentry()

    def _get_user(self):
        """Get the user detail when the identity headers are set."""
        from flask import request

        user_identity = OptionalIdentityHeaders().load(request.headers)
        if user_identity.get("user_id", False):
            return {
                "logged": True,
                "id": user_identity["user_id"],
                "email": user_identity.get("email", "N/A"),
                "fullname": user_identity.get("fullname", "N/A"),
            }
        return {"logged": False}

    def _handle_sentry(self):
        """Send the expection to Sentry when available."""
        from sentry_sdk import capture_exception, set_context, set_tag, set_user

        if HAS_SENTRY:
            user = self._get_user()
            set_user(user)
            set_tag("error_code", self.code)
            details = {"code": self.code}
            if hasattr(self, "devMessage"):
                details["developer_message"] = self.devMessage
            if hasattr(self, "devReference"):
                details["developer_reference"] = self.devReference
            if hasattr(self, "userMessage"):
                details["user_message"] = self.userMessage
            if hasattr(self, "userReference"):
                details["user_reference"] = self.userReference
            set_context("details", details)

            try:
                sentry = capture_exception(self)
                if SENTRY_URL is not None:
                    sentry = f"{SENTRY_URL}?query={sentry}"
            except KeyError as e:
                sentry = f"Unexpected error while reporting to Sentry: {str(e)}"
        else:
            sentry = "Unavailable"

        self.sentry = sentry


class UserRepoUrlInvalidError(ServiceError):
    """The provided URL is not a valid Git repository.

    This usually happens when the URL is wrong.

    There are many possibilities:

    * The URL is wrong
    * The target is not a valid git repository
    * The user doesn't have the required permissions to access the repository
    * The target may temporarily be unavailable.
    """

    code = SVC_ERROR_USER + 10
    userMessage = (
        "A valid Git repository is not available at the target URL, "
        "or the user does not have the required permissions to access it."
    )
    devMessage = "Repository not found. Git error message: {error_message}"

    def __init__(self, exception=None, error_message=ERROR_NOT_AVAILABLE):
        super().__init__(devMessage=self.devMessage.format(error_message=error_message), exception=exception)


class UserRepoNoAccessError(ServiceError):
    """The target repository cannot be accessed by the current user.

    This is usually due to lack of permissions by the user, although in that case
    the repository manager usually returns a Not Found error instead.
    """

    code = SVC_ERROR_USER + 11
    userMessage = "Error accessing the repository due to lack of permissions."
    devMessage = "Access denied to the repository. Git error message: {error_message}"

    def __init__(self, exception=None, error_message=ERROR_NOT_AVAILABLE):
        super().__init__(devMessage=self.devMessage.format(error_message=error_message), exception=exception)


class UserAnonymousError(ServiceError):
    """The user must login in to use the target endpoint."""

    code = SVC_ERROR_USER + 30
    userMessage = "It's necessary to be authenticated to access the target data."
    devMessage = "User identification is incorrect or missing."

    def __init__(self, exception=None):
        super().__init__(exception=exception)


class ProgramRepoUnknownError(ServiceError):
    """Unknown error when working with the repository.

    This is a fallback error and it should ideally never show up.
    When it happens, it's best to investigate the underlying error and either
    create a more specific error or open an issue.
    """

    code = SVC_ERROR_PROGRAMMING + 10
    userMessage = "Fatal error occured while working on the repository."
    devMessage = "Unexpected repository error. Git error message: {error_message}"

    def __init__(self, exception=None, error_message=ERROR_NOT_AVAILABLE):
        super().__init__(devMessage=self.devMessage.format(error_message=error_message), exception=exception)


class ProgramGitError(ServiceError):
    """Unknown error when working with git.

    This is a fallback error and it should ideally never show up.
    When it happens, it's best to investigate the underlying error and either
    create a more specific error or open an issue.
    """

    code = SVC_ERROR_PROGRAMMING + 20
    userMessage = "Fatal error occured while processing a git operation on the repository."
    devMessage = "Unexpected git error. Git error message: {error_message}"

    def __init__(self, exception=None, error_message=ERROR_NOT_AVAILABLE):
        super().__init__(devMessage=self.devMessage.format(error_message=error_message), exception=exception)


class UserTemplateInvalidError(ServiceError):
    """The provided URL doesn't lead to a valid template repository.

    There are many possibilities:

    * The URL is wrong
    * The target is not a valid git repository
    * The target may temporarily be unavailable.
    """

    code = SVC_ERROR_USER + 101
    userMessage = "The target repository is not a valid RenkuLab template repository."
    devMessage = "Target repository is not a valid template."

    def __init__(self):
        super().__init__()


class ProgramInternalError(ServiceError):
    """Unknown internal error.

    This is an unexpected exception probably triggered at the core level.
    Please use sentry to get more information.
    """

    code = SVC_ERROR_PROGRAMMING + 1
    userMessage = "Our servers generated an unexpected error while processing data."
    devMessage = "Renku service internal error. Further information: {error_message}"

    def __init__(self, exception=None, error_message=ERROR_NOT_AVAILABLE):
        super().__init__(devMessage=self.devMessage.format(error_message=error_message), exception=exception)


class ProgramContentTypeError(ServiceError):
    """Content type not correctly specified."""

    code = SVC_ERROR_PROGRAMMING + 30
    userMessage = "Our servers could not process the received data."
    devMessage = (
        "Invalid requests headers. The content type should be set to '{expected_type}' instead of '{content_type}'"
    )

    def __init__(self, content_type=ERROR_NOT_AVAILABLE, expected_type=ERROR_NOT_AVAILABLE):
        super().__init__(devMessage=self.devMessage.format(content_type=content_type, expected_type=expected_type))


class ProgramHttpMethodError(ServiceError):
    """HTTP error 405 method not allowed.

    The method cannot be used on the target endpoint. The service only supports GET and POST.
    """

    code = SVC_ERROR_PROGRAMMING + 405
    userMessage = "One of the resources on our servers could not process the data properly."
    devMessage = (
        "Error 405 - method not allowed. Check if you are using GET or POST as specified in the"
        "API documentation. Mind that the service doesn't support any other method."
    )

    def __init__(self, exception=None):
        super().__init__(exception=exception)


class ProgramHttpMissingError(ServiceError):
    """HTTP error 404 not found.

    Either the URL is wrong or the user doesn't have permissions to access it.
    """

    code = SVC_ERROR_PROGRAMMING + 404
    userMessage = "One of the resources on our servers is not available at the moment."
    devMessage = (
        "Error 404 - not found. The resources are currently unavailable. "
        "This may happen when the user doesn't have permissions to access the target endpoint, "
        "or the endpoint URL is wrong or has been renamed."
    )

    def __init__(self, exception=None):
        super().__init__(exception=exception)


class ProgramHttpRequestError(ServiceError):
    """HTTP error 400 bad request.

    This is usually triggered by wrong parameters or payload. Double check the API documentation.
    """

    code = SVC_ERROR_PROGRAMMING + 400
    userMessage = "One of the resources on our servers could not process the data properly."
    devMessage = "Error 400 - bad request. Check if the payload and the parameters are correctly structured."

    def __init__(self, exception=None):
        super().__init__(exception=exception)


class ProgramHttpTimeoutError(ServiceError):
    """HTTP error 408 request timeout.

    This is usually triggered by wrong parameters or payload. Double check the API documentation.
    """

    code = SVC_ERROR_PROGRAMMING + 408
    userMessage = "The request took too long and was interruped."
    devMessage = "Error 408 - request timeout. There may be a client side delay."

    def __init__(self, exception=None):
        super().__init__(exception=exception)


class ProgramHttpServerError(ServiceError):
    """Any other HTTP error.

    Most of the times (always?) this will be a 50x error.
    The only way to get more details is to access the sentry exception.
    """

    code = SVC_ERROR_PROGRAMMING
    userMessage = "Fatal error occured while working on the repository."
    devMessage = "Unexpected repository error. Please check sentry to get more information."

    def __init__(self, exception=None, http_error_code=599):
        super().__init__(exception=exception, code=self.code + http_error_code)


# NOTE: old errors


class ProjectNotFound(RenkuException):
    """Project reference not found exception."""

    def __init__(self, project_id):
        """Build a custom message."""
        message = f'project_id "{project_id}" not found'
        super(ProjectNotFound, self).__init__(message)


class IdentificationError(RenkuException):
    """User identification not found or failed validation."""

    def __init__(self, message):
        """Build a custom message."""
        super(IdentificationError, self).__init__(message)


class OperationNotSupported(RenkuException):
    """Operation not supported exception."""

    def __init__(self, reason):
        message = f'operation not supported: "{reason}"'
        super(OperationNotSupported, self).__init__(message)


class AuthenticationTokenMissing(RenkuException):
    """Authentication token is missing."""

    def __init__(self):
        message = "authentication token is missing"
        super(AuthenticationTokenMissing, self).__init__(message)


class RenkuOpTimeoutError(RenkuException):
    """Renku operation timeout error."""

    def __init__(self):
        message = "renku operation timed out"
        super(RenkuOpTimeoutError, self).__init__(message)


class RenkuServiceLockError(RenkuException):
    """Renku operation lock error."""

    def __init__(self):
        message = "renku operation couldn't get a lock on the project"
        super(RenkuOpTimeoutError, self).__init__(message)
