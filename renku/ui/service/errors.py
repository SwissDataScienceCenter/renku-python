# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
from urllib.parse import urlparse

from renku.ui.service.config import (
    DOCS_URL_BASE,
    DOCS_URL_ERRORS,
    ERROR_NOT_AVAILABLE,
    SENTRY_ENABLED,
    SVC_ERROR_INTERMITTENT,
    SVC_ERROR_PROGRAMMING,
    SVC_ERROR_USER,
)
from renku.ui.service.serializers.headers import OptionalIdentityHeaders


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

        if SENTRY_ENABLED:
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

        if SENTRY_ENABLED:
            # NOTE: Trap the process working directory when internal error occurs.
            try:
                path = os.readlink(f"/proc/{os.getpid()}/cwd")
                set_context("pwd", {"path": path})
            except (Exception, BaseException):
                pass

            # NOTE: try to set user details when available
            user = self._get_user()
            set_user(user)

            # NOTE: set erro code tag and details section
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

            # NOTE: add the sentry URL to the exception
            try:
                sentry = capture_exception(self)
                sentry_dsn = os.getenv("SENTRY_DSN", None)
                if sentry_dsn is not None:
                    try:
                        sentry_url = urlparse(sentry_dsn)
                        sentry_target = sentry_url.netloc.split("@")[-1]
                        # NOTE: sentry doesn't support a global search. A proper link would require the specific org
                        sentry = f"{sentry_url.scheme }://{sentry_target}/organizations/sentry?query={sentry}"
                    except Exception:
                        pass
            except KeyError as e:
                sentry = f"Unexpected error while reporting to Sentry: {str(e)}"
        else:
            sentry = "Unavailable"

        self.sentry = sentry


class UserInvalidGenericFieldsError(ServiceError):
    """One or more fields provided by the user has an invalid value.

    This is a generic error. Where possible, it would be best to create specific errors meaningful for the users.
    """

    code = SVC_ERROR_USER + 1
    userMessage = "Some of the provided values are wrong: {wrong_values}"
    devMessage = "Wrong values have been provided: {wrong_values}"

    def __init__(self, exception=None, wrong_values=ERROR_NOT_AVAILABLE):
        super().__init__(
            userMessage=self.userMessage.format(wrong_values=wrong_values),
            devMessage=self.devMessage.format(wrong_values=wrong_values),
            exception=exception,
        )


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


class UserRepoBranchInvalidError(ServiceError):
    """The provided URL is a valid Git repository, but the target branch does not exist.

    This usually happens when the target branch does not exist anymore.
    It is possible it was there at some point and it has been removed in the meanwhile.
    """

    code = SVC_ERROR_USER + 12
    userMessage = "The target URL is a valid Git repository, but we could not find the branch '{branch}'."
    devMessage = "Branch not valid. Git error message: {error_message}"

    def __init__(self, exception=None, branch=ERROR_NOT_AVAILABLE, error_message=ERROR_NOT_AVAILABLE):
        super().__init__(
            userMessage=self.userMessage.format(branch=branch),
            devMessage=self.devMessage.format(error_message=error_message),
            exception=exception,
        )


class UserRepoReferenceInvalidError(ServiceError):
    """The provided URL is a valid Git repository, but the target reference does not exist.

    This usually happens when the target branch, tag or commit does not exist anymore.
    It's possible it was there and the history has been re-written in the meanwhile, or it
    was never there at all.
    """

    code = SVC_ERROR_USER + 13
    userMessage = (
        "The target URL is a valid Git repository, but we could not find the reference"
        "'{reference}'. Please specify a valid tag, branch or commit"
    )
    devMessage = "Reference not valid. Git error message: {error_message}"

    def __init__(self, exception=None, branch=ERROR_NOT_AVAILABLE, error_message=ERROR_NOT_AVAILABLE):
        super().__init__(
            userMessage=self.userMessage.format(branch=branch),
            devMessage=self.devMessage.format(error_message=error_message),
            exception=exception,
        )


class UserAnonymousError(ServiceError):
    """The user must login in to use the target endpoint."""

    code = SVC_ERROR_USER + 30
    userMessage = "It's necessary to be authenticated to access the target data."
    devMessage = "User identification is incorrect or missing."

    def __init__(self, exception=None):
        super().__init__(exception=exception)


class UserMissingFieldError(ServiceError):
    """The user has not provided an expected field."""

    code = SVC_ERROR_USER + 40
    userMessage = "The following field is required: {field}."
    devMessage = "User did not provide the mandatory field '{field}'."

    def __init__(self, exception=None, field=ERROR_NOT_AVAILABLE):
        super().__init__(
            userMessage=self.userMessage.format(field=field),
            devMessage=self.devMessage.format(field=field),
            exception=exception,
        )


class UserTemplateInvalidError(ServiceError):
    """The provided URL doesn't lead to a valid template repository."""

    code = SVC_ERROR_USER + 101
    userMessage = "The target repository is not a valid Renku template repository."
    devMessage = "Target repository is not a valid template."

    def __init__(self, exception=None):
        super().__init__(exception=exception)


class UserProjectCreationError(ServiceError):
    """Error when creating a new project from a Renku template.

    One (or more) project field is wrong.
    """

    code = SVC_ERROR_USER + 102
    userMessage = "There is an error with a project field: {error_message}."
    devMessage = "Project creation from a Renku template failed. The user provided wrong field(s): {error_message}."

    def __init__(self, exception=None, error_message=ERROR_NOT_AVAILABLE):
        super().__init__(
            userMessage=self.userMessage.format(error_message=error_message),
            devMessage=self.devMessage.format(error_message=error_message),
            exception=exception,
        )


class UserNonRenkuProjectError(ServiceError):
    """The target repository is valid but it is not a Renku project."""

    code = SVC_ERROR_USER + 110
    userMessage = "The target project is not a Renku project."
    devMessage = "Cannot work on a non Renku project."

    def __init__(self, exception=None):
        super().__init__(exception=exception)


class UserDatasetsMultipleImagesError(ServiceError):
    """Multiple images dataset have the same priority."""

    code = SVC_ERROR_USER + 130
    userMessage = "Multiple dataset images must have different priorities."
    devMessage = "The input parameters contain multiple dataset images with the same priority."

    def __init__(self, exception=None):
        super().__init__(exception=exception)


class UserDatasetsUnreachableImageError(ServiceError):
    """Dataset image not reachable."""

    code = SVC_ERROR_USER + 131
    userMessage = "Cannot download the dataset image."
    devMessage = "The remote dataset image is not reachable."

    def __init__(self, exception=None):
        super().__init__(exception=exception)


class UserDatasetsUnlinkError(ServiceError):
    """Dataset unlink parameters not valid."""

    code = SVC_ERROR_USER + 132
    userMessage = "Please provide valid unlink parameter."
    devMessage = "Unlink parameters not valid."

    def __init__(self, exception=None):
        super().__init__(exception=exception)


class UserOutdatedProjectError(ServiceError):
    """The operation can be done only after updating the target project."""

    code = SVC_ERROR_USER + 140
    userMessage = "This operation requires to update the project first."
    devMessage = "The requested operation can only be performed on an up-to-date project."

    def __init__(self, exception=None):
        super().__init__(exception=exception)


class UserNewerRenkuProjectError(ServiceError):
    """The target repository is valid but it needs a newer renku version than is currently deployed."""

    code = SVC_ERROR_USER + 141
    userMessage = (
        "The target project requires renku version {minimum_version}, but this deployment is on "
        "version {current_version}."
    )
    devMessage = "Update the deployed core-service version to support newer projects."

    def __init__(self, exception=None, minimum_version=ERROR_NOT_AVAILABLE, current_version=ERROR_NOT_AVAILABLE):
        super().__init__(
            userMessage=self.userMessage.format(minimum_version=minimum_version, current_version=current_version),
            exception=exception,
        )


class UserProjectTemplateReferenceError(ServiceError):
    """The project's template original reference cannot be found anymore.

    The reference has probably been removed, either on purpose or as a side effect of a
    forced push.
    """

    code = SVC_ERROR_USER + 141
    userMessage = (
        "The project's template original reference has been removed or overwritten."
        " Manually changing it in a session may fix the problem."
        " Further details: {message}."
    )
    devMessage = "Template reference is not available anymore. Details: {message}."

    def __init__(self, exception):
        super().__init__(
            userMessage=self.userMessage.format(message=str(exception)),
            devMessage=self.devMessage.format(message=str(exception)),
            exception=exception,
        )


class UserUploadTooLargeError(ServiceError):
    """The user tried to upload a file that is too large.

    Maximum upload size can be set with the ``maximumUploadSizeBytes`` chart value or ``MAX_CONTENT_LENGTH``
    environment value.
    """

    code = SVC_ERROR_USER + 150
    userMessage = "The file you are trying to upload is too large. Maximum allowed size is: {maximum_size}"
    devMessage = "Uploaded file size was larger than ``MAX_CONTENT_LENGTH``."

    def __init__(self, exception, maximum_size: str):
        super().__init__(
            userMessage=self.userMessage.format(maximum_size=maximum_size),
            devMessage=self.devMessage.format(maximum_size=maximum_size),
            exception=exception,
        )


class ProgramInvalidGenericFieldsError(ServiceError):
    """One or more fields are unexpected.

    This error should not be triggered by any user input, but rather by unexpected fields.
    It is most likely a bug on the client side.
    """

    code = SVC_ERROR_PROGRAMMING + 1
    userMessage = "There was an unexpected error while handling project data."
    devMessage = "Unexpected fields have been provided: {wrong_values}"

    def __init__(self, exception=None, wrong_values=ERROR_NOT_AVAILABLE):
        super().__init__(
            devMessage=self.devMessage.format(wrong_values=wrong_values),
            exception=exception,
        )


class ProgramRepoUnknownError(ServiceError):
    """Unknown error when working with the repository.

    This is a fallback error and it should ideally never show up.
    When it happens, it's best to investigate the underlying error and either
    create a more specific error or open an issue.
    """

    code = SVC_ERROR_PROGRAMMING + 10
    userMessage = "Fatal error occurred while working on the repository."
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
    userMessage = "Fatal error occurred while processing a git operation on the repository."
    devMessage = "Unexpected git error. Git error message: {error_message}"

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


class ProgramProjectCreationError(ServiceError):
    """Error when creating a new project due to one or more wrong fields not controlled by the user."""

    code = SVC_ERROR_PROGRAMMING + 102
    userMessage = "Our servers could not create the new project."
    devMessage = (
        "Project creation from a Renku template failed."
        "It is likely that some non user input fields contain unexpected values: {error_message}."
    )

    def __init__(self, exception=None, error_message=ERROR_NOT_AVAILABLE):
        super().__init__(
            devMessage=self.devMessage.format(error_message=error_message),
            exception=exception,
        )


class ProgramProjectCorruptError(ServiceError):
    """Error triggered by an unexpected project corruption, not easily fixable by the user."""

    code = SVC_ERROR_PROGRAMMING + 120
    userMessage = "The Renku project seems partially corrupted, and the operation could not finish."
    devMessage = "Project corrupted. It probably requires manually fixing it, or reverting to a previous commit."

    def __init__(self, exception=None):
        super().__init__(exception=exception)


class ProgramGraphCorruptError(ServiceError):
    """Error triggered by an unexpected failure while exporting the graph ."""

    code = SVC_ERROR_PROGRAMMING + 121
    userMessage = "The Renku project seems partially corrupted, and the graph could not be generated."
    devMessage = "Could not generate the project graph. Check the Sentry exception for further details."

    def __init__(self, exception=None):
        super().__init__(exception=exception)


class ProgramUpdateProjectError(ServiceError):
    """Updating the project failed."""

    code = SVC_ERROR_USER + 140
    userMessage = "Our servers could not update the project succesfully. You could try doing it manually in a session."
    devMessage = "Updating the target project failed. Check the Sentry exception for further details."

    def __init__(self, exception=None):
        super().__init__(exception=exception)


class ProgramRenkuError(ServiceError):
    """Renku error unexpected at service level for a specific operation."""

    code = SVC_ERROR_PROGRAMMING + 200
    userMessage = "Our servers could not process the requested operation. The following details may help: {renku_error}"
    devMessage = "Unexpected Renku exception from a service operation: {renku_error}"

    def __init__(self, exception=None):
        message = str(exception)
        super().__init__(
            userMessage=self.userMessage.format(renku_error=message),
            devMessage=self.devMessage.format(renku_error=message),
            exception=exception,
        )


class ProgramHttpMethodError(ServiceError):
    """HTTP error 405 method not allowed.

    The method cannot be used on the target endpoint. The service only supports GET and POST.
    """

    code = SVC_ERROR_PROGRAMMING + 405
    userMessage = "One of the resources on our servers could not process the data properly."
    devMessage = (
        "Error 405 - method not allowed. Check if you are using GET or POST as specified in the "
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
    The only way to get more details is to access the Sentry exception.
    """

    code = SVC_ERROR_PROGRAMMING
    userMessage = "Fatal error occurred while working on the repository."
    devMessage = "Unexpected repository error. Check the Sentry exception for further details."

    def __init__(self, exception=None, http_error_code=599):
        super().__init__(exception=exception, code=self.code + http_error_code)


class ProgramInternalError(ServiceError):
    """Unknown internal error.

    This is an unexpected exception probably triggered at the core level.
    Please use Sentry to get more information.
    """

    code = SVC_ERROR_PROGRAMMING + 900
    userMessage = "Our servers generated an unexpected error while processing data."
    devMessage = "Renku service internal error. Further information: {error_message}"

    def __init__(self, exception=None, error_message=ERROR_NOT_AVAILABLE):
        super().__init__(devMessage=self.devMessage.format(error_message=error_message), exception=exception)


class IntermittentProjectIdError(ServiceError):
    """The project id cannot be found in the cache.

    This is an unexpected error possibly related to a cache malfunction, or just an unlucky case if the operation
    was attempted just after the project was cleaned up form the local cache. The latter is unlikely to happen.
    The culprit may be the client if it explicitly provides the project id.
    """

    code = SVC_ERROR_INTERMITTENT + 1
    userMessage = "An unexpected error occurred. This may be a temporary problem. Please try again in a few minutes."
    devMessage = (
        "Project id cannot be found. It may be a temporary problem. Check the Sentry exception for further details."
    )

    def __init__(self, exception=None):
        super().__init__(exception=exception)


class IntermittentAuthenticationError(ServiceError):
    """The user credentials were received but they are not valid.

    This may happen for a number of reasons. Triggering a new login will likely fix it.
    """

    code = SVC_ERROR_INTERMITTENT + 30
    userMessage = "Invalid user credentials. Please try to log out and in again."
    devMessage = "Authentication error. Check the Sentry exception to inspect the headers"

    def __init__(self, exception=None):
        super().__init__(exception=exception)


class IntermittentFileExistsError(ServiceError):
    """An operation failed because one or more files were expected not to exist.

    It may be a synchronization error happening when two or more concurrent operations overlap
    and one creates content unexpected from another one.
    """

    code = SVC_ERROR_INTERMITTENT + 110
    userMessage = "There was an error with the file '{file_name}'. Please refresh the page and try again."
    devMessage = "Unexpected error on file '{file_name}', possibly caused by concurrent actions."

    def __init__(self, exception=None, file_name=ERROR_NOT_AVAILABLE):
        super().__init__(
            userMessage=self.userMessage.format(file_name=file_name),
            devMessage=self.devMessage.format(file_name=file_name),
            exception=exception,
        )


class IntermittentFileNotExistsError(ServiceError):
    """An operation failed because one or more files were expected to exist.

    It may be a synchronization error happening when two or more concurrent operations overlap
    and one consumes the file expected by another one.
    """

    code = SVC_ERROR_INTERMITTENT + 111
    userMessage = "There was an error with the file '{file_name}'. Please refresh the page and try again."
    devMessage = "Unexpected error on file '{file_name}', possibly caused by concurrent actions."

    def __init__(self, exception=None, file_name=ERROR_NOT_AVAILABLE):
        super().__init__(
            userMessage=self.userMessage.format(file_name=file_name),
            devMessage=self.devMessage.format(file_name=file_name),
            exception=exception,
        )


class IntermittentSettingExistsError(ServiceError):
    """An operation failed because one or more project settings does not exist.

    It may be a synchronization error happening when two or more concurrent operations overlap
    and one tries to work on content already deleted from another one.
    """

    code = SVC_ERROR_INTERMITTENT + 112
    userMessage = "There was an error with the setting '{setting_name}'. Please refresh the page and try again."
    devMessage = "Unexpected error on setting '{setting_name}', possibly caused by concurrent actions."

    def __init__(self, exception=None, setting_name=ERROR_NOT_AVAILABLE):
        super().__init__(
            userMessage=self.userMessage.format(setting_name=setting_name),
            devMessage=self.devMessage.format(setting_name=setting_name),
            exception=exception,
        )


class IntermittentDatasetExistsError(ServiceError):
    """An operation failed because a dataset was expected not to exist.

    It may be a synchronization error happening when two or more concurrent operations overlap
    and one tries to create content already created from another one.
    """

    code = SVC_ERROR_INTERMITTENT + 130
    userMessage = "The dataset creation failed because it already exists. Please refresh the page."
    devMessage = "Unexpected error creating a dataset, possibly caused by concurrent actions."

    def __init__(self, exception=None):
        super().__init__(exception=exception)


class IntermittentProjectTemplateUnavailable(ServiceError):
    """The reference template for the project is currently unavailable.

    It may be a temporary issue in accessing the remote template, or it may have been deleted,
    moved, or otherwise not-accessible.
    """

    code = SVC_ERROR_INTERMITTENT + 140
    userMessage = (
        "The reference template for the project is currently unavailable."
        " It may be a temporary problem, or the template may not be accessible anymore."
    )
    devMessage = (
        "Error accessing the project template. This may be temporary, or the project may not be accessible anymore."
    )

    def __init__(self, exception=None):
        super().__init__(exception=exception)


class IntermittentWorkflowNotFound(ServiceError):
    """An operation failed because a workflow could not be found.

    It may be a synchronization error happening when two or more concurrent operations overlap
    and one tries to read content not yet created.
    """

    code = SVC_ERROR_INTERMITTENT + 150
    userMessage = "The workflow '{name_or_id}' could not be found. Check that the name/id is correct and try again."
    devMessage = "Unexpected error on workflow '{name_or_id}', possibly caused by concurrent actions."

    def __init__(self, exception=None, name_or_id=ERROR_NOT_AVAILABLE):
        super().__init__(
            userMessage=self.userMessage.format(name_or_id=name_or_id),
            devMessage=self.devMessage.format(name_or_id=name_or_id),
            exception=exception,
        )


class IntermittentTimeoutError(ServiceError):
    """An operation timed out."""

    code = SVC_ERROR_INTERMITTENT + 200
    userMessage = "The operation was taking too long. Please try it again."
    devMessage = "Timeout error. See Sentry exceptions for details."

    def __init__(self, exception=None):
        super().__init__(exception=exception)


class IntermittentLockError(ServiceError):
    """An operation was deadlocked."""

    code = SVC_ERROR_INTERMITTENT + 201
    userMessage = "The operation was taking too long. Please try it again."
    devMessage = "Deadlocked operation. See Sentry exceptions for details."

    def __init__(self, exception=None, message=ERROR_NOT_AVAILABLE):
        super().__init__(exception=exception)


class IntermittentRedisError(ServiceError):
    """An error occurred when interacting with Redis."""

    code = SVC_ERROR_INTERMITTENT + 202
    userMessage = "The servers could not run the request operation. Please try it again."
    devMessage = "Redis error. See Sentry exceptions for details."

    def __init__(self, exception=None):
        super().__init__(exception=exception)


class IntermittentCacheError(ServiceError):
    """An operation in the cache failed."""

    code = SVC_ERROR_INTERMITTENT + 203
    userMessage = "A server-side operation unexpectedly failed. Please try again."
    devMessage = "Cache error. See Sentry exceptions for details."

    def __init__(self, exception=None):
        super().__init__(exception=exception)
