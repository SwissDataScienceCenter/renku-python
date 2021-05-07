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
from renku.core.errors import RenkuException


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
