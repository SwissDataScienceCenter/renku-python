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

from marshmallow import ValidationError

from renku.core.errors import GitError
from renku.service.errors import (
    ProgramProjectCreationError,
    UserProjectCreationError,
    UserRepoBranchInvalidError,
    UserRepoUrlInvalidError,
    UserTemplateInvalidError,
)


def handle_templates_read_errors(f):
    """Wrapper which handles reading templates errors."""
    # noqa
    @wraps(f)
    def decorated_function(*args, **kwargs):
        """Represents decorated function."""
        try:
            return f(*args, **kwargs)
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

        def match_schema(target, message):
            if re.match(f".*{target}.*contains.*unsupported characters.*", message):
                return True
            return False

        try:
            return f(*args, **kwargs)
        except ValidationError as e:
            if getattr(e, "field_name", None) == "_schema":
                error_message = str(e)
                if match_schema("Project name", error_message):
                    raise UserProjectCreationError(e, "project name must contain at least a valid character")
                elif match_schema("git_url", error_message):
                    raise ProgramProjectCreationError(e, "git_url is invalid")
            raise
        except KeyError as e:
            # NOTE: it's hard to determine if the error is user generated here
            error_message = str(e).strip("'").replace("_", " ")
            raise UserProjectCreationError(e, f"provide a value for {error_message}")

    return decorated_function
