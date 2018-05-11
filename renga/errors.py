# -*- coding: utf-8 -*-
#
# Copyright 2017-2018 - Swiss Data Science Center (SDSC)
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
"""Renga exceptions."""

import click
import requests


class RengaException(Exception):
    """A base class for all Renga related exception.

    You can catch all errors raised by Renga SDK by using
    ``except RengaException:``.
    """


class APIError(requests.exceptions.HTTPError, RengaException):
    """Catch HTTP errors from API calls."""

    @classmethod
    def from_http_exception(cls, e):
        """Create ``APIError`` from ``requests.exception.HTTPError``."""
        assert isinstance(e, requests.exceptions.HTTPError)
        response = e.response
        try:
            message = response.json()['message']
        except (KeyError, ValueError):
            message = response.content.strip()

        raise cls(message)


class UnexpectedStatusCode(APIError):
    """Raise when the status code does not match specification."""

    def __init__(self, response):
        """Build custom message."""
        super(UnexpectedStatusCode, self).__init__(
            'Unexpected status code: {0}'.format(response.status_code),
            response=response
        )

    @classmethod
    def return_or_raise(cls, response, expected_status_code):
        """Check for ``expected_status_code``."""
        try:
            if response.status_code in expected_status_code:
                return response
        except TypeError:
            if response.status_code == expected_status_code:
                return response

        raise cls(response)


class InvalidFileOperation(RengaException):
    """Raise when trying to perfrom invalid file operation."""


class UsageError(RengaException):
    """Raise in case of unintended usage of certain function calls."""


class DirtyRepository(RengaException, click.ClickException):
    """Raise when trying to work with dirty repository."""

    def __init__(self, repo):
        """Build a custom message."""
        super(DirtyRepository, self).__init__(
            'The repository is dirty. '
            'Please use the "git" command to clean it.'
            '\n\n' + str(repo.git.status()) + '\n\n'
            'Once you have added the untracked files, '
            'commit them with "git commit".'
        )


class NotFound(APIError):
    """Raise when an API object is not found."""
