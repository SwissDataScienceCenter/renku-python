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
"""Renku exceptions."""

import os

import click
import requests


class RenkuException(Exception):
    """A base class for all Renku related exception.

    You can catch all errors raised by Renku SDK by using
    ``except RenkuException:``.
    """


class APIError(requests.exceptions.HTTPError, RenkuException):
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


class InvalidFileOperation(RenkuException):
    """Raise when trying to perfrom invalid file operation."""


class UsageError(RenkuException):
    """Raise in case of unintended usage of certain function calls."""


class DirtyRepository(RenkuException, click.ClickException):
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


class UnmodifiedOutputs(RenkuException, click.ClickException):
    """Raise when there are unmodified outputs in the repository."""

    def __init__(self, repo, unmodified):
        """Build a custom message."""
        super(UnmodifiedOutputs, self).__init__(
            'There are unmodified outputs in the repository.\n'
            '  (use "git rm <file>..." to remove them first)'
            '\n\n' + '\n'.
            join('\t' + click.style(path, fg='green')
                 for path in unmodified) + '\n\n'
            'Once you have removed the outputs, '
            'you can safely rerun the previous command.'
        )


class OutputsNotFound(RenkuException, click.ClickException):
    """Raise when there are not any detected outputs in the repository."""

    def __init__(self, repo, inputs):
        """Build a custom message."""
        msg = 'There are not any detected outputs in the repository.'

        from renku.models.cwl.types import File
        paths = [
            os.path.relpath(input_.default.path)
            for input_ in inputs if isinstance(input_.default, File)
        ]

        if paths:
            msg += (
                '\n  (use "git rm <file>..." to remove them first)'
                '\n\n' + '\n'.join(
                    '\t' + click.style(path, fg='yellow') for path in paths
                ) + '\n\n'
                'Once you have removed files that should be used as outputs,\n'
                'you can safely rerun the previous command.'
            )
        else:
            msg += (
                '\n\nIf you want to track the command anyway use '
                '--no-output option.'
            )

        super(OutputsNotFound, self).__init__(msg)


class NotFound(APIError):
    """Raise when an API object is not found."""
