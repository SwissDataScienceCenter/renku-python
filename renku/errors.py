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


class UsageError(RenkuException, click.UsageError):
    """Raise in case of unintended usage of certain function calls."""


class ConfigurationError(RenkuException, click.ClickException):
    """Raise in case of misconfiguration."""


class MissingUsername(ConfigurationError):
    """Raise when the username is not configured."""

    def __init__(self, message=None):
        """Build a custom message."""
        message = message or (
            'The user name is not configured. '
            'Please use the "git config" command to configure it.\n\n'
            '\tgit config --set user.name "John Doe"\n'
        )
        super(MissingUsername, self).__init__(message)


class MissingEmail(ConfigurationError):
    """Raise when the email is not configured."""

    def __init__(self, message=None):
        """Build a custom message."""
        message = message or (
            'The email address is not configured. '
            'Please use the "git config" command to configure it.\n\n'
            '\tgit config --set user.email "john.doe@example.com"\n'
        )
        super(MissingUsername, self).__init__(message)


class AuthenticationError(RenkuException, click.ClickException):
    """Raise when there is a problem with authentication."""


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
            'There are no detected new outputs or changes.\n'
            '\nIf any of the following files should be considered as outputs,'
            '\nthey need to be removed first in order to be detected '
            'correctly.'
            '\n  (use "git rm <file>..." to remove them first)'
            '\n\n' + '\n'.
            join('\t' + click.style(path, fg='green')
                 for path in unmodified) + '\n'
            '\nOnce you have removed the files that should be used as outputs,'
            '\nyou can safely rerun the previous command.'
        )


class InvalidOutputPath(RenkuException, click.ClickException):
    """Raise when trying to work with an invalid output path."""


class OutputsNotFound(RenkuException, click.ClickException):
    """Raise when there are not any detected outputs in the repository."""

    def __init__(self, repo, inputs):
        """Build a custom message."""
        msg = 'There are not any detected outputs in the repository.'

        from renku.models.cwl.types import File
        paths = [
            os.path.relpath(str(input_.default.path))  # relative to cur path
            for input_ in inputs  # only choose files
            if isinstance(input_.default, File)
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


class InvalidSuccessCode(RenkuException, click.ClickException):
    """Raise when the exit-code is not 0 or redefined."""

    def __init__(self, returncode, success_codes=None):
        """Build a custom message."""
        if not success_codes:
            msg = 'Command returned non-zero exit status {0}.'.format(
                returncode
            )
        else:
            msg = (
                'Command returned {0} exit status, but it expects {1}'.format(
                    returncode,
                    ', '.join((str(code) for code in success_codes))
                )
            )
        super(InvalidSuccessCode, self).__init__(msg)


class NotFound(APIError):
    """Raise when an API object is not found."""


class ExternalStorageNotInstalled(RenkuException, click.ClickException):
    """Raise when LFS is required but not found or installed in the repo."""

    def __init__(self, repo):
        """Build a custom message."""
        msg = (
            'Git-LFS is either not installed or not configured '
            'for this repo.\n'
            'By running this command without LFS you could be committing\n'
            'large files directly to the git repository.\n\n'
            'If this is your intention, please repeat the command with '
            'the -S flag (e.g. renku -S run <cmd>), \n'
            'otherwise install LFS with "git lfs install --local".'
        )

        super(ExternalStorageNotInstalled, self).__init__(msg)


class UninitializedProject(RenkuException, click.ClickException):
    """Raise when a project does not seem to have been initialized yet."""

    def __init__(self, repo_path):
        """Build a custom message."""
        msg = (
            '{repo_path} does not seem to be a Renku project.\n'
            'Initialize it with "renku init"'.format(repo_path=repo_path)
        )
        super(UninitializedProject, self).__init__(msg)
