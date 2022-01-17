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
"""Renku exceptions."""

import os
from typing import List

import click

from renku.core.management import RENKU_HOME


class RenkuException(Exception):
    """A base class for all Renku related exception.

    You can catch all errors raised by Renku SDK by using ``except RenkuException:``.
    """


class RequestError(RenkuException):
    """Raise when a ``requests`` call fails."""


class NotFound(RenkuException):
    """Raise when an object is not found in KG."""


class ParameterError(RenkuException):
    """Raise in case of invalid parameter."""

    def __init__(self, message, param_hint=None):
        """Build a custom message."""
        if param_hint:
            if isinstance(param_hint, (tuple, list)):
                param_hint = " / ".join('"{}"'.format(x) for x in param_hint)
            message = "Invalid parameter value for {}: {}".format(param_hint, message)
        else:
            message = "Invalid parameter value - {}".format(message)

        super().__init__(message)


class InvalidFileOperation(RenkuException):
    """Raise when trying to perform invalid file operation."""


class UsageError(RenkuException):
    """Raise in case of unintended usage of certain function calls."""


class ConfigurationError(RenkuException):
    """Raise in case of misconfiguration; use GitConfigurationError for git-related configuration errors."""


class AuthenticationError(RenkuException):
    """Raise when there is a problem with authentication."""


class DirtyRepository(RenkuException):
    """Raise when trying to work with dirty repository."""

    def __init__(self, repository):
        """Build a custom message."""
        super(DirtyRepository, self).__init__(
            "The repository is dirty. "
            'Please use the "git" command to clean it.'
            "\n\n" + str(repository.status()) + "\n\n"
            "Once you have added the untracked files, "
            'commit them with "git commit".'
        )


class DirtyRenkuDirectory(RenkuException):
    """Raise when a directory in the renku repository is dirty."""

    def __init__(self, repository):
        """Build a custom message."""
        super(DirtyRenkuDirectory, self).__init__(
            (
                "The renku directory {0} contains uncommitted changes.\n"
                'Please use "git" command to resolve.\n'
                "Files within {0} directory "
                "need to be manually committed or removed."
            ).format(RENKU_HOME)
            + "\n\n"
            + str(repository.status())
            + "\n\n"
        )


class ProtectedFiles(RenkuException):
    """Raise when trying to work with protected files."""

    def __init__(self, ignored):
        """Build a custom message."""
        super(ProtectedFiles, self).__init__(
            "The following paths are protected as part of renku:"
            "\n\n" + "\n".join("\t" + click.style(str(path), fg="yellow") for path in ignored) + "\n"
            "They cannot be used in renku commands."
        )


class MigrationRequired(RenkuException):
    """Raise when migration is required."""

    def __init__(self):
        """Build a custom message."""
        super().__init__(
            "Project version is outdated and a migration is required.\n" "Run `renku migrate` command to fix the issue."
        )


class ProjectNotSupported(RenkuException):
    """Raise when project version is newer than the supported version."""

    def __init__(self):
        """Build a custom message."""
        super().__init__(
            "Project is not supported by this version of Renku.\n" "Upgrade to the latest version of Renku."
        )


class ProjectNotFound(RenkuException):
    """Raise when one or more projects couldn't be found in the KG."""


class NothingToCommit(RenkuException):
    """Raise when there is nothing to commit."""

    def __init__(self):
        """Build a custom message."""
        super(NothingToCommit, self).__init__("There is nothing to commit.")


class DatasetFileExists(RenkuException):
    """Raise when file is already in dataset."""

    def __init__(self):
        """Build a custom message."""
        super(DatasetFileExists, self).__init__("File already exists in dataset. Use --force to add.")


class CommitMessageEmpty(RenkuException):
    """Raise invalid commit message."""

    def __init__(self):
        """Build a custom message."""
        super(CommitMessageEmpty, self).__init__("Invalid commit message.")


class FailedMerge(RenkuException):
    """Raise when automatic merge failed."""

    def __init__(self, repository, branch, merge_args):
        """Build a custom message."""
        super(FailedMerge, self).__init__(
            "Failed merge of branch {0} with args {1}".format(branch, ",".join(merge_args))
            + "The automatic merge failed.\n\n"
            'Please use the "git" command to clean it.'
            "\n\n" + str(repository.status())
        )


class UnmodifiedOutputs(RenkuException):
    """Raise when there are unmodified outputs in the repository."""

    def __init__(self, repository, unmodified):
        """Build a custom message."""
        super(UnmodifiedOutputs, self).__init__(
            "There are no detected new outputs or changes.\n"
            "\nIf any of the following files should be considered as outputs,"
            "\nthey need to be removed first in order to be detected "
            "correctly."
            '\n  (use "git rm <file>..." to remove them first)'
            "\n\n" + "\n".join("\t" + click.style(path, fg="green") for path in unmodified) + "\n"
            "\nOnce you have removed the files that should be used as outputs,"
            "\nyou can safely rerun the previous command."
            "\nYou can use --output flag to specify outputs explicitly."
        )


class InvalidOutputPath(RenkuException):
    """Raise when trying to work with an invalid output path."""


class OutputsNotFound(RenkuException):
    """Raise when there are not any detected outputs in the repository."""

    def __init__(self, repository, inputs):
        """Build a custom message."""
        from pathlib import Path

        msg = "There are not any detected outputs in the repository."

        paths = [
            os.path.relpath(input_.default_value)  # relative to cur path
            for input_ in inputs
            if Path(input_.default_value).is_dir()
        ]

        if paths:
            msg += (
                '\n  (use "git rm <file>..." to remove them first)'
                "\n\n" + "\n".join("\t" + click.style(path, fg="yellow") for path in paths) + "\n\n"
                "Once you have removed files that should be used as outputs,\n"
                "you can safely rerun the previous command."
                "\nYou can use --output flag to specify outputs explicitly."
            )
        else:
            msg += "\n\nIf you want to track the command anyway use " "--no-output option."

        super(OutputsNotFound, self).__init__(msg)


class InvalidInputPath(RenkuException):
    """Raise when input path does not exist or is not in the repository."""


class InvalidSuccessCode(RenkuException):
    """Raise when the exit-code is not 0 or redefined."""

    def __init__(self, returncode, success_codes=None):
        """Build a custom message."""
        if not success_codes:
            msg = "Command returned non-zero exit status {0}.".format(returncode)
        else:
            msg = "Command returned {0} exit status, but it expects {1}".format(
                returncode, ", ".join((str(code) for code in success_codes))
            )
        super(InvalidSuccessCode, self).__init__(msg)


class DatasetNotFound(RenkuException):
    """Raise when dataset is not found."""

    def __init__(self, *, name=None, message=None):
        """Build a custom message."""
        if message:
            msg = message
        elif name:
            msg = f'Dataset "{name}" is not found.'
        else:
            msg = "Dataset is not found."
        super().__init__(msg)


class DatasetExistsError(RenkuException):
    """Raise when trying to create an existing dataset."""


class ExternalStorageNotInstalled(RenkuException):
    """Raise when LFS is required but not found or installed in the repository."""

    def __init__(self, repository):
        """Build a custom message."""
        msg = (
            "External storage is not installed, "
            "but this repository depends on it. \n"
            "By running this command without storage installed "
            "you could be committing\n"
            "large files directly to the git repository.\n\n"
            "If this is your intention, please repeat the command with "
            "the -S flag (e.g. renku -S run <cmd>), \n"
            'otherwise install LFS with "git lfs install --local".'
        )

        super(ExternalStorageNotInstalled, self).__init__(msg)


class ExternalStorageDisabled(RenkuException):
    """Raise when disabled repository storage API is trying to be used."""

    def __init__(self, repository):
        """Build a custom message."""
        msg = (
            "External storage is not configured, "
            "but this action is trying to use it.\n"
            "By running this command without storage enabled "
            "you could be committing\n"
            "large files directly to the git repository.\n\n"
            "If this is your intention, please repeat the command with "
            "the -S flag (e.g. renku -S run <cmd>), \n"
            'otherwise install e.g. git-LFS with "git lfs install --local".'
        )

        super(ExternalStorageDisabled, self).__init__(msg)


class UninitializedProject(RenkuException):
    """Raise when a project does not seem to have been initialized yet."""

    def __init__(self, repo_path):
        """Build a custom message."""
        msg = "{repo_path} does not seem to be a Renku project.\n" 'Initialize it with "renku init"'.format(
            repo_path=repo_path
        )
        super(UninitializedProject, self).__init__(msg)


class InvalidAccessToken(RenkuException):
    """Raise when access token is incorrect."""

    def __init__(self):
        """Build a custom message."""
        msg = "Invalid access token.\n" "Please, update access token."
        super(InvalidAccessToken, self).__init__(msg)


class GitError(RenkuException):
    """Raised when a Git operation fails."""


class InvalidGitURL(GitError):
    """Raise when a Git URL is not valid."""


class GitCommandError(GitError):
    """Raised when a Git command fails."""

    def __init__(self, message="Git command failed.", command=None, stdout=None, stderr=None, status=None):
        super().__init__(message)
        self.command = command
        self.stdout = stdout
        self.stderr = stderr
        self.status = status


class GitCommitNotFoundError(GitError):
    """Raised when a commit cannot be found in a Repository."""


class GitRemoteNotFoundError(GitError):
    """Raised when a remote cannot be found."""


class GitReferenceNotFoundError(GitError):
    """Raised when a branch or a reference cannot be found."""


class GitConfigurationError(GitError):
    """Raised when a git configuration cannot be accessed."""


class GitMissingUsername(GitConfigurationError):
    """Raise when the username is not configured."""

    def __init__(self, message=None):
        """Build a custom message."""
        message = message or (
            "The user name is not configured. "
            'Please use the "git config" command to configure it.\n\n'
            '\tgit config --set user.name "John Doe"\n'
        )
        super().__init__(message)


class GitMissingEmail(GitConfigurationError):
    """Raise when the email is not configured."""

    def __init__(self, message=None):
        """Build a custom message."""
        message = message or (
            "The email address is not configured. "
            'Please use the "git config" command to configure it.\n\n'
            '\tgit config --set user.email "john.doe@example.com"\n'
        )
        super().__init__(message)


class GitLFSError(RenkuException):
    """Raised when a Git LFS operation fails."""


class UrlSchemeNotSupported(RenkuException):
    """Raised when adding data from unsupported URL schemes."""


class OperationError(RenkuException):
    """Raised when an operation at runtime raises an error."""


class SHACLValidationError(RenkuException):
    """Raises when SHACL validation of the graph fails."""


class CommitProcessingError(RenkuException):
    """Raised when a commit couldn't be processed during graph build."""


class WorkflowExecuteError(RenkuException):
    """Raises when a workflow execution fails."""

    def __init__(self, fail_reason=None):
        """Build a custom message."""

        msg = "Unable to finish executing workflow"
        if fail_reason:
            msg += f": {fail_reason}"
        super(WorkflowExecuteError, self).__init__(msg)


class WorkflowRerunError(RenkuException):
    """Raises when a workflow re-execution fails."""

    def __init__(self, workflow_file):
        """Build a custom message."""
        msg = (
            "Unable to finish re-executing workflow; check the workflow"
            " execution outline above and the generated {0} file for"
            " potential issues, then remove the {0} file and try again".format(str(workflow_file))
        )
        super(WorkflowRerunError, self).__init__(msg)


class InvalidTemplateError(RenkuException):
    """Raised when using a non-valid template."""


class ExportError(RenkuException):
    """Raised when a dataset cannot be exported."""


class TemplateUpdateError(RenkuException):
    """Raised when a project couldn't be updated from its template."""


class DockerfileUpdateError(RenkuException):
    """Raised when the renku version in the Dockerfile couldn't be updated."""


class MigrationError(RenkuException):
    """Raised when something went wrong during migrations."""


class RenkuImportError(RenkuException):
    """Raised when a dataset cannot be imported."""

    def __init__(self, exp, msg):
        """Embed exception and build a custom message."""
        self.exp = exp
        super(RenkuImportError, self).__init__(msg)


class CommandNotFinalizedError(RenkuException):
    """Raised when a non-finalized command is executed."""


class CommandFinalizedError(RenkuException):
    """Raised when trying to modify a finalized command builder."""


class RenkuSaveError(RenkuException):
    """Raised when renku save doesn't work."""


class DatasetImageError(RenkuException):
    """Raised when a local dataset image is not accessible."""


class NodeNotFoundError(RenkuException):
    """Raised when NodeJs is not installed on the system."""

    def __init__(self):
        """Build a custom message."""
        msg = (
            "NodeJs could not be found on this system\n"
            "Please install it, for details see https://nodejs.org/en/download/package-manager/"
        )
        super(NodeNotFoundError, self).__init__(msg)


class ObjectNotFoundError(RenkuException):
    """Raised when an object is not found in the storage."""

    def __init__(self, filename):
        """Embed exception and build a custom message."""
        super().__init__(f"Cannot find object: '{filename}'")


class ParameterNotFoundError(RenkuException):
    """Raised when a parameter reference cannot be resolved to a parameter."""

    def __init__(self, parameter: str, workflow: str):
        """Embed exception and build a custom message."""
        super().__init__(f"Cannot find parameter '{parameter}' on workflow {workflow}")


class MappingExistsError(RenkuException):
    """Raised when a parameter mapping exists already."""

    def __init__(self, existing_mappings: List[str]):
        """Embed exception and build a custom message."""
        existing = "\n\t".join(existing_mappings)
        super().__init__(
            "Duplicate mapping detected. The following mapping targets "
            f"already exist on these mappings: \n\t{existing}"
        )


class MappingNotFoundError(RenkuException):
    """Raised when a parameter mapping does not exist."""

    def __init__(self, mapping: str, workflow: str):
        """Embed exception and build a custom message."""
        super().__init__(f"Cannot find mapping '{mapping}' on workflow {workflow}")


class ChildWorkflowNotFoundError(RenkuException):
    """Raised when a parameter reference cannot be resolved to a parameter."""

    def __init__(self, child: str, workflow: str):
        """Embed exception and build a custom message."""
        super().__init__(f"Cannot find child step '{child}' on workflow {workflow}")


class ParameterLinkError(RenkuException):
    """Raised when a parameter link cannot be created."""

    def __init__(self, reason: str):
        """Embed exception and build a custom message."""
        super().__init__(f"Can't create parameter link, reason: {reason}")


class GraphCycleError(RenkuException):
    """Raised when a parameter reference cannot be resolved to a parameter."""

    def __init__(self, cycles: List[List[str]]):
        """Embed exception and build a custom message."""
        cycles = "), (".join(", ".join(cycle) for cycle in cycles)
        super().__init__(f"Cycles detected in execution graph: ({cycles})")


class NothingToExecuteError(RenkuException):
    """Raised when a rerun/update command does not execute any workflows."""


class TerminalSizeError(RenkuException):
    """Raised when terminal is too small for a command."""
