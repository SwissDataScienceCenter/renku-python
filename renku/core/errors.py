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
"""Renku exceptions."""

from pathlib import Path
from typing import List, Union

import click
from packaging.version import Version

from renku.core.constant import RENKU_HOME


class RenkuException(Exception):
    """A base class for all Renku related exception.

    You can catch all errors raised by Renku SDK by using ``except RenkuException:``.
    """


class ProjectContextError(RenkuException):
    """Raise when no project context is pushed or there is a project context-related error."""


class DatasetException(RenkuException):
    """Base class for all dataset-related exceptions."""


class ActivityDownstreamNotEmptyError(RenkuException):
    """Raised when an activity cannot be deleted because its downstream is not empty."""

    def __init__(self, activity):
        self.activity = activity
        super().__init__(f"Activity '{activity.id}' has non-empty downstream")


class LockError(RenkuException):
    """Raise when a project cannot be locked."""


class RequestError(RenkuException):
    """Raise when a ``requests`` call fails."""


class NotFound(RenkuException):
    """Raise when an object is not found in KG."""


class ParameterError(RenkuException):
    """Raise in case of invalid parameter."""

    def __init__(self, message, param_hint=None, show_prefix: bool = True):
        """Build a custom message."""
        if param_hint:
            if isinstance(param_hint, (tuple, list)):
                param_hint = " / ".join('"{}"'.format(x) for x in param_hint)
            message = f"Invalid parameter value for {param_hint}: {message}"
        else:
            if show_prefix:
                message = f"Invalid parameter value - {message}"

        super().__init__(message)


class ParseError(RenkuException):
    """Raise when a workflow file command has invalid format."""


class IncompatibleParametersError(ParameterError):
    """Raise in case of incompatible parameters/flags."""

    def __init__(self, a: str = None, b: str = None):
        """Build a custom message."""
        message = f"{a} is incompatible with {b}" if a is not None and b is not None else "Incompatible parameters"
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

    def __init__(self, ignored: List[Union[Path, str]]):
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

    def __init__(self):
        """Build a custom message."""

        msg = (
            "There are not any detected outputs in the repository. This can be due to your command not creating "
            "any new files or due to files that get created already existing before the command was run. In the "
            "latter case, you can remove those files prior to running your command.\nIf you want to track the command"
            "without outputs, use the use --no-output option.\nYou can also use the --output flag to track outputs"
            "manually."
        )

        super(OutputsNotFound, self).__init__(msg)


class InvalidInputPath(RenkuException):
    """Raise when input path does not exist or is not in the repository."""


class InvalidSuccessCode(RenkuException):
    """Raise when the exit-code is not 0 or redefined."""

    def __init__(self, return_code, success_codes=None, message=None):
        """Build a custom message."""
        if message:
            msg = message
        elif not success_codes:
            msg = "Command returned non-zero exit status {0}.".format(return_code)
        else:
            msg = "Command returned {0} exit status, but it expects {1}".format(
                return_code, ", ".join((str(code) for code in success_codes))
            )
        super(InvalidSuccessCode, self).__init__(msg)


class DatasetNotFound(DatasetException):
    """Raise when dataset is not found."""

    def __init__(self, *, name=None, message=None):
        """Build a custom message."""
        if message:
            msg = message
        elif name:
            msg = f"Dataset '{name}' is not found."
        else:
            msg = "Dataset is not found."
        super().__init__(msg)


class DatasetTagNotFound(DatasetException):
    """Raise when a tag can't be found."""

    def __init__(self, tag) -> None:
        msg = f"Couldn't find dataset tag '{tag}'."
        super().__init__(msg)


class FileNotFound(RenkuException):
    """Raise when a file is not found."""

    def __init__(self, path, checksum=None, revision=None):
        """Build a custom message."""
        if checksum:
            message = f"File not found in the repository: {checksum}:{path}"
        elif revision:
            message = f"File not found in the repository: {path}@{revision}"
        else:
            message = f"Cannot find file {path}"

        super().__init__(message)


class ExternalFileNotFound(DatasetException):
    """Raise when an external file is not found."""

    def __init__(self, path):
        """Build a custom message."""
        super().__init__(f"Cannot find external file '{path}'")


class DirectoryNotEmptyError(RenkuException):
    """Raised when a directory passed as output is not empty."""

    def __init__(self, path):
        """Build a custom message."""
        super().__init__(f"Destination directory is not empty: '{path}'")


class DatasetExistsError(DatasetException):
    """Raise when trying to create an existing dataset."""

    def __init__(self, name):
        super().__init__(f"Dataset exists: '{name}'")


class ExternalStorageNotInstalled(RenkuException):
    """Raise when LFS is required but not found or installed in the repository."""

    def __init__(self):
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

    def __init__(self):
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
        """Build a custom message."""
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


class OperationError(RenkuException):
    """Raised when an operation at runtime raises an error."""


class SHACLValidationError(RenkuException):
    """Raises when SHACL validation of the graph fails."""


class CommitProcessingError(RenkuException):
    """Raised when a commit couldn't be processed during graph build."""


class ExportError(DatasetException):
    """Raised when a dataset cannot be exported."""


class TemplateError(RenkuException):
    """Base class for template-related exceptions."""


class InvalidTemplateError(TemplateError):
    """Raised when using a non-valid template."""


class TemplateMissingReferenceError(TemplateError):
    """Raised when using a non-valid template."""


class TemplateUpdateError(TemplateError):
    """Raised when a project couldn't be updated from its template."""


class TemplateNotFoundError(TemplateError):
    """Raised when a template cannot be found in a template source or at a specific reference."""


class DockerfileUpdateError(RenkuException):
    """Raised when the renku version in the Dockerfile couldn't be updated."""


class MigrationError(RenkuException):
    """Raised when something went wrong during migrations."""


class DatasetImportError(DatasetException):
    """Raised when a dataset cannot be imported/pulled from a remote source."""


class CommandNotFinalizedError(RenkuException):
    """Raised when a non-finalized command is executed."""


class CommandFinalizedError(RenkuException):
    """Raised when trying to modify a finalized command builder."""


class RenkuSaveError(RenkuException):
    """Raised when renku save doesn't work."""


class DatasetImageError(DatasetException):
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


class WorkflowError(RenkuException):
    """Base class for workflow-related errors."""


class WorkflowExportError(WorkflowError):
    """Raises when a workflow cannot be exported."""


class DuplicateWorkflowNameError(WorkflowError):
    """Raises when a workflow name already exists."""


class WorkflowExecuteError(WorkflowError):
    """Raises when a workflow execution fails."""

    def __init__(self, fail_reason=None, show_prefix: bool = True):
        """Build a custom message."""

        msg = "Unable to finish executing workflow"
        if fail_reason:
            msg += f": {fail_reason}"
        super().__init__(msg)


class WorkflowRerunError(WorkflowError):
    """Raises when a workflow re-execution fails."""

    def __init__(self, cwl_file):
        """Build a custom message."""
        msg = (
            "Unable to finish re-executing workflow; check the workflow execution outline above and the generated "
            f"{cwl_file} file for potential issues, then remove the {cwl_file} file and try again"
        )
        super().__init__(msg)


class ParameterNotFoundError(WorkflowError):
    """Raised when a parameter reference cannot be resolved to a parameter."""

    def __init__(self, parameter: str, workflow: str):
        """Embed exception and build a custom message."""
        super().__init__(f"Cannot find parameter '{parameter}' on workflow {workflow}")


class MappingExistsError(WorkflowError):
    """Raised when a parameter mapping exists already."""

    def __init__(self, existing_mappings: List[str]):
        """Embed exception and build a custom message."""
        existing = "\n\t".join(existing_mappings)
        super().__init__(
            "Duplicate mapping detected. The following mapping targets "
            f"already exist on these mappings: \n\t{existing}"
        )


class MappingNotFoundError(WorkflowError):
    """Raised when a parameter mapping does not exist."""

    def __init__(self, mapping: str, workflow: str):
        """Embed exception and build a custom message."""
        super().__init__(f"Cannot find mapping '{mapping}' on workflow {workflow}")


class ChildWorkflowNotFoundError(WorkflowError):
    """Raised when a child could not be found on a composite workflow."""

    def __init__(self, child: str, workflow: str):
        """Embed exception and build a custom message."""
        super().__init__(f"Cannot find child step '{child}' on workflow {workflow}")


class WorkflowNotFoundError(WorkflowError):
    """Raised when a workflow could not be found."""

    def __init__(self, name_or_id: str):
        """Embed exception and build a custom message."""
        self.name_or_id = name_or_id
        super().__init__(f"The specified workflow '{name_or_id}' cannot be found.")


class ParameterLinkError(RenkuException):
    """Raised when a parameter link cannot be created."""

    def __init__(self, reason: str):
        """Embed exception and build a custom message."""
        super().__init__(f"Can't create parameter link, reason: {reason}")


class GraphCycleError(RenkuException):
    """Raised when a parameter reference cannot be resolved to a parameter."""

    def __init__(self, cycles: List[List[str]], message: str = None):
        """Embed exception and build a custom message."""
        if message:
            super().__init__(message)
        else:
            cycle_str = "), (".join(", ".join(cycle) for cycle in cycles)
            super().__init__(
                f"Cycles detected in execution graph: ({cycle_str})\nCircular workflows are not supported in renku\n"
                "If this happened as part of a 'renku run' or 'renku workflow execute', please git reset and clean"
                "the project and try again. This might be due to renku erroneously detecting an input as an output, "
                "if so, please specify the wrongly detected output as an explicit input using '--input'."
            )


class NothingToExecuteError(RenkuException):
    """Raised when a rerun/update command does not execute any workflows."""


class TerminalSizeError(RenkuException):
    """Raised when terminal is too small for a command."""


class DockerError(RenkuException):
    """Raised when error has occurred while executing docker command."""

    def __init__(self, reason: str):
        """Embed exception and build a custom message."""
        super().__init__(f"Docker failed: {reason}")


class SessionStartError(RenkuException):
    """Raised when an error occurs trying to start sessions."""


class RenkulabSessionError(SessionStartError):
    """Raised when an error occurs trying to start sessions with the notebook service."""


class RenkulabSessionGetUrlError(RenkuException):
    """Raised when Renku deployment's URL cannot be gotten from project's remotes or configured remotes."""

    def __init__(self):
        message = (
            "Cannot determine the Renku deployment's URL. Ensure your current project is a valid Renku project and has "
            "a remote URL."
        )
        super().__init__(message)


class NotebookSessionNotReadyError(RenkuException):
    """Raised when a user attempts to open a session that is not ready."""


class NotebookSessionImageNotExistError(RenkuException):
    """Raised when a user attempts to start a session with an image that does not exist."""


class MetadataMergeError(RenkuException):
    """Raise when merging of metadata failed."""


class MinimumVersionError(RenkuException):
    """Raised when accessing a project whose minimum version is larger than the current renku version."""

    def __init__(self, current_version: Version, minimum_version: Version) -> None:
        self.current_version = current_version
        self.minimum_version = minimum_version

        super().__init__(
            f"You are using renku version {current_version} but this project requires at least version "
            f"{minimum_version}. Please upgrade renku to work on this project."
        )


class DatasetProviderNotFound(DatasetException, ParameterError):
    """Raised when a dataset provider cannot be found based on a URI or a provider name."""

    def __init__(self, *, name: str = None, uri: str = None, message: str = None):
        if message is None:
            if name:
                message = f"Provider '{name}' not found"
            elif uri:
                message = f"Cannot find a provider to process '{uri}'"
            else:
                message = "Provider not found"

        super().__init__(message)


class StorageProviderNotFound(DatasetException, ParameterError):
    """Raised when a storage provider cannot be found based on a URI."""

    def __init__(self, uri: str):
        super().__init__(f"Cannot find a storage provider to process '{uri}'")


class RCloneException(DatasetException):
    """Base class for all rclone-related exceptions."""


class StorageObjectNotFound(RCloneException):
    """Raised when a file or directory cannot be found in the remote storage."""

    def __init__(self, error: str = None):
        message = "Cannot find file/directory"
        if error:
            message = f"{message}: {error}"

        super().__init__(message)
