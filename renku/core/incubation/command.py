# -*- coding: utf-8 -*-
#
# Copyright 2018-2020 - Swiss Data Science Center (SDSC)
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
"""Command builder."""

import contextlib
import functools
from collections import defaultdict

import click

from renku.core import errors
from renku.core.management import LocalClient
from renku.core.management.config import RENKU_HOME
from renku.core.management.migrate import check_for_migration
from renku.core.management.repository import default_path
from renku.core.utils import communication


def check_finalized(f):
    """Decorator to prevent modification of finalized builders."""

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        """Decorator to prevent modification of finalized builders."""
        if not args or not isinstance(args[0], Command):
            raise errors.ParameterError("Command hooks need to be `Command` object methods.")

        if args[0].finalized:
            raise errors.CommandFinalizedError("Cannot modify a finalized `Command`.")

        return f(*args, **kwargs)

    return wrapper


class Command:
    """Base renku command builder."""

    CLIENT_HOOK_ORDER = 1

    def __init__(self):
        """__init__ of Command."""
        self.pre_hooks = defaultdict(list)
        self.post_hooks = defaultdict(list)
        self._operation = None
        self._finalized = False
        self._track_std_streams = False
        self._git_isolation = False
        self._working_directory = None

    def __getattr__(self, name):
        """Bubble up attributes of wrapped builders."""
        if "_builder" in self.__dict__:
            return getattr(self._builder, name)

        raise AttributeError(f"{self.__class__.__name__} object has no attribute {name}")

    def __setattr__(self, name, value):
        """Set attributes of wrapped builders."""
        if hasattr(self, "_builder") and self.__class__ is not self._builder.__class__:
            self._builder.__setattr__(name, value)

        object.__setattr__(self, name, value)

    def _pre_hook(self, builder, context, *args, **kwargs):
        """Setup local client."""
        ctx = click.get_current_context(silent=True)
        if ctx is None:
            client = LocalClient(
                path=default_path(self._working_directory or "."),
                renku_home=RENKU_HOME,
                external_storage_requested=True,
            )
            ctx = click.Context(click.Command(builder._operation))
        else:
            client = ctx.ensure_object(LocalClient)

        stack = contextlib.ExitStack()

        # Handle --isolation option:
        if self._git_isolation:
            client = stack.enter_context(client.worktree())

        context["client"] = client
        context["stack"] = stack
        context["click_context"] = ctx

    def _post_hook(self, builder, context, result, *args, **kwargs):
        """Post-hook method."""
        if result.error:
            raise result.error

    def execute(self, *args, **kwargs):
        """Execute the wrapped operation.

        First executes `pre_hooks` in ascending `order`, passing a read/write context between them.
        It then calls the wrapped `operation`. The result of the operation then gets pass to all the `post_hooks`,
        but in descending `order`. It then returns the result or error if there was one.
        """
        if not self.finalized:
            raise errors.CommandNotFinalizedError("Call `build()` before executing a command")

        context = {}
        if any(self.pre_hooks):
            order = sorted(self.pre_hooks.keys())

            for o in order:
                for hook in self.pre_hooks[o]:
                    hook(self, context, *args, **kwargs)

        output = None
        error = None

        try:
            with context["stack"]:
                output = context["click_context"].invoke(self._operation, context["client"], *args, **kwargs)
        except errors.RenkuException as e:
            error = e

        result = CommandResult(output, error, CommandResult.FAILURE if error else CommandResult.SUCCESS)

        if any(self.post_hooks):
            order = sorted(self.post_hooks.keys(), reverse=True)

            for o in order:
                for hook in self.post_hooks[o]:
                    hook(self, context, result, *args, **kwargs)

        return result

    @property
    def finalized(self):
        """Whether this builder is still being constructed or has been finalized."""
        if hasattr(self, "_builder"):
            return self._builder.finalized
        return self._finalized

    @check_finalized
    def add_pre_hook(self, order, hook):
        """Add a pre-execution hook.

        :param order: Determines the order of executed hooks, lower numbers get executed first.
        :param hook: The hook to add
        """
        if hasattr(self, "_builder"):
            self._builder.add_pre_hook(order, hook)
        else:
            self.pre_hooks[order].append(hook)

    @check_finalized
    def add_post_hook(self, order, hook):
        """Add a post-execution hook.

        :param order: Determines the order of executed hooks, lower numbers get executed first.
        :param hook: The hook to add
        """
        if hasattr(self, "_builder"):
            self._builder.add_post_hook(order, hook)
        else:
            self.post_hooks[order].append(hook)

    @check_finalized
    def build(self):
        """Build (finalize) the command."""
        if not self._operation:
            raise errors.ConfigurationError("`Command` needs to have a wrapped `command` set")
        self.add_pre_hook(self.CLIENT_HOOK_ORDER, self._pre_hook)
        self.add_post_hook(self.CLIENT_HOOK_ORDER, self._post_hook)

        self._finalized = True

        return self

    @check_finalized
    def command(self, operation):
        """Set the wrapped command.

        :param operation: The function to wrap in the command builder.
        """
        self._operation = operation

        return self

    @check_finalized
    def working_directory(self, directory):
        """Set the working directory for the command.

        :param directory: The working directory to work in.
        """
        self._working_directory = directory

        return self

    @check_finalized
    def track_std_streams(self):
        """Whether to track STD streams or not."""
        self._track_std_streams = True

        return self

    @check_finalized
    def with_git_isolation(self):
        """Whether to run in git isolation or not."""
        self._git_isolation = True

        return self

    @check_finalized
    def with_commit(self, message=None, commit_if_empty=False, raise_if_empty=False, commit_only=None):
        """Create a commit.

        :param message: The commit message. Autogenerated if left empty.
        :param commit_if_empty: Whether to commit if there are no modified files .
        :param raise_if_empty: Whether to raise an exception if there are no modified files.
        :param commit_only: Only commit the supplied paths.
        """
        return Commit(self, message, commit_if_empty, raise_if_empty, commit_only)

    @check_finalized
    def lock_project(self):
        """Acquire a lock for the whole project."""
        return ProjectLock(self)

    @check_finalized
    def lock_dataset(self):
        """Acquire a lock for a dataset."""
        return DatasetLock(self)

    @check_finalized
    def require_migration(self):
        """Check if a migration is needed."""
        return RequireMigration(self)

    @check_finalized
    def require_clean(self):
        """Check that the repository is clean."""
        return RequireClean(self)

    @check_finalized
    def with_communicator(self, communicator):
        """Create a communicator."""
        return Communicator(self, communicator)


class Commit(Command):
    """Builder for commands that create a commit."""

    DEFAULT_ORDER = 3

    def __init__(self, builder, message=None, commit_if_empty=False, raise_if_empty=False, commit_only=None):
        """__init__ of Commit.

        :param message: The commit message. Autogenerated if left empty.
        :param commit_if_empty: Whether to commit if there are no modified files .
        :param raise_if_empty: Whether to raise an exception if there are no modified files.
        :param commit_only: Only commit the supplied paths.
        """
        self._builder = builder
        self._message = message
        self._commit_if_empty = commit_if_empty
        self._raise_if_empty = raise_if_empty
        self._commit_filter_paths = commit_only

    def _pre_hook(self, builder, context, *args, **kwargs):
        """Hook to create a commit transaction."""
        if "client" not in context:
            raise ValueError("Commit builder needs a LocalClient to be set.")
        if "stack" not in context:
            raise ValueError("Commit builder needs a stack to be set.")

        transaction = context["client"].transaction(
            clean=False,
            commit=True,
            commit_empty=self._commit_if_empty,
            commit_message=self._message,
            commit_only=self._commit_filter_paths,
            ignore_std_streams=not builder._track_std_streams,
            raise_if_empty=self._raise_if_empty,
        )
        context["stack"].enter_context(transaction)

    @check_finalized
    def build(self):
        """Build the command."""
        self._builder.add_pre_hook(self.DEFAULT_ORDER, self._pre_hook)

        return self._builder.build()

    @check_finalized
    def with_commit_message(self, message):
        """Set a new commit message."""
        self._message = message

        return self


class RequireMigration(Command):
    """Builder to check for migrations."""

    DEFAULT_ORDER = 2

    def __init__(self, builder):
        """__init__ of RequireMigration."""
        self._builder = builder

    def _pre_hook(self, builder, context, *args, **kwargs):
        """Check if migration is necessary."""
        if "client" not in context:
            raise ValueError("Commit builder needs a LocalClient to be set.")

        check_for_migration(context["client"])

    @check_finalized
    def build(self):
        """Build the command."""
        self._builder.add_pre_hook(self.DEFAULT_ORDER, self._pre_hook)

        return self._builder.build()


class RequireClean(Command):
    """Builder to check if repo is clean."""

    DEFAULT_ORDER = 3

    def __init__(self, builder):
        """__init__ of RequireClean."""
        self._builder = builder

    def _pre_hook(self, builder, context, *args, **kwargs):
        """Check if repo is clean."""
        if "client" not in context:
            raise ValueError("Commit builder needs a LocalClient to be set.")
        context["client"].ensure_clean(ignore_std_streams=not builder._track_std_streams)

    @check_finalized
    def build(self):
        """Build the command."""
        self._builder.add_pre_hook(self.DEFAULT_ORDER, self._pre_hook)

        return self._builder.build()


class ProjectLock(Command):
    """Builder to get a project wide lock."""

    DEFAULT_ORDER = 4

    def __init__(self, builder):
        """__init__ of ProjectLock."""
        self._builder = builder

    def _pre_hook(self, builder, context, *args, **kwargs):
        """Lock the project."""
        if "client" not in context:
            raise ValueError("Commit builder needs a LocalClient to be set.")
        if "stack" not in context:
            raise ValueError("Commit builder needs a stack to be set.")

        context["stack"].enter_context(context["client"].lock)

    @check_finalized
    def build(self):
        """Build the command."""
        self._builder.add_pre_hook(self.DEFAULT_ORDER, self._pre_hook)

        return self._builder.build()


class DatasetLock(Command):
    """Builder to lock on a dataset."""

    DEFAULT_ORDER = 5

    def __init__(self, builder):
        """__init__ of DatasetLock."""
        self._builder = builder

    def _pre_hook(self, builder, context, *args, **kwargs):
        if "client" not in context:
            raise ValueError("Commit builder needs a LocalClient to be set.")
        if "stack" not in context:
            raise ValueError("Commit builder needs a stack to be set.")

        context["stack"].enter_context(context["client"].lock)

    @check_finalized
    def build(self):
        """Build the command."""
        self._builder.add_pre_hook(self.DEFAULT_ORDER, self._pre_hook)

        return self._builder.build()


class Communicator(Command):
    """Hook for logging and interaction with user."""

    DEFAULT_ORDER = 2

    def __init__(self, builder, communicator):
        """__init__ of Communicator.

        :param communicator: Instance of CommunicationCallback.
        """
        self._builder = builder
        self._communicator = communicator

    def _pre_hook(self, builder, context, *args, **kwargs):
        communication.subscribe(self._communicator)

    def _post_hook(self, builder, context, result, *args, **kwargs):
        communication.unsubscribe(self._communicator)

    @check_finalized
    def build(self):
        """Build the command."""
        self._builder.add_pre_hook(self.DEFAULT_ORDER, self._pre_hook)
        self._builder.add_post_hook(self.DEFAULT_ORDER, self._post_hook)

        return self._builder.build()


class CommandResult:
    """The result of a command.

    The return value of the command is set as `.output`, if there was an error, it is set as `.error`, and
    the status of the command is set to either `CommandResult.SUCCESS` or CommandResult.FAILURE`.
    """

    SUCCESS = 0

    FAILURE = 1

    def __init__(self, output, error, status):
        """__init__ of CommandResult."""
        self.output = output
        self.error = error
        self.status = status
