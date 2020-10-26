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
from collections import defaultdict

import click

from renku.core import errors
from renku.core.commands.git import get_git_isolation
from renku.core.management import LocalClient
from renku.core.management.config import RENKU_HOME
from renku.core.management.migrate import check_for_migration
from renku.core.management.repository import default_path


def check_finalized(f):
    """Decorator to prevent modification of finalized builders."""

    def wrapper(*args, **kwargs):
        """Check finalized status."""
        if args[0].finalized:
            raise errors.CommandFinalizedError("Cannot modify a finalized command.")

        return f(*args, **kwargs)

    return wrapper


class Command(object):
    """Base renku command builder."""

    CLIENT_HOOK_PRIORITY = 5

    def __init__(self):
        """__init__ of Command."""
        self.pre_hooks = defaultdict(list)
        self.post_hooks = defaultdict(list)
        self._operation = None
        self._finalized = False
        self._track_std_streams = True

    def __getattr__(self, name):
        """Bubble up attributes of wrapped builders."""
        if "_builder" in self.__dict__:
            return getattr(self._builder, name)

        raise AttributeError(f"{self.__class__.__name__} object has no attribute {name}")

    def _pre_hook(self, builder, context):
        """Setup local client."""
        ctx = click.get_current_context(silent=True)
        if ctx is None:
            client = LocalClient(path=default_path(), renku_home=RENKU_HOME, external_storage_requested=True,)
            ctx = click.Context(click.Command(builder._operation))
        else:
            client = ctx.ensure_object(LocalClient)

        stack = contextlib.ExitStack()

        # Handle --isolation option:
        if get_git_isolation():
            client = stack.enter_context(client.worktree())

        context["client"] = client
        context["stack"] = stack
        context["click_context"] = ctx

    def execute(self, *args, **kwargs):
        """Execute the wrapped operation."""
        if not self.finalized:
            raise errors.CommandNotFinalizedError("Call `build()` before executing a command")

        context = {}
        if any(self.pre_hooks):
            priorities = sorted(self.pre_hooks.keys(), reverse=True)

            for p in priorities:
                for hook in self.pre_hooks[p]:
                    hook(self, context)

        output = None
        error = None
        try:
            with context["stack"]:
                output = context["click_context"].invoke(self._operation, context["client"], *args, **kwargs)
        except errors.RenkuException as e:
            error = e

        result = CommandResult(output, error, CommandResult.FAILURE if error else CommandResult.SUCCESS)

        if any(self.post_hooks):
            priorities = sorted(self.post_hooks.keys(), reverse=True)

            for p in priorities:
                for hook in self.post_hooks[p]:
                    hook(self, context, result)

        return result

    @property
    def finalized(self):
        """Whether this builder is still being constructed or has been finalized."""
        if hasattr(self, "_builder"):
            return self._builder.finalized
        return self._finalized

    @check_finalized
    def add_pre_hook(self, priority, hook):
        """Add a pre-execution hook."""
        if hasattr(self, "_builder"):
            self._builder.add_pre_hook(priority, hook)
        else:
            self.pre_hooks[priority].append(hook)

    @check_finalized
    def add_post_hook(self, priority, hook):
        """Add a post-execution hook."""
        if hasattr(self, "_builder"):
            self._builder.add_post_hook(priority, hook)
        else:
            self.post_hooks[priority].append(hook)

    @check_finalized
    def build(self):
        """Build (finalize) the command."""
        assert self._operation is not None
        self.add_pre_hook(self.CLIENT_HOOK_PRIORITY, self._pre_hook)

        self._finalized = True

        return self

    @check_finalized
    def command(self, operation):
        """Set the wrapped command."""
        self._operation = operation

        return self

    @check_finalized
    def track_std_streams(self):
        """Whether to track STD streams or not."""
        self._track_std_streams = True

        return self

    @check_finalized
    def with_commit(self):
        """Create a commit."""
        return Commit(self)

    @check_finalized
    def lock_project(self):
        """Aquire a lock for the whole project."""
        return ProjectLock(self)

    @check_finalized
    def lock_dataset(self):
        """Aquire a lock for a dataset."""
        return DatasetLock(self)

    @check_finalized
    def require_migration(self):
        """Check if a migration is needed."""
        return RequireMigration(self)

    @check_finalized
    def require_clean(self):
        """Check that the repository is clean."""
        return RequireClean(self)


class Commit(Command):
    """Builder for commands that create a commit."""

    DEFAULT_PRIORITY = 4

    def __init__(self, builder):
        """__init__ of Commit."""
        self._builder = builder
        self._message = None
        self._commit_empty = False
        self._raise_empty = False

    @check_finalized
    def commit_message(self, message):
        """Set the commit message."""
        self._message = message

        return self

    @check_finalized
    def commit_only(self, *paths):
        """Set which paths to commit."""
        self._commit_filter_paths = paths

        return self

    @check_finalized
    def commit_if_empty(self):
        """Whether to create an empty commit."""
        self._commit_empty = True

        return self

    @check_finalized
    def raise_if_empty(self):
        """Whether to raise an exception if no files changed."""
        self._raise_empty = True

        return self

    def _pre_hook(self, builder, context):
        """Hook to create a commit transaction."""
        if "client" not in context:
            raise ValueError("Commit builder needs a LocalClient to be set.")
        if "stack" not in context:
            raise ValueError("Commit builder needs a stack to be set.")

        transaction = context["client"].transaction(
            clean=False,
            commit=True,
            commit_empty=self._commit_empty,
            commit_message=self._message,
            commit_only=self._commit_filter_paths,
            ignore_std_streams=not builder._track_std_streams,
            raise_if_empty=self._raise_empty,
        )
        context["stack"].enter_context(transaction)

    @check_finalized
    def build(self):
        """Build the command."""
        self._builder.add_pre_hook(self.DEFAULT_PRIORITY, self._pre_hook)

        return self._builder.build()


class RequireMigration(Command):
    """Builder to check for migrations."""

    DEFAULT_PRIORITY = 4

    def __init__(self, builder):
        """__init__ of RequireMigration."""
        self._builder = builder

    def _pre_hook(self, builder, context):
        """Check if migration is necessary."""
        if "client" not in context:
            raise ValueError("Commit builder needs a LocalClient to be set.")

        check_for_migration(context["client"])

    @check_finalized
    def build(self):
        """Build the command."""
        self._builder.add_pre_hook(self.DEFAULT_PRIORITY, self._pre_hook)

        return self._builder.build()


class RequireClean(Command):
    """Builder to check if repo is clean."""

    DEFAULT_PRIORITY = 4

    def __init__(self, builder):
        """__init__ of RequireClean."""
        self._builder = builder

    def _pre_hook(self, builder, context):
        """Check if repo is clean."""
        if "client" not in context:
            raise ValueError("Commit builder needs a LocalClient to be set.")
        context["client"].ensure_clean(ignore_std_streams=not builder._track_std_streams)

    @check_finalized
    def build(self):
        """Build the command."""
        self._builder.add_pre_hook(self.DEFAULT_PRIORITY, self._pre_hook)

        return self._builder.build()


class ProjectLock(Command):
    """Builder to get a project wide lock."""

    DEFAULT_PRIORITY = 3

    def __init__(self, builder):
        """__init__ of ProjectLock."""
        self._builder = builder

    def _pre_hook(self, builder, context):
        """Lock the project."""
        if "client" not in context:
            raise ValueError("Commit builder needs a LocalClient to be set.")
        if "stack" not in context:
            raise ValueError("Commit builder needs a stack to be set.")

        context["stack"].enter_context(context["client"].lock)

    @check_finalized
    def build(self):
        """Build the command."""
        self._builder.add_pre_hook(self.DEFAULT_PRIORITY, self._pre_hook)

        return self._builder.build()


class DatasetLock(Command):
    """Builder to lock on a dataset."""

    DEFAULT_PRIORITY = 2

    def __init__(self, builder):
        """__init__ of DatasetLock."""
        self._builder = builder

    def _pre_hook(self, builder, context):
        raise NotImplementedError()

    @check_finalized
    def build(self):
        """Build the command."""
        self._builder.add_pre_hook(self.DEFAULT_PRIORITY, self._pre_hook)

        return self._builder.build()


class CommandResult(object):
    """The result of a command."""

    SUCCESS = 0

    FAILURE = 1

    def __init__(self, output, error, status):
        """__init__ of CommandResult."""
        self.output = output
        self.error = error
        self.status = status
