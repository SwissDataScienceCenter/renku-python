# -*- coding: utf-8 -*-
#
# Copyright 2018-2022 - Swiss Data Science Center (SDSC)
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
"""Command builder for repository."""

from typing import Optional

from renku.command.command_builder.command import Command, CommandResult, check_finalized
from renku.core import errors


class Commit(Command):
    """Builder for commands that create a commit."""

    DEFAULT_ORDER = 4

    def __init__(
        self,
        builder: Command,
        message: Optional[str] = None,
        commit_if_empty: Optional[bool] = False,
        raise_if_empty: Optional[bool] = False,
        commit_only: Optional[bool] = None,
        skip_staging: bool = False,
    ) -> None:
        """__init__ of Commit.

        Args:
            message (str): The commit message. Auto-generated if left empty (Default value = None).
            commit_if_empty (bool): Whether to commit if there are no modified files (Default value = None).
            raise_if_empty (bool): Whether to raise an exception if there are no modified files (Default value = None).
            commit_only (bool): Only commit the supplied paths (Default value = None).
        """
        self._builder = builder
        self._message = message
        self._commit_if_empty = commit_if_empty
        self._raise_if_empty = raise_if_empty
        self._commit_filter_paths = commit_only
        self._skip_staging: bool = skip_staging

    def _pre_hook(self, builder: Command, context: dict, *args, **kwargs) -> None:
        """Hook to create a commit transaction.

        Args:
            builder(Command): The current ``CommandBuilder``.
            context(dict): The current context object.
        """
        if "client_dispatcher" not in context:
            raise ValueError("Commit builder needs a IClientDispatcher to be set.")
        if "stack" not in context:
            raise ValueError("Commit builder needs a stack to be set.")

        from renku.core.management.git import prepare_commit

        self.diff_before = prepare_commit(
            context["client_dispatcher"].current_client,
            commit_only=self._commit_filter_paths,
            skip_staging=self._skip_staging,
        )

    def _post_hook(self, builder: Command, context: dict, result: CommandResult, *args, **kwargs):
        """Hook that commits changes.

        Args:
            builder(Command):The current ``CommandBuilder``.
            context(dict): The current context object.
            result(CommandResult): The result of the command execution.
        """
        from renku.core.management.git import finalize_commit

        if result.error:
            # TODO: Cleanup repo
            return

        try:
            finalize_commit(
                context["client_dispatcher"].current_client,
                self.diff_before,
                commit_only=self._commit_filter_paths,
                commit_empty=self._commit_if_empty,
                raise_if_empty=self._raise_if_empty,
                commit_message=self._message,
                skip_staging=self._skip_staging,
            )
        except errors.RenkuException as e:
            result.error = e

    @check_finalized
    def build(self) -> Command:
        """Build the command.

        Returns:
            Command: Finalized version of this command.
        """
        self._builder.add_pre_hook(self.DEFAULT_ORDER, self._pre_hook)
        self._builder.add_post_hook(self.DEFAULT_ORDER, self._post_hook)

        return self._builder.build()

    @check_finalized
    def with_commit_message(self, message: str) -> Command:
        """Set a new commit message.

        Args:
            message(str): Commit message to set.

        Returns:
            Command: This command with commit message applied.
        """
        self._message = message

        return self


class RequireClean(Command):
    """Builder to check if repo is clean."""

    DEFAULT_ORDER = 4

    def __init__(self, builder: Command) -> None:
        """__init__ of RequireClean."""
        self._builder = builder

    def _pre_hook(self, builder: Command, context: dict, *args, **kwargs) -> None:
        """Check if repo is clean.

        Args:
            builder(Command): Current ``CommandBuilder``.
            context(dict): Current context.
        """
        if "client_dispatcher" not in context:
            raise ValueError("Commit builder needs a IClientDispatcher to be set.")

        context["client_dispatcher"].current_client.ensure_clean(ignore_std_streams=not builder._track_std_streams)

    @check_finalized
    def build(self) -> Command:
        """Build the command.

        Returns:
            Command: Finalized version of this command.
        """
        self._builder.add_pre_hook(self.DEFAULT_ORDER, self._pre_hook)

        return self._builder.build()


class Isolation(Command):
    """Builder to run a command in git isolation."""

    DEFAULT_ORDER = 3

    def __init__(
        self,
        builder: Command,
    ) -> None:
        """__init__ of Commit."""
        self._builder = builder

    def _injection_pre_hook(self, builder: Command, context: dict, *args, **kwargs) -> None:
        """Hook to setup dependency injection for commit transaction.

        Args:
            builder(Command): Current ``CommandBuilder``.
            context(dict): Current context.
        """
        if "client_dispatcher" not in context:
            raise ValueError("Commit builder needs a IClientDispatcher to be set.")
        from renku.core.management.git import prepare_worktree

        self.original_client = context["client_dispatcher"].current_client

        self.new_client, self.isolation, self.path, self.branch_name = prepare_worktree(
            context["client_dispatcher"].current_client, path=None, branch_name=None, commit=None
        )

        context["client_dispatcher"].push_created_client_to_stack(self.new_client)

    def _post_hook(self, builder: Command, context: dict, result: CommandResult, *args, **kwargs):
        """Hook that commits changes.

        Args:
            builder(Command): Current ``CommandBuilder``.
            context(dict): Current context.
        """
        from renku.core.management.git import finalize_worktree

        context["client_dispatcher"].pop_client()

        try:
            finalize_worktree(
                self.original_client,
                self.isolation,
                self.path,
                self.branch_name,
                delete=True,
                new_branch=True,
                exception=result.error,
            )
        except errors.RenkuException as e:
            if not result.error:
                result.error = e

    @check_finalized
    def build(self) -> Command:
        """Build the command.

        Returns:
            Command: Finalized version of this command.
        """
        self._builder.add_injection_pre_hook(self.DEFAULT_ORDER, self._injection_pre_hook)
        self._builder.add_post_hook(self.DEFAULT_ORDER, self._post_hook)

        return self._builder.build()
