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
"""Command builder."""

import contextlib
import functools
import threading
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Type, Union

import inject

from renku.core import errors
from renku.core.util.communication import CommunicationCallback
from renku.core.util.git import get_git_path
from renku.domain_model.project_context import project_context

_LOCAL = threading.local()


def check_finalized(f):
    """Decorator to prevent modification of finalized builders.

    Args:
        f: Decorated function.

    Returns:
        Wrapped function.
    """

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        """Decorator to prevent modification of finalized builders."""
        if not args or not isinstance(args[0], Command):
            raise errors.ParameterError("Command hooks need to be `Command` object methods.")

        if args[0].finalized:
            raise errors.CommandFinalizedError("Cannot modify a finalized `Command`.")

        return f(*args, **kwargs)

    return wrapper


def _patched_get_injector_or_die() -> inject.Injector:
    """Patched version of get_injector_or_die with thread local injectors.

    Allows deferred definition of an injector per thread.
    """
    injector = getattr(_LOCAL, "injector", None)
    if not injector:
        raise inject.InjectorException("No injector is configured")

    return injector


def _patched_configure(config: Optional[inject.BinderCallable] = None, bind_in_runtime: bool = True) -> inject.Injector:
    """Create an injector with a callable config or raise an exception when already configured.

    Args:
        config(Optional[inject.BinderCallable], optional): Injection binding config (Default value = None).
        bind_in_runtime(bool, optional): Whether to allow binding at runtime (Default value = True).

    Returns:
        Injector: Thread-safe injector with bindings applied.
    """

    if getattr(_LOCAL, "injector", None):
        raise inject.InjectorException("Injector is already configured")

    _LOCAL.injector = inject.Injector(config, bind_in_runtime=bind_in_runtime)

    return _LOCAL.injector


inject.configure = _patched_configure
inject.get_injector_or_die = _patched_get_injector_or_die


def remove_injector():
    """Remove a thread-local injector."""
    if getattr(_LOCAL, "injector", None):
        del _LOCAL.injector


@contextlib.contextmanager
def replace_injection(bindings: Dict, constructor_bindings=None):
    """Temporarily inject various test objects.

    Args:
        bindings: New normal injection bindings to apply.
        constructor_bindings: New constructor bindings to apply (Default value = None).
    """
    constructor_bindings = constructor_bindings or {}

    def bind(binder):
        for key, value in bindings.items():
            binder.bind(key, value)
        for key, value in constructor_bindings.items():
            binder.bind_to_constructor(key, value)

    old_injector = getattr(_LOCAL, "injector", None)
    try:
        if old_injector:
            remove_injector()
        inject.configure(bind, bind_in_runtime=False)

        yield
    finally:
        remove_injector()

        if old_injector:
            _LOCAL.injector = old_injector


class Command:
    """Base renku command builder."""

    HOOK_ORDER = 1

    def __init__(self) -> None:
        """__init__ of Command."""
        self.injection_pre_hooks: Dict[int, List[Callable]] = defaultdict(list)
        self.pre_hooks: Dict[int, List[Callable]] = defaultdict(list)
        self.post_hooks: Dict[int, List[Callable]] = defaultdict(list)
        self._operation: Optional[Callable] = None
        self._finalized: bool = False
        self._track_std_streams: bool = False
        self._working_directory: Optional[str] = None
        self._context_added: bool = False

    def __getattr__(self, name: str) -> Any:
        """Bubble up attributes of wrapped builders."""
        if "_builder" in self.__dict__:
            return getattr(self._builder, name)

        raise AttributeError(f"{self.__class__.__name__} object has no attribute {name}")

    def __setattr__(self, name: str, value: Any) -> None:
        """Set attributes of wrapped builders."""
        if hasattr(self, "_builder") and self.__class__ is not self._builder.__class__:
            self._builder.__setattr__(name, value)

        object.__setattr__(self, name, value)

    def _injection_pre_hook(self, builder: "Command", context: dict, *args, **kwargs) -> None:
        """Setup dependency injections.

        Args:
            builder("Command"): Current ``CommandBuilder``.
            context(dict): Current context dictionary.
        """
        if not project_context.has_context():
            path = get_git_path(self._working_directory or ".")
            project_context.push_path(path)
            self._context_added = True

        context["bindings"] = {}
        context["constructor_bindings"] = {}

    def _pre_hook(self, builder: "Command", context: dict, *args, **kwargs) -> None:
        """Setup project.

        Args:
            builder("Command"): Current ``CommandBuilder``.
            context(dict): Current context dictionary.
        """

        stack = contextlib.ExitStack()
        context["stack"] = stack

    def _post_hook(self, builder: "Command", context: dict, result: "CommandResult", *args, **kwargs) -> None:
        """Post-hook method.

        Args:
            builder("Command"): Current ``CommandBuilder``.
            context(dict): Current context dictionary.
            result("CommandResult"): Result of command execution.
        """
        remove_injector()

        if self._context_added:
            project_context.pop_context()

        if result.error:
            raise result.error

    def execute(self, *args, **kwargs) -> "CommandResult":
        """Execute the wrapped operation.

        First executes `pre_hooks` in ascending `order`, passing a read/write context between them.
        It then calls the wrapped `operation`. The result of the operation then gets pass to all the `post_hooks`,
        but in descending `order`. It then returns the result or error if there was one.

        Returns:
            CommandResult: Result of execution of command.
        """
        if not self.finalized:
            raise errors.CommandNotFinalizedError("Call `build()` before executing a command")

        context: Dict[str, Any] = {}
        if any(self.injection_pre_hooks):
            order = sorted(self.injection_pre_hooks.keys())

            for o in order:
                for hook in self.injection_pre_hooks[o]:
                    hook(self, context, *args, **kwargs)

        def _bind(binder):
            for key, value in context["bindings"].items():
                binder.bind(key, value)
            for key, value in context["constructor_bindings"].items():
                binder.bind_to_constructor(key, value)

            return binder

        inject.configure(_bind, bind_in_runtime=False)

        if any(self.pre_hooks):
            order = sorted(self.pre_hooks.keys())

            for o in order:
                for hook in self.pre_hooks[o]:
                    try:
                        hook(self, context, *args, **kwargs)
                    except (Exception, BaseException):
                        # don't leak injections from failed hook
                        remove_injector()
                        raise

        output = None
        error = None

        try:
            with context["stack"]:
                output = self._operation(*args, **kwargs)  # type: ignore
        except errors.RenkuException as e:
            error = e
        except (Exception, BaseException):
            remove_injector()
            raise

        result = CommandResult(output, error, CommandResult.FAILURE if error else CommandResult.SUCCESS)

        if any(self.post_hooks):
            order = sorted(self.post_hooks.keys(), reverse=True)

            for o in order:
                for hook in self.post_hooks[o]:
                    hook(self, context, result, *args, **kwargs)

        return result

    @property
    def finalized(self) -> bool:
        """Whether this builder is still being constructed or has been finalized."""
        if hasattr(self, "_builder"):
            return self._builder.finalized
        return self._finalized

    def any_builder_is_instance_of(self, cls: Type) -> bool:
        """Check if any 'chained' command builder is an instance of a specific command builder class."""
        if isinstance(self, cls):
            return True
        elif "_builder" in self.__dict__:
            return self._builder.any_builder_is_instance_of(cls)
        else:
            return False

    @property
    def will_write_to_database(self) -> bool:
        """Will running the command write anything to the metadata store."""
        try:
            return self._write
        except AttributeError:
            return False

    @check_finalized
    def add_injection_pre_hook(self, order: int, hook: Callable):
        """Add a pre-execution hook for dependency injection.

        Args:
            order(int): Determines the order of executed hooks, lower numbers get executed first.
            hook(Callable): The hook to add.
        """
        if hasattr(self, "_builder"):
            self._builder.add_injection_pre_hook(order, hook)
        else:
            self.injection_pre_hooks[order].append(hook)

    @check_finalized
    def add_pre_hook(self, order: int, hook: Callable):
        """Add a pre-execution hook.

        Args:
            order(int): Determines the order of executed hooks, lower numbers get executed first.
            hook(Callable): The hook to add.
        """
        if hasattr(self, "_builder"):
            self._builder.add_pre_hook(order, hook)
        else:
            self.pre_hooks[order].append(hook)

    @check_finalized
    def add_post_hook(self, order: int, hook: Callable):
        """Add a post-execution hook.

        Args:
            order(int): Determines the order of executed hooks, higher numbers get executed first.
            hook(Callable): The hook to add.
        """
        if hasattr(self, "_builder"):
            self._builder.add_post_hook(order, hook)
        else:
            self.post_hooks[order].append(hook)

    @check_finalized
    def build(self) -> "Command":
        """Build (finalize) the command.

        Returns:
            Command: Finalized command that cannot be modified.
        """
        if not self._operation:
            raise errors.ConfigurationError("`Command` needs to have a wrapped `command` set")
        self.add_injection_pre_hook(self.HOOK_ORDER, self._injection_pre_hook)
        self.add_pre_hook(self.HOOK_ORDER, self._pre_hook)
        self.add_post_hook(self.HOOK_ORDER, self._post_hook)

        self._finalized = True

        return self

    @check_finalized
    def command(self, operation: Callable):
        """Set the wrapped command.

        Args:
            operation(Callable): The function to wrap in the command builder.

        Returns:
            Command: This command.
        """

        self._operation = operation

        return self

    @check_finalized
    def working_directory(self, directory: str) -> "Command":
        """Set the working directory for the command.

        WARNING: Should not be used in the core service.

        Args:
            directory(str): The working directory to work in.

        Returns:
            Command: This command.
        """
        self._working_directory = directory

        return self

    @check_finalized
    def track_std_streams(self) -> "Command":
        """Whether to track STD streams or not.

        Returns:
            Command: This command.
        """
        self._track_std_streams = True

        return self

    @check_finalized
    def with_git_isolation(self) -> "Command":
        """Whether to run in git isolation or not."""
        from renku.command.command_builder.repo import Isolation

        return Isolation(self)

    @check_finalized
    def with_commit(
        self,
        message: Optional[str] = None,
        commit_if_empty: bool = False,
        raise_if_empty: bool = False,
        commit_only: Optional[Union[str, List[Union[str, Path]]]] = None,
        skip_staging: bool = False,
        skip_dirty_checks: bool = False,
    ) -> "Command":
        """Create a commit.

        Args:
            message(str, optional): The commit message. Auto-generated if left empty (Default value = None).
            commit_if_empty(bool, optional): Whether to commit if there are no modified files (Default value = False).
            raise_if_empty(bool, optional): Whether to raise an exception if there are no modified files
                (Default value = False).
            commit_only(bool, optional): Only commit the supplied paths (Default value = None).
            skip_staging(bool): Don't commit staged files.
            skip_dirty_checks(bool): Don't check if paths are dirty or staged.
        """
        from renku.command.command_builder.repo import Commit

        return Commit(
            self,
            message=message,
            commit_if_empty=commit_if_empty,
            raise_if_empty=raise_if_empty,
            commit_only=commit_only,
            skip_staging=skip_staging,
            skip_dirty_checks=skip_dirty_checks,
        )

    @check_finalized
    def lock_project(self) -> "Command":
        """Acquire a lock for the whole project."""
        from renku.command.command_builder.lock import ProjectLock

        return ProjectLock(self)

    @check_finalized
    def lock_dataset(self) -> "Command":
        """Acquire a lock for a dataset."""
        from renku.command.command_builder.lock import DatasetLock

        return DatasetLock(self)

    @check_finalized
    def require_migration(self) -> "Command":
        """Check if a migration is needed."""
        from renku.command.command_builder.migration import RequireMigration

        return RequireMigration(self)

    @check_finalized
    def require_clean(self) -> "Command":
        """Check that the repository is clean."""
        from renku.command.command_builder.repo import RequireClean

        return RequireClean(self)

    @check_finalized
    def with_communicator(self, communicator: CommunicationCallback) -> "Command":
        """Create a communicator.

        Args:
            communicator(CommunicationCallback): Communicator to use for writing to user.
        """
        from renku.command.command_builder.communication import Communicator

        return Communicator(self, communicator)

    @check_finalized
    def with_database(self, write: bool = False, path: str = None, create: bool = False) -> "Command":
        """Provide an object database connection.

        Args:
            write(bool, optional): Whether or not to persist changes to the database (Default value = False).
            path(str, optional): Location of the database (Default value = None).
            create(bool, optional): Whether the database should be created if it doesn't exist (Default value = False).
        """
        from renku.command.command_builder.database import DatabaseCommand

        return DatabaseCommand(self, write, path, create)


class CommandResult:
    """The result of a command.

    The return value of the command is set as `.output`, if there was an error, it is set as `.error`, and
    the status of the command is set to either `CommandResult.SUCCESS` or CommandResult.FAILURE`.
    """

    SUCCESS = 0

    FAILURE = 1

    def __init__(self, output, error, status) -> None:
        """__init__ of CommandResult."""
        self.output = output
        self.error = error
        self.status = status
