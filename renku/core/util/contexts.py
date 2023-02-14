# -*- coding: utf-8 -*-
#
# Copyright 2018-2022- Swiss Data Science Center (SDSC)
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
"""Implement various context managers."""

import contextlib
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Union

from renku.command.command_builder import inject
from renku.core import errors
from renku.core.interface.database_gateway import IDatabaseGateway
from renku.core.interface.project_gateway import IProjectGateway


@contextlib.contextmanager
def chdir(path):
    """Change the current working directory."""
    cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(cwd)


class redirect_stdin(contextlib.ContextDecorator):
    """Implement missing redirect stdin based on ``contextlib.py``."""

    _stream = "stdin"

    def __init__(self, new_target):
        """Keep the original stream."""
        self._new_target = new_target
        # We use a list of old targets to make this CM re-entrant
        self._old_targets = []

    def __enter__(self):
        """Change the stream value."""
        self._old_targets.append(getattr(sys, self._stream))
        setattr(sys, self._stream, self._new_target)
        return self._new_target

    def __exit__(self, exception_type, exception_value, traceback):
        """Restore the stream value."""
        setattr(sys, self._stream, self._old_targets.pop())


def _wrap_path_or_stream(method, mode):
    """Open path with context or close stream at the end."""

    def decorator(path_or_stream):
        """Open the path if needed."""
        if isinstance(path_or_stream, (str, Path)):
            return method(Path(path_or_stream).open(mode))
        return method(path_or_stream)

    return decorator


class Isolation(contextlib.ExitStack):
    """Isolate execution."""

    CONTEXTS = {
        "stdin": _wrap_path_or_stream(redirect_stdin, "r"),
        "stdout": _wrap_path_or_stream(contextlib.redirect_stdout, "w"),
        "stderr": _wrap_path_or_stream(contextlib.redirect_stderr, "w"),
        "cwd": chdir,
    }

    def __init__(self, **kwargs):
        """Create a context manager."""
        super().__init__()
        for key, value in kwargs.items():
            if value is not None:
                self.enter_context(self.CONTEXTS[key](value))


@contextlib.contextmanager
def measure(message="TOTAL"):
    """Measure execution time of enclosing code block."""
    import time

    start = time.time()
    try:
        yield
    finally:
        end = time.time()
        total_seconds = float("%.2f" % (end - start))
        print(f"{message}: {total_seconds} seconds")


@contextlib.contextmanager
def renku_project_context(path, check_git_path=True):
    """Provide a project context with repo path injected."""
    from renku.core.util.git import get_git_path
    from renku.domain_model.project_context import project_context

    if check_git_path:
        path = get_git_path(path)

    with project_context.with_path(path=path):
        project_context.external_storage_requested = True
        yield project_context.path


@contextlib.contextmanager
@inject.autoparams("project_gateway", "database_gateway")
def with_project_metadata(
    project_gateway: IProjectGateway,
    database_gateway: IDatabaseGateway,
    read_only: bool = False,
    name: Optional[str] = None,
    namespace: Optional[str] = None,
    description: Optional[str] = None,
    keywords: Optional[List[str]] = None,
    custom_metadata: Optional[Dict] = None,
):
    """Yield an editable metadata object.

    Args:
        project_gateway(IProjectGateway): Injected project gateway.
        database_gateway(IDatabaseGateway): Injected database gateway.
        read_only(bool): Whether to save changes or not (Default value = False).
        name(Optional[str]): Name of the project (when creating a new one) (Default value = None).
        namespace(Optional[str]): Namespace of the project (when creating a new one) (Default value = None).
        description(Optional[str]): Project description (when creating a new one) (Default value = None).
        keywords(Optional[List[str]]): Keywords for the project (when creating a new one) (Default value = None).
        custom_metadata(Optional[Dict]): Custom JSON-LD metadata (when creating a new project)
            (Default value = None).
    """
    from renku.domain_model.project import Project
    from renku.domain_model.project_context import project_context

    try:
        project = project_gateway.get_project()
    except ValueError:
        project = Project.from_project_context(
            project_context=project_context,
            name=name,
            namespace=namespace,
            description=description,
            keywords=keywords,
            custom_metadata=custom_metadata,
        )

    yield project

    if not read_only:
        project_gateway.update_project(project)
        database_gateway.commit()


@contextlib.contextmanager
def Lock(filename: Union[Path, str], timeout: int = 0, mode: str = "shared", blocking: bool = False):
    """A file-based lock context manager."""
    import portalocker

    if mode == "shared":
        flags = portalocker.LOCK_SH
    elif mode == "exclusive":
        flags = portalocker.LOCK_EX
    else:
        raise errors.ParameterError(f"Mode can be 'shared' or 'exclusive' not '{mode}'")

    if not blocking:
        flags |= portalocker.LOCK_NB

    try:
        with portalocker.Lock(filename, timeout=timeout, flags=flags):
            yield
    except (portalocker.LockException, portalocker.AlreadyLocked) as e:
        raise errors.LockError(f"Cannot lock {e.__class__.__name__}")


@contextlib.contextmanager
def wait_for(delay: float):
    """Make sure that at least ``delay`` seconds are passed during the execution of the wrapped code block."""
    start = time.time()

    yield

    exec_time = time.time() - start
    if exec_time < delay:
        time.sleep(delay - exec_time)
