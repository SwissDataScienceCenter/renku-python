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
from pathlib import Path
from typing import Union

import click

from renku.core import errors


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

    def __exit__(self, exctype, excinst, exctb):
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


def click_context(path, command):
    """Provide a click context with repo path injected."""
    from renku.core.constant import RENKU_HOME
    from renku.core.management.client import LocalClient
    from renku.core.util.git import default_path

    return click.Context(
        click.Command(command),
        obj=LocalClient(path=default_path(path), renku_home=RENKU_HOME, external_storage_requested=True),
    ).scope()


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
