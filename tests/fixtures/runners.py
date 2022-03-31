# -*- coding: utf-8 -*-
#
# Copyright 2021 Swiss Data Science Center (SDSC)
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
"""Renku common configurations."""

import contextlib
import subprocess
import sys
import time
from pathlib import Path
from typing import IO, Any, Mapping, Optional, Sequence, Union, cast

import click
import pytest
from click.testing import CliRunner, Result


class OutputStreamProxy:
    """A proxy class to allow reading from stdout/stderr objects."""

    def __init__(self, stream):
        self._stream = stream
        self._buffer: bytes = b""

    def __getattr__(self, name):
        return getattr(self._stream, name)

    def __setattr__(self, name, value):
        if name == "_stream":
            super().__setattr__(name, value)
        else:
            setattr(self._stream, name, value)

    def write(self, value: str):
        """Write to the output stream."""
        # NOTE: Disabled the write if stream is a TTY to avoid cluttering the screen during tests.
        if not self._stream.isatty():
            self._stream.write(value)

        byte_value = value.encode("utf-8")
        self._buffer += byte_value

        return len(byte_value)

    def getvalue(self) -> bytes:
        """Return everything that has been written to the stream."""
        return self._buffer


class RenkuRunner(CliRunner):
    """Custom CliRunner that allows passing stdout and stderr to the ``invoke`` method."""

    @contextlib.contextmanager
    def isolation(self, input=None, env=None, color: bool = False):
        """See ``click.testing.CliRunner::isolation``."""
        # Preserve original stdout and stderr
        stdout = OutputStreamProxy(sys.stdout)  # type: ignore
        stderr = stdout if self.mix_stderr else OutputStreamProxy(sys.stderr)  # type: ignore

        # NOTE: CliRunner.isolation replaces original stdout and stderr with BytesIO so that it can read program
        # outputs from them. This causes Renku CLI to create custom terminal (since stdout and stderr are not tty)
        # and therefore, tests fail because nothing is printed to the outputs. We use a proxy around the original
        # stderr and stdout so that we can read from them without a need for BytesIO objects.
        with super().isolation(input=input, env=env, color=color):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):  # type: ignore
                yield stdout, stderr

    def invoke(  # type: ignore
        self,
        cli: click.BaseCommand,
        args: Optional[Union[str, Sequence[Union[Path, str]]]] = None,
        input: Optional[Union[str, bytes, IO]] = None,
        env: Optional[Mapping[str, Optional[str]]] = None,
        catch_exceptions: bool = True,
        color: bool = False,
        stdin: Optional[Union[str, Path, IO]] = None,
        stdout: Optional[Union[str, Path, IO]] = None,
        stderr: Optional[Union[str, Path, IO]] = None,
        **extra: Any,
    ) -> Result:  # type: ignore
        """See ``click.testing.CliRunner::invoke``."""
        from renku.core.utils.contexts import Isolation
        from renku.core.utils.util import to_string

        assert not input or not stdin, "Cannot set both ``stdin`` and ``input``"

        if isinstance(args, Path):
            args = str(args)
        elif args is not None and not isinstance(args, str):
            args = [to_string(a) for a in args]

        if isinstance(stdin, Path):
            stdin = str(stdin)

        with Isolation(stdout=stdout, stderr=stderr):
            return super().invoke(
                cli=cli,
                args=cast(Optional[Union[str, Sequence[str]]], args),
                input=stdin or input,
                env=env,
                catch_exceptions=catch_exceptions,
                color=color,
                **extra,
            )


@pytest.fixture()
def run_shell():
    """Create a shell cmd runner."""

    def run_(cmd, return_ps=None, sleep_for=None):
        """Spawn subprocess and execute shell command.

        Args:
            cmd(str): The command to run.
            return_ps: Return process object.
            sleep_for: After executing command sleep for n seconds.
        Returns:
            Process object or tuple (stdout, stderr).
        """
        ps = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        if return_ps:
            return ps

        output = ps.communicate()

        if sleep_for:
            time.sleep(sleep_for)

        return output

    return run_


@pytest.fixture()
def runner():
    """Create a runner on isolated filesystem."""
    return RenkuRunner()


@pytest.fixture()
def split_runner():
    """A RenkuRunner with split stdout and stderr streams."""
    return RenkuRunner(mix_stderr=False)


@pytest.fixture()
def run(runner, capsys):
    """Return a callable runner."""
    from renku.cli import cli
    from renku.core.utils.contexts import Isolation

    def generate(args=("update", "--all"), cwd=None, **streams):
        """Generate an output."""
        with capsys.disabled(), Isolation(cwd=cwd, **streams):
            try:
                cli.main(args=args, prog_name=runner.get_default_prog_name(cli))
                return 0
            except SystemExit as e:
                return 0 if e.code is None else e.code
            except Exception:
                raise

    return generate


@pytest.fixture()
def isolated_runner():
    """Create a runner on isolated filesystem."""
    runner_ = RenkuRunner()
    with runner_.isolated_filesystem():
        yield runner_
