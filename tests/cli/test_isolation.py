# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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
"""Test execution of commands in parallel."""

import os
import subprocess
import sys
import time
from pathlib import Path


def test_run_in_isolation(runner, project, client, run, subdirectory):
    """Test run in isolation."""
    import filelock

    cwd = Path(project)
    with client.commit():
        with (cwd / ".gitignore").open("a") as f:
            f.write("\nlock")

    prefix = [
        "run",
        "--no-output",
    ]
    cmd = ["python", "-S", "-c", 'import os, sys; sys.exit(1 if os.path.exists("lock") else 0)']

    head = client.repo.head.commit.hexsha

    with filelock.FileLock("lock"):
        assert 1 == run(args=prefix + cmd)
        assert client.repo.head.commit.hexsha == head

        assert 0 == run(prefix + ["--isolation"] + cmd)
        assert client.repo.head.commit.hexsha != head


def test_file_modification_during_run(
    tmpdir, runner, project, client, run, subdirectory, no_lfs_size_limit, no_lfs_warning
):
    """Test run in isolation."""
    script = client.path / "script.py"
    output = client.path / "output"
    lock = Path(str(tmpdir.join("lock")))

    with client.commit():
        script.write_text(
            "import os, time, sys\n"
            'open("{lock}", "a")\n'
            'while os.path.exists("{lock}"):\n'
            "    time.sleep(1)\n"
            "sys.stdout.write(sys.stdin.read())\n"
            "sys.stdout.flush()\n".format(lock=str(lock))
        )

    prefix = [
        sys.executable,
        "-m",
        "renku.cli",
        "run",
        "--isolation",
    ]
    cmd = ["python", os.path.relpath(script, os.getcwd())]

    previous = client.repo.head.commit

    with output.open("wb") as stdout:
        process = subprocess.Popen(prefix + cmd, stdin=subprocess.PIPE, stdout=stdout)

        while not lock.exists() and process.poll() is None:
            time.sleep(1)

        assert process.poll() is None, "Subprocess exited prematurely"

        with script.open("w") as fp:
            fp.write('print("edited")')

        lock.unlink()

        process.communicate(input=b"test")
        assert 0 == process.wait()

    with output.open("r") as fp:
        assert "test" == fp.read().strip()

    diff = previous.diff(client.repo.head.commit)
    modifications = [modification for modification in diff if modification.change_type == "M"]
    assert 0 == len(modifications)
