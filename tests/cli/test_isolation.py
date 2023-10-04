# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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

import subprocess
import sys
import textwrap
import time

from renku.core.util.contexts import Lock
from tests.utils import write_and_commit_file


def test_run_in_isolation(runner, project, run, subdirectory):
    """Test run in isolation."""
    write_and_commit_file(project.repository, path=project.path / ".gitignore", content="\nlock")

    prefix = ["run", "--no-output"]
    cmd = ["python", "-S", "-c", 'import os, sys; sys.exit(1 if os.path.exists("lock") else 0)']

    head = project.repository.head.commit.hexsha

    with Lock("lock"):
        assert 1 == run(args=prefix + cmd)
        assert project.repository.head.commit.hexsha == head

        assert 0 == run(prefix + ["--isolation"] + cmd)
        assert project.repository.head.commit.hexsha != head


def test_file_modification_during_run(tmp_path, runner, project, subdirectory, no_lfs_size_limit, no_lfs_warning):
    """Test run in isolation."""
    script = project.path / "script.py"
    output = project.path / "output"
    lock_file = tmp_path / "lock"

    write_and_commit_file(
        project.repository,
        script,
        textwrap.dedent(
            f"""
            import os, time, sys
            open("{lock_file}", "a")
            while os.path.exists("{lock_file}"):
                time.sleep(0.1)
            sys.stdout.write(sys.stdin.read())
            sys.stdout.flush()
            """
        ),
    )

    with output.open("wb") as stdout:
        command = [sys.executable, "-m", "renku.ui.cli", "run", "--isolation", "--", "python", script]
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=stdout)

        while not lock_file.exists() and process.poll() is None:
            time.sleep(0.1)

        assert process.poll() is None, "Subprocess exited prematurely"

        # NOTE: Modify ``script.py`` in the current worktree
        script.write_text("print('edited')")

        # NOTE: Signal the isolated run to continue
        lock_file.unlink()

        process.communicate(input=b"test")
        assert 0 == process.wait()

    # NOTE: ``script.py`` is modified in the current worktree
    assert {"script.py"} == {c.a_path for c in project.repository.unstaged_changes}

    # NOTE: Isolated run finished with the expected result
    assert "test" == output.read_text().strip()
    # NOTE: Isolated run committed its results
    committed_changed_files_in_run = {c.a_path for c in project.repository.head.commit.get_changes()}
    assert "output" in committed_changed_files_in_run
    assert "script.py" not in committed_changed_files_in_run
