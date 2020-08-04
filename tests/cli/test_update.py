# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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
"""Test ``update`` command."""

from pathlib import Path

import git

from renku.cli import cli
from renku.core.management.repository import DEFAULT_DATA_DIR as DATA_DIR
from renku.core.models.entities import Collection


def update_and_commit(data, file_, repo):
    """Update source.txt."""
    with file_.open("w") as fp:
        fp.write(data)

    repo.git.add(file_)
    repo.index.commit("Updated source.txt")


def test_update(runner, project, run, no_lfs_warning):
    """Test automatic file update."""
    from renku.core.utils.shacl import validate_graph

    cwd = Path(project)
    data = cwd / DATA_DIR
    data.mkdir(exist_ok=True, parents=True)
    source = cwd / "source.txt"
    output = data / "result.txt"

    repo = git.Repo(project)

    update_and_commit("1", source, repo)

    assert 0 == run(args=("run", "wc", "-c"), stdin=source, stdout=output)

    with output.open("r") as f:
        assert f.read().strip() == "1"

    result = runner.invoke(cli, ["status"])
    assert 0 == result.exit_code

    update_and_commit("12", source, repo)

    result = runner.invoke(cli, ["status"])
    assert 1 == result.exit_code

    assert 0 == run()

    result = runner.invoke(cli, ["status"])
    assert 0 == result.exit_code

    with output.open("r") as f:
        assert f.read().strip() == "2"

    result = runner.invoke(cli, ["log"], catch_exceptions=False)
    assert "(part of" in result.output, result.output

    # Source has been updated but output is unchanged.
    update_and_commit("34", source, repo)

    result = runner.invoke(cli, ["status"])
    assert 1 == result.exit_code

    assert 0 == run()

    result = runner.invoke(cli, ["status"])
    assert 0 == result.exit_code

    with output.open("r") as f:
        assert f.read().strip() == "2"

    from renku.cli.log import FORMATS

    for output_format in FORMATS:
        # Make sure the log contains the original parent.
        result = runner.invoke(cli, ["log", "--format", output_format], catch_exceptions=False,)
        assert 0 == result.exit_code, result.output
        assert source.name in result.output, output_format

        if output_format == "nt":
            r, _, t = validate_graph(result.output, format="nt")
            assert r is True, t


def test_update_multiple_steps(runner, project, run, no_lfs_warning):
    """Test automatic file update."""
    cwd = Path(project)
    data = cwd / "data"
    data.mkdir(exist_ok=True, parents=True)
    source = cwd / "source.txt"
    intermediate = cwd / "intermediate.txt"
    output = cwd / "result.txt"

    repo = git.Repo(project)

    update_and_commit("1", source, repo)

    assert 0 == run(args=("run", "cp", str(source), str(intermediate)))

    with intermediate.open("r") as f:
        assert f.read().strip() == "1"

    result = runner.invoke(cli, ["status"])
    assert 0 == result.exit_code

    assert 0 == run(args=("run", "cp", str(intermediate), str(output)))

    with output.open("r") as f:
        assert f.read().strip() == "1"

    result = runner.invoke(cli, ["status"])
    assert 0 == result.exit_code

    update_and_commit("2", source, repo)

    result = runner.invoke(cli, ["status"])
    assert 1 == result.exit_code

    assert 0 == run()

    result = runner.invoke(cli, ["status"])
    assert 0 == result.exit_code

    with output.open("r") as f:
        assert f.read().strip() == "2"


def test_workflow_without_outputs(runner, project, run):
    """Test workflow without outputs."""
    repo = git.Repo(project)
    cwd = Path(project)
    input_ = cwd / "input.txt"
    with input_.open("w") as f:
        f.write("first")

    repo.git.add("--all")
    repo.index.commit("Created input.txt")

    cmd = ["run", "cat", "--no-output", input_.name]
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code

    cmd = ["status", "--no-output"]
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code

    with input_.open("w") as f:
        f.write("second")

    repo.git.add("--all")
    repo.index.commit("Updated input.txt")

    cmd = ["status", "--no-output"]
    result = runner.invoke(cli, cmd)
    assert 1 == result.exit_code
    assert 0 == run(args=("update", "--no-output"))

    cmd = ["status", "--no-output"]
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code


def test_siblings_update(runner, project, run, no_lfs_warning):
    """Test detection of siblings during update."""
    cwd = Path(project)
    parent = cwd / "parent.txt"
    brother = cwd / "brother.txt"
    sister = cwd / "sister.txt"
    siblings = {brother, sister}

    repo = git.Repo(project)

    def update_source(data):
        """Update parent.txt."""
        with parent.open("w") as fp:
            fp.write(data)

        repo.git.add("--all")
        repo.index.commit("Updated parent.txt")

    update_source("1")

    # The output files do not exist.
    assert not any(sibling.exists() for sibling in siblings)

    cmd = ["run", "tee", "brother.txt"]
    assert 0 == run(args=cmd, stdin=parent, stdout=sister)

    # The output file is copied from the source.
    for sibling in siblings:
        with sibling.open("r") as f:
            assert f.read().strip() == "1", sibling

    update_source("2")

    # Siblings must be updated together.
    for sibling in siblings:
        assert 1 == run(args=("update", sibling.name))

    # Update brother and check the sister has not been changed.
    assert 0 == run(args=("update", "--with-siblings", brother.name))

    for sibling in siblings:
        with sibling.open("r") as f:
            assert f.read().strip() == "2", sibling

    update_source("3")

    # Siblings kept together even when one is removed.
    repo.index.remove([brother.name], working_tree=True)
    repo.index.commit("Brother removed")

    assert not brother.exists()

    # Update should find also missing siblings.
    assert 1 == run(args=("update",))
    assert 0 == run(args=("update", "--with-siblings"))

    for sibling in siblings:
        with sibling.open("r") as f:
            assert f.read().strip() == "3", sibling


def test_siblings_in_output_directory(runner, project, run):
    """Files in output directory are linked or removed after update."""
    repo = git.Repo(project)
    cwd = Path(project)
    source = cwd / "source.txt"
    output = cwd / "output"

    files = [
        ("first", "1"),
        ("second", "2"),
        ("third", "3"),
    ]

    def write_source():
        """Write source from files."""
        with source.open("w") as fp:
            fp.write("\n".join(" ".join(line) for line in files) + "\n")

        repo.git.add("--all")
        repo.index.commit("Update source.txt")

    def check_files():
        """Check file content."""
        assert len(files) == len(list(output.rglob("*")))

        for name, content in files:
            with (output / name).open() as fp:
                assert content == fp.read().strip(), name

    write_source()

    script = 'mkdir -p "$0"; ' "cat - | while read -r name content; do " 'echo "$content" > "$0/$name"; done'
    base_sh = ["sh", "-c", script, "output"]

    assert not output.exists()
    assert 0 == run(args=["run"] + base_sh + ["output"], stdin=source)
    assert output.exists()
    check_files()

    files = [
        ("first", "11"),
        ("third", "3"),
        ("fourth", "4"),
    ]
    write_source()
    assert 0 == run(args=["update", "output"])
    check_files()


def test_relative_path_for_directory_input(client, run, cli):
    """Test having a directory input generates relative path in CWL."""
    (client.path / DATA_DIR / "file1").write_text("file1")
    client.repo.git.add("--all")
    client.repo.index.commit("Add file")

    assert 0 == run(args=["run", "ls", DATA_DIR], stdout="ls.data")

    (client.path / DATA_DIR / "file2").write_text("file2")
    client.repo.git.add("--all")
    client.repo.index.commit("Add one more file")

    exit_code, cwl = cli("update")
    cwl = cwl.association.plan
    assert 0 == exit_code
    assert 1 == len(cwl.inputs)
    assert isinstance(cwl.inputs[0].consumes, Collection)
    assert "data" == cwl.inputs[0].consumes.path
