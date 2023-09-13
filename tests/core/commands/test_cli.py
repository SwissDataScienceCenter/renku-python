#
# Copyright 2017-2023 - Swiss Data Science Center (SDSC)
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
"""CLI tests."""

import contextlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

import renku.core.lfs
from renku import __version__
from renku.core.config import get_value, load_config, remove_value, set_value, store_config
from renku.core.constant import DEFAULT_DATA_DIR as DATA_DIR
from renku.core.util.contexts import chdir
from renku.domain_model.enums import ConfigFilter
from renku.domain_model.project_context import project_context
from renku.infrastructure.repository import Repository
from renku.ui.cli import cli
from tests.utils import format_result_exception


def test_version(runner):
    """Test cli version."""
    result = runner.invoke(cli, ["--version"])
    assert __version__ in result.output.split("\n")


@pytest.mark.parametrize("arg", (("help",), ("-h",), ("--help",)))
def test_help(arg, runner):
    """Test cli help."""
    result = runner.invoke(cli, [arg])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Show this message and exit." in result.output


@pytest.mark.parametrize("cwd", (DATA_DIR, "notebooks", "subdir"))
def test_run_from_non_root(runner, project, cwd):
    """Test running renku not from project's root."""
    path = project.path / cwd
    path.mkdir(parents=True, exist_ok=True)
    with chdir(path):
        result = runner.invoke(cli, ["dataset", "ls"])
        assert 0 == result.exit_code, format_result_exception(result)
        assert "Run CLI commands only from project's root" in result.output

        result = runner.invoke(cli, ["help"])
        assert 0 == result.exit_code, format_result_exception(result)
        assert "Run CLI commands only from project" not in result.output

    result = runner.invoke(cli, ["dataset", "ls"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "Run CLI commands only from project's root" not in result.output


def test_config_path(runner):
    """Test config path."""
    result = runner.invoke(cli, ["--global-config-path"])
    output = result.output.split("\n")[0]
    assert "renku.ini" in output


_CMD_EXIT_2 = ["bash", "-c", "exit 2"]


@pytest.mark.parametrize(
    "cmd, exit_code",
    (
        (_CMD_EXIT_2, 1),
        (["--success-code", "1", "--no-output"] + _CMD_EXIT_2, 1),
        (["--success-code", "2", "--no-output"] + _CMD_EXIT_2, 0),
        (["--success-code", "0", "--no-output", "echo", "hola"], 0),
    ),
)
def test_exit_code(cmd, exit_code, runner, project):
    """Test exit-code of run command."""
    result = runner.invoke(cli, ["run"] + cmd)
    assert exit_code == result.exit_code


def test_streams(runner, project, capsys, no_lfs_warning):
    """Test redirection of std streams."""
    repository = Repository(".")

    with open("source.txt", "w") as source:
        source.write("first,second,third")

    repository.add(all=True)
    repository.commit("Added source.txt")

    workflow_name = "run1"
    with capsys.disabled():
        with open("source.txt", "rb") as stdin:
            with open("result.txt", "wb") as stdout:
                try:
                    old_stdin, old_stdout = sys.stdin, sys.stdout
                    sys.stdin, sys.stdout = stdin, stdout
                    try:
                        cli.main(
                            args=("run", "--name", workflow_name, "cut", "-d,", "-f", "2", "-s"),
                            prog_name=runner.get_default_prog_name(cli),
                        )
                    except SystemExit as e:
                        assert e.code in {None, 0}
                finally:
                    sys.stdin, sys.stdout = old_stdin, old_stdout

    with open("result.txt") as f:
        assert f.read().strip() == "second"

    result = runner.invoke(cli, ["workflow", "export", workflow_name])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["status"])
    assert 0 == result.exit_code, format_result_exception(result)

    # Check that source.txt is not shown in outputs.
    result = runner.invoke(cli, ["workflow", "outputs", "source.txt"])
    assert 1 == result.exit_code

    result = runner.invoke(cli, ["workflow", "outputs"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert {"result.txt"} == set(result.output.strip().split("\n"))

    # Check that source.txt is shown in inputs.
    result = runner.invoke(cli, ["workflow", "inputs"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert {"source.txt"} == set(result.output.strip().split("\n"))

    with open("source.txt", "w") as source:
        source.write("first,second,third,fourth")

    repository.add(all=True)
    repository.commit("Changed source.txt")

    result = runner.invoke(cli, ["status"])
    assert 1 == result.exit_code
    assert "source.txt" in result.output


def test_streams_cleanup(runner, project, run):
    """Test cleanup of standard streams."""
    source = project.path / "source.txt"
    stdout = project.path / "result.txt"

    with source.open("w") as fp:
        fp.write("first,second,third")

    # File outside the Git index should be deleted.

    with source.open("r") as fp:
        assert fp.read() == "first,second,third"

    assert not stdout.exists()

    result = runner.invoke(cli, ["status"])

    # Dirty repository check.
    assert 0 == result.exit_code

    # File from the Git index should be restored.
    with stdout.open("w") as fp:
        fp.write("1")

    project.repository.add("result.txt")

    with stdout.open("r") as fp:
        assert fp.read() == "1"


def test_streams_and_args_names(runner, project, capsys, no_lfs_warning):
    """Test streams and conflicting argument names."""
    with capsys.disabled():
        with open("lalala", "wb") as stdout:
            try:
                old_stdout = sys.stdout
                sys.stdout = stdout
                try:
                    cli.main(args=("run", "echo", "lalala"))
                except SystemExit as e:
                    assert e.code in {None, 0}
            finally:
                sys.stdout = old_stdout

    with open("lalala") as f:
        assert f.read().strip() == "lalala"

    result = runner.invoke(cli, ["status"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)


def test_show_inputs(tmpdir_factory, project, runner, run, template):
    """Test show inputs with submodules."""
    second_project = Path(str(tmpdir_factory.mktemp("second_project")))

    parameters = []
    for key in set(template["metadata"].keys()):
        parameters.append("--parameter")
        parameters.append(f'{key}="{template["metadata"][key]}"')
    assert 0 == run(args=("init", str(second_project), "--template-id", template["id"], *parameters))

    woop = second_project / "woop"
    with woop.open("w") as fp:
        fp.write("woop")

    second_repository = Repository(second_project)
    second_repository.add(all=True)
    second_repository.commit("Added woop file")

    assert 0 == run(args=("dataset", "create", "foo"))
    assert 0 == run(args=("dataset", "add", "--copy", "foo", str(woop)))

    imported_woop = project.path / DATA_DIR / "foo" / woop.name
    assert imported_woop.exists()

    woop_wc = project.path / "woop.wc"
    assert 0 == run(args=("run", "wc"), stdin=imported_woop, stdout=woop_wc)

    result = runner.invoke(cli, ["workflow", "inputs"], catch_exceptions=False)
    assert {str(imported_woop.resolve().relative_to(project.path.resolve()))} == set(result.output.strip().split("\n"))


def test_configuration_of_no_external_storage(isolated_runner, monkeypatch, project_init):
    """Test the LFS requirement for renku run with disabled storage."""
    runner = isolated_runner
    data, commands = project_init

    os.mkdir("test-project")
    os.chdir("test-project")

    result = runner.invoke(cli, ["--no-external-storage"] + commands["init"] + commands["id"], commands["confirm"])
    assert 0 == result.exit_code, format_result_exception(result)
    # Pretend that git-lfs is not installed.
    with monkeypatch.context() as monkey:
        monkey.setattr(renku.core.lfs, "storage_installed", lambda: False)
        # Missing --no-external-storage flag.
        result = runner.invoke(cli, ["run", "touch", "output"])
        assert "External storage is not configured" in result.output
        assert 1 == result.exit_code

        # Since repo is not using external storage.
        result = runner.invoke(cli, ["--no-external-storage", "run", "touch", "output"])
        assert 0 == result.exit_code, format_result_exception(result)

    subprocess.call(["git", "clean", "-df"])
    result = runner.invoke(cli, ["--no-external-storage", "run", "touch", "output"])
    # Needs to result in error since output file
    # is now considered an input file (check run.py doc).
    assert 1 == result.exit_code


def test_configuration_of_external_storage(isolated_runner, monkeypatch, project_init):
    """Test the LFS requirement for renku run."""
    runner = isolated_runner
    data, commands = project_init

    result = runner.invoke(cli, ["--external-storage"] + commands["init"] + commands["id"], commands["confirm"])
    assert 0 == result.exit_code, format_result_exception(result)
    # Pretend that git-lfs is not installed.
    with monkeypatch.context() as monkey:
        monkey.setattr(renku.core.lfs, "storage_installed", lambda: False)
        # Repo is using external storage but it's not installed.
        result = runner.invoke(cli, ["run", "touch", "output"])
        assert 1 == result.exit_code
        assert "External storage is not configured" in result.output

        result = runner.invoke(cli, ["-S", "run", "touch", "output"])
        assert 1 == result.exit_code
        assert "External storage is not installed" in result.output

    # Clean repo and check external storage.
    subprocess.call(["git", "clean", "-df"])
    result = runner.invoke(cli, ["run", "touch", "output2"])
    assert 0 == result.exit_code, format_result_exception(result)


def test_early_check_of_external_storage(isolated_runner, monkeypatch, directory_tree, project_init):
    """Test LFS is checked early."""
    data, commands = project_init

    result = isolated_runner.invoke(
        cli, ["--no-external-storage"] + commands["init"] + commands["id"], commands["confirm"]
    )
    assert 0 == result.exit_code, format_result_exception(result)

    result = isolated_runner.invoke(cli, ["dataset", "create", "my-dataset"])
    assert 0 == result.exit_code, format_result_exception(result)

    # Pretend that git-lfs is not installed.
    with monkeypatch.context() as monkey:
        monkey.setattr(renku.core.lfs, "storage_installed", lambda: False)

        failing_command = ["dataset", "add", "--copy", "-s", "src", "my-dataset", str(directory_tree)]
        result = isolated_runner.invoke(cli, failing_command)
        assert 1 == result.exit_code
        assert "External storage is not configured" in result.output

        result = isolated_runner.invoke(cli, ["--no-external-storage"] + failing_command)
        assert 2 == result.exit_code
        assert "Cannot use '-s/--src/--source' with URLs" in result.output


def test_file_tracking(isolated_runner, project_init):
    """Test .gitattribute handling on renku run."""
    runner = isolated_runner
    data, commands = project_init

    os.mkdir("test-project")
    os.chdir("test-project")
    result = runner.invoke(cli, commands["init"] + commands["id"], commands["confirm"])
    assert 0 == result.exit_code, format_result_exception(result)
    result = runner.invoke(cli, ["config", "set", "lfs_threshold", "0b"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["run", "touch", "tracked"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "tracked" in Path(".gitattributes").read_text()

    result = runner.invoke(cli, ["-S", "run", "touch", "untracked"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert "untracked" not in Path(".gitattributes").read_text()


@pytest.mark.xfail
def test_status_with_submodules(isolated_runner, monkeypatch, project_init):
    """Test status calculation with submodules."""
    runner = isolated_runner
    data, commands = project_init

    os.mkdir("foo")
    os.mkdir("bar")

    with open("woop", "w") as f:
        f.write("woop")

    os.chdir("foo")
    result = runner.invoke(
        cli, commands["init"] + commands["id"] + ["--no-external-storage"], commands["confirm"], catch_exceptions=False
    )
    assert 0 == result.exit_code, format_result_exception(result)

    os.chdir("../bar")
    result = runner.invoke(
        cli, commands["init"] + commands["id"] + ["--no-external-storage"], commands["confirm"], catch_exceptions=False
    )
    assert 0 == result.exit_code, format_result_exception(result)

    os.chdir("../foo")
    with monkeypatch.context() as monkey:
        monkey.setattr(renku.core.lfs, "storage_installed", lambda: False)

        result = runner.invoke(cli, ["dataset", "add", "--copy", "f", "../woop"], catch_exceptions=False)

        assert 1 == result.exit_code
        subprocess.call(["git", "clean", "-dff"])

    result = runner.invoke(cli, ["-S", "dataset", "add", "--copy", "f", "../woop"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    os.chdir("../bar")
    result = runner.invoke(cli, ["-S", "dataset", "add", "--copy", "b", "../foo/data/f/woop"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    # Produce a derived data from the imported data.
    with open("woop.wc", "w") as stdout:
        with contextlib.redirect_stdout(stdout):
            try:
                cli.main(args=("-S", "run", "wc", "data/b/woop"), prog_name=runner.get_default_prog_name(cli))
            except SystemExit as e:
                assert e.code in {None, 0}

    result = runner.invoke(cli, ["status"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    # Modify the source data.
    os.chdir("../foo")
    with open("data/f/woop", "w") as f:
        f.write("woop2")

    subprocess.call(["git", "commit", "-am", "commiting changes to woop"])

    os.chdir("../bar")
    subprocess.call(["git", "submodule", "update", "--rebase", "--remote"])
    subprocess.call(["git", "commit", "-am", "update submodule"])

    result = runner.invoke(cli, ["status"], catch_exceptions=False)
    assert 0 != result.exit_code

    # Test relative graph export output
    cmd = ["--path", "../foo", "graph", "export"]
    result = runner.invoke(cli, cmd, catch_exceptions=False)
    assert "../foo/data/f/woop" in result.output
    assert 0 == result.exit_code, format_result_exception(result)


def test_status_consistency(runner, project):
    """Test status consistency in subdirectories."""
    os.mkdir("somedirectory")
    with open("somedirectory/woop", "w") as fp:
        fp.write("woop")

    project.repository.add("somedirectory/woop")
    project.repository.commit("add woop")

    result = runner.invoke(cli, ["run", "cp", "somedirectory/woop", "somedirectory/meeh"])
    assert 0 == result.exit_code, format_result_exception(result)

    with open("somedirectory/woop", "w") as fp:
        fp.write("weep")

    project.repository.add("somedirectory/woop")
    project.repository.commit("fix woop")

    base_result = runner.invoke(cli, ["status"])
    os.chdir("somedirectory")
    comp_result = runner.invoke(cli, ["status"])

    assert 1 == base_result.exit_code, format_result_exception(base_result)
    assert 1 == comp_result.exit_code, format_result_exception(comp_result)

    base_result_stdout = "\n".join(base_result.stdout.split("\n"))
    comp_result_stdout = "\n".join(comp_result.stdout.split("\n"))
    assert base_result_stdout.replace("somedirectory/", "") == comp_result_stdout


def test_unchanged_output(runner, project):
    """Test detection of unchanged output."""
    result = runner.invoke(cli, ["run", "touch", "1"], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["run", "touch", "1"], catch_exceptions=False)
    assert 1 == result.exit_code


def test_unchanged_stdout(runner, project, capsys, no_lfs_warning):
    """Test detection of unchanged stdout."""
    with capsys.disabled():
        with open("output.txt", "wb") as stdout:
            try:
                old_stdout = sys.stdout
                sys.stdout = stdout
                try:
                    cli.main(args=("run", "echo", "1"))
                except SystemExit as e:
                    assert e.code in {None, 0}
            finally:
                sys.stdout = old_stdout

    with capsys.disabled():
        with open("output.txt", "wb") as stdout:
            try:
                old_stdout = sys.stdout
                sys.stdout = stdout
                try:
                    cli.main(args=("run", "echo", "1"))
                except SystemExit as e:
                    # The stdout has not been modified!
                    assert e.code in {None, 1}
            finally:
                sys.stdout = old_stdout


@pytest.mark.skip(reason="renku update not implemented with new metadata yet, reenable later")
def test_modified_output(runner, project, run):
    """Test detection of changed file as output."""
    cwd = project.path
    source = cwd / "source.txt"
    data = cwd / DATA_DIR / "results"
    data.mkdir(parents=True)
    output = data / "result.txt"

    cmd = ["run", "cp", "-r", str(source), str(output)]

    def update_source(content):
        """Update source.txt."""
        with source.open("w") as fp:
            fp.write(content)

        project.repository.add(all=True)
        project.repository.commit("Updated source.txt")

    update_source("1")

    # The output file does not exist.
    assert not output.exists()

    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code, format_result_exception(result)

    # The output file is copied from the source.
    with output.open("r") as f:
        assert f.read().strip() == "1"

    update_source("2")

    # The input file has been updated and output is recreated.
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code, format_result_exception(result)

    with output.open("r") as f:
        assert f.read().strip() == "2"

    update_source("3")

    # The input has been modifed and we check that the previous
    # run command correctly recognized output.txt.
    assert 0 == run()

    with output.open("r") as f:
        assert f.read().strip() == "3"


def test_outputs(runner, project):
    """Test detection of outputs."""
    siblings = {"brother", "sister"}

    cmd = ["run", "touch"] + list(siblings)
    result = runner.invoke(cli, cmd)
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["workflow", "outputs"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert siblings == set(result.output.strip().split("\n"))


def test_deleted_input(runner, project, capsys):
    """Test deleted input."""
    input = project.path / "input.txt"
    with input.open("w") as f:
        f.write("first")

    project.repository.add(all=True)
    project.repository.commit("Created input.txt")

    cmd = ["run", "mv", input.name, "input.mv"]
    result = runner.invoke(cli, cmd, catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)
    assert not input.exists()
    assert Path("input.mv").exists()


@pytest.mark.skip(reason="renku update not implemented with new metadata yet, reenable later")
def test_input_directory(runner, project, run, no_lfs_warning):
    """Test detection of input directory."""
    cwd = project.path
    output = cwd / "output.txt"
    inputs = cwd / "inputs"
    inputs.mkdir(parents=True)

    gitkeep = inputs / ".gitkeep"
    gitkeep.touch()
    project.repository.add(all=True)
    project.repository.commit("Empty inputs directory")

    assert 0 == run(args=("run", "ls", str(inputs)), stdout=output)
    with output.open("r") as fp:
        assert "" == fp.read().strip()

    (inputs / "first").touch()

    project.repository.add(all=True)
    project.repository.commit("Created inputs")

    assert 0 == run(args=("update", output.name))

    with output.open("r") as fp:
        assert "first\n" == fp.read()

    (inputs / "second").touch()
    project.repository.add(all=True)
    project.repository.commit("Added second input")

    assert 0 == run(args=("update", output.name))
    with output.open("r") as fp:
        assert "first\nsecond\n" == fp.read()

    result = runner.invoke(cli, ["workflow", "inputs"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert {str(p.relative_to(cwd)) for p in inputs.rglob("*") if p.name != ".gitkeep"} == set(
        result.output.strip().split("\n")
    )


@pytest.mark.parametrize("global_only, config_path_attr", ((False, "local_config_path"), (True, "global_config_path")))
def test_config_manager_creation(project, global_only, config_path_attr):
    """Check creation of configuration file."""
    path = str(getattr(project_context, config_path_attr))
    assert path.endswith("renku.ini")
    config = load_config(config_filter=ConfigFilter.ALL)
    store_config(config, global_only=global_only)
    assert Path(path).exists()


@pytest.mark.parametrize("global_only", (False, True))
def test_config_manager_set_value(project, global_only):
    """Check writing to configuration."""
    config_filter = ConfigFilter.GLOBAL_ONLY

    if not global_only:
        config_filter = ConfigFilter.LOCAL_ONLY

    set_value("zenodo", "access_token", "my-secret", global_only=global_only)

    config = load_config(config_filter=config_filter)
    assert config.get("zenodo", "access_token") == "my-secret"

    remove_value("zenodo", "access_token", global_only=global_only)
    config = load_config(config_filter=config_filter)
    assert "zenodo" not in config.sections()


def test_config_get_value(project):
    """Check reading from configuration."""
    # Value set locally is not visible globally
    set_value("local", "key", "local-value")
    value = get_value("local", "key")
    assert "local-value" == value
    value = get_value("local", "key", config_filter=ConfigFilter.GLOBAL_ONLY)
    assert value is None

    # Value set globally is stored globally
    set_value("global", "key", "global-value", global_only=True)
    value = get_value("global", "key", config_filter=ConfigFilter.LOCAL_ONLY)
    assert value is None
    value = get_value("global", "key", config_filter=ConfigFilter.GLOBAL_ONLY)
    assert "global-value" == value
    value = get_value("global", "key")
    assert "global-value" == value

    # Reading non-existing values returns None
    value = get_value("non-existing", "key")
    assert value is None


def test_lfs_size_limit(isolated_runner, project_init):
    """Test inclusion of files in lfs by size."""
    runner = isolated_runner
    data, commands = project_init

    os.mkdir("test-project")
    os.chdir("test-project")
    result = runner.invoke(cli, commands["init"] + commands["id"], commands["confirm"])
    assert 0 == result.exit_code, format_result_exception(result)

    large = Path("large")
    with large.open("w") as f:
        f.write("x" * 1024**2)

    result = runner.invoke(
        cli, ["dataset", "add", "--copy", "--create", "my-dataset", str(large)], catch_exceptions=False
    )
    assert 0 == result.exit_code, format_result_exception(result)
    assert "large" in Path(".gitattributes").read_text()

    small = Path("small")
    with small.open("w") as f:
        f.write("small file")

    result = runner.invoke(cli, ["dataset", "add", "--copy", "my-dataset", str(small)], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)
    assert "small" not in Path(".gitattributes").read_text()


@pytest.mark.parametrize(
    "ignore,path,tracked",
    (
        ("file1", "file1", False),
        ("!file1", "file1", True),
        ("dir1", "dir1/file1", False),
        ("dir1/file1", "dir1/file1", False),
        ("dir1\n!dir1/file1", "dir1/file1", True),
        ("*.test", "file.test", False),
        ("*.test", "file.txt", True),
        ("*.test", "dir2/file.test", False),
        ("dir2\n!*.test", "dir2/file.test", True),
    ),
)
def test_lfs_ignore(isolated_runner, ignore, path, tracked, project_init):
    """Test inclusion of files in lfs by size."""
    runner = isolated_runner
    data, commands = project_init

    os.mkdir("test-project")
    os.chdir("test-project")
    result = runner.invoke(cli, commands["init"] + commands["id"], commands["confirm"])
    assert 0 == result.exit_code, format_result_exception(result)
    result = runner.invoke(cli, ["config", "set", "lfs_threshold", "0b"])
    assert 0 == result.exit_code, format_result_exception(result)

    with Path(".renkulfsignore").open("w") as f:
        f.write(ignore)
    subprocess.call(["git", "commit", "-am", "Add lfs ignore"])

    # force creation of .gitattributes by adding tracked dummy file
    with Path("dummy").open("w") as f:
        f.write("dummy")

    runner.invoke(cli, ["dataset", "add", "--move", "--create", "my-dataset", "dummy"], catch_exceptions=False)

    # add dataset file
    filepath = Path(path)

    if str(filepath.parent) != ".":
        filepath.parent.mkdir(parents=True, exist_ok=True)

    with filepath.open("w") as f:
        f.write("x" * 1024**2)

    result = runner.invoke(cli, ["dataset", "add", "--move", "my-dataset", str(filepath)], catch_exceptions=False)
    assert 0 == result.exit_code, format_result_exception(result)

    # check if file is tracked or not
    if tracked:
        assert str(filepath.name) in Path(".gitattributes").read_text()
    else:
        assert str(filepath) not in Path(".gitattributes").read_text()
