# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
"""Test ``config`` command."""
import subprocess
import sys
from threading import Thread
from time import sleep

import pytest

from renku.cli import cli
from tests.utils import format_result_exception, retry_failed


def test_config_value_locally(client, runner, project, global_config_dir):
    """Check setting/getting from local configuration."""
    result = runner.invoke(cli, ["config", "set", "key", "local-value"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["config", "show", "key"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert result.output == "local-value\n"
    # Value set locally is not visible globally
    result = runner.invoke(cli, ["config", "show", "key", "--global"])
    assert 2 == result.exit_code

    # Reading non-existing values is an error
    result = runner.invoke(cli, ["config", "show", "non-existing"])
    assert 2 == result.exit_code


def test_config_value_globally(client, runner, project, global_config_dir):
    """Check setting/getting from global configuration."""
    result = runner.invoke(cli, ["config", "set", "key", "global-value", "--global"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["config", "show", "key"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert result.output == "global-value\n"
    result = runner.invoke(cli, ["config", "show", "key", "--global"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert result.output == "global-value\n"
    # Value set globally is not visible in local config
    result = runner.invoke(cli, ["config", "show", "key", "--local"])
    assert 2 == result.exit_code


def test_config_default(client, runner, project, global_config_dir):
    """Check setting/getting from local configuration."""
    result = runner.invoke(cli, ["config", "set", "lfs_threshold", "0b"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["config", "set", "lfs_threshold", "10mb", "--global"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["config", "show", "lfs_threshold"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert result.output == "0b\n"

    result = runner.invoke(cli, ["config", "show", "lfs_threshold", "--global"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert result.output == "10mb\n"

    result = runner.invoke(cli, ["config", "show", "lfs_threshold", "--default"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert result.output == "100kb\n"


def test_config_get_non_existing_value(client, runner, project, global_config_dir):
    """Check getting non-existing value is an error."""
    result = runner.invoke(cli, ["config", "show", "non-existing"])
    assert 2 == result.exit_code


def test_local_overrides_global_config(client, runner, project, global_config_dir):
    """Test setting config both global and locally."""
    result = runner.invoke(cli, ["config", "set", "key", "global-value", "--global"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["config", "show", "key"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert result.output == "global-value\n"

    result = runner.invoke(cli, ["config", "set", "key", "local-value"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["config", "show", "key"])
    assert 0 == result.exit_code, format_result_exception(result)
    assert result.output == "local-value\n"


@pytest.mark.parametrize("global_only", (False, True))
def test_config_remove_value_locally(client, runner, project, global_config_dir, global_only):
    """Check removing value from local configuration."""
    param = ["--global"] if global_only else []
    result = runner.invoke(cli, ["config", "set", "key", "some-value"] + param)
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["config", "show", "key"] + param)
    assert "some-value\n" == result.output

    result = runner.invoke(cli, ["config", "remove", "key"] + param)
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["config", "show", "key"] + param)
    assert "some-value" not in result.output


def test_local_config_committed(client, runner, data_repository, global_config_dir):
    """Test local configuration update is committed only when it is changed."""
    commit_sha_before = client.repo.head.object.hexsha

    result = runner.invoke(cli, ["config", "set", "local-key", "value"])
    assert 0 == result.exit_code, format_result_exception(result)
    commit_sha_after = client.repo.head.object.hexsha
    assert commit_sha_after != commit_sha_before

    # Adding the same config should not create a new commit
    commit_sha_before = client.repo.head.object.hexsha

    result = runner.invoke(cli, ["config", "set", "local-key", "value"])
    assert 0 == result.exit_code, format_result_exception(result)
    commit_sha_after = client.repo.head.object.hexsha
    assert commit_sha_after == commit_sha_before

    # Adding a global config should not create a new commit
    result = runner.invoke(cli, ["config", "set", "global-key", "value", "--global"])
    assert 0 == result.exit_code, format_result_exception(result)
    commit_sha_after = client.repo.head.object.hexsha
    assert commit_sha_after == commit_sha_before


@pytest.mark.parametrize(
    "args,message",
    [
        (
            ["show", "--local", "--global", "key"],
            "Illegal usage: `local_only` is mutually exclusive with arguments `--default, --global`",
        ),
    ],
)
def test_invalid_command_args(client, runner, project, global_config_dir, args, message):
    """Test invalid combination of command-line arguments."""
    result = runner.invoke(cli, ["config"] + args)
    assert 2 == result.exit_code
    assert message in result.output


@pytest.mark.parametrize("config_key", ["data_directory"])
def test_readonly_config(client, runner, project, config_key):
    """Test readonly config can only be set once."""
    result = runner.invoke(cli, ["config", "set", config_key, "value"])
    assert 0 == result.exit_code, format_result_exception(result)

    result = runner.invoke(cli, ["config", "set", config_key, "value"])
    assert 2 == result.exit_code
    assert f"Configuration {config_key} cannot be modified." in result.output

    result = runner.invoke(cli, ["config", "remove", config_key])
    assert 2 == result.exit_code
    assert f"Configuration {config_key} cannot be modified." in result.output


def test_config_read_concurrency(runner, project, client, run):
    """Test config can be read concurrently."""
    result = runner.invoke(cli, ["config", "set", "test", "value"])
    assert 0 == result.exit_code, format_result_exception(result)

    command = [
        "nice",  # NOTE: Set low priority to increase chance of concurrency issues happening
        "-n",
        "19",
        sys.executable,
        "-m",
        "renku.cli",
        "config",
        "show",
        "test",
    ]

    processes = []

    for _ in range(20):
        processes.append(subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE))

    assert all(p.wait() == 0 for p in processes)
    assert all(p.stdout.read().decode("utf8") == "value\n" for p in processes)


@retry_failed
def test_config_write_concurrency(monkeypatch, runner, project, client, run):
    """Test config cannot be written concurrently. Only one execution succeedes in that case."""
    from renku.core.management.config import ConfigManagerMixin

    REPETITIONS = 4
    CONFIG_KEY = "write_key"
    CONFIG_VALUE = "write_value"

    # NOTE: monkey patch the _write_config private method to introduce a slowdown when writing to the file
    with monkeypatch.context() as mp:

        def _write_config(s, filepath, config):
            with open(filepath, "w+") as file:
                sleep(REPETITIONS + 1)
                config.write(file)

        mp.setattr(ConfigManagerMixin, "_write_config", _write_config)

        def write_value(index):
            result = runner.invoke(cli, ["config", "set", "--global", CONFIG_KEY, CONFIG_VALUE])
            results[index] = result

        def get_value():
            result = runner.invoke(cli, ["config", "show", "--global", CONFIG_KEY])
            return result.output if "not found" not in result.output else False

        # NOTE: check the value was not previously set
        assert not get_value()

        threads = [None] * REPETITIONS
        results = [None] * REPETITIONS
        for i in range(REPETITIONS):
            threads[i] = Thread(target=write_value, args=(i,))
            threads[i].start()
            sleep(1)

        for i in range(REPETITIONS):
            threads[i].join()

        # NOTE: verify all executions finish, some succesfully and others not
        KO = "Unable to acquire lock"
        OK = "OK"
        assert all(0 == r.exit_code for r in results)
        assert any(KO in r.output for r in results)
        assert any(OK in r.output for r in results)

        # NOTE: assess only one execution succeeded and all the other failed
        def single_true(iterable):
            i = iter(iterable)
            return any(i) and not any(i)

        assert single_true(OK in r.output for r in results)
        assert all(KO in r.output or OK in r.output for r in results)

        # NOTE: assess the value was actually written
        assert CONFIG_VALUE in get_value()
