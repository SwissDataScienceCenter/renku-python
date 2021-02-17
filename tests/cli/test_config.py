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
import pytest

from renku.cli import cli


def test_config_value_locally(client, runner, project, global_config_dir):
    """Check setting/getting from local configuration."""
    result = runner.invoke(cli, ["config", "set", "key", "local-value"])
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["config", "show", "key"])
    assert 0 == result.exit_code
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
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["config", "show", "key"])
    assert 0 == result.exit_code
    assert result.output == "global-value\n"
    result = runner.invoke(cli, ["config", "show", "key", "--global"])
    assert 0 == result.exit_code
    assert result.output == "global-value\n"
    # Value set globally is not visible in local config
    result = runner.invoke(cli, ["config", "show", "key", "--local"])
    assert 2 == result.exit_code


def test_config_default(client, runner, project, global_config_dir):
    """Check setting/getting from local configuration."""
    result = runner.invoke(cli, ["config", "set", "lfs_threshold", "0b"])
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["config", "set", "lfs_threshold", "10mb", "--global"])
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["config", "show", "lfs_threshold"])
    assert 0 == result.exit_code
    assert result.output == "0b\n"

    result = runner.invoke(cli, ["config", "show", "lfs_threshold", "--global"])
    assert 0 == result.exit_code
    assert result.output == "10mb\n"

    result = runner.invoke(cli, ["config", "show", "lfs_threshold", "--default"])
    assert 0 == result.exit_code
    assert result.output == "100kb\n"


def test_config_get_non_existing_value(client, runner, project, global_config_dir):
    """Check getting non-existing value is an error."""
    result = runner.invoke(cli, ["config", "show", "non-existing"])
    assert 2 == result.exit_code


def test_local_overrides_global_config(client, runner, project, global_config_dir):
    """Test setting config both global and locally."""
    result = runner.invoke(cli, ["config", "set", "key", "global-value", "--global"])
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["config", "show", "key"])
    assert 0 == result.exit_code
    assert result.output == "global-value\n"

    result = runner.invoke(cli, ["config", "set", "key", "local-value"])
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["config", "show", "key"])
    assert 0 == result.exit_code
    assert result.output == "local-value\n"


@pytest.mark.parametrize("global_only", (False, True))
def test_config_remove_value_locally(client, runner, project, global_config_dir, global_only):
    """Check removing value from local configuration."""
    param = ["--global"] if global_only else []
    result = runner.invoke(cli, ["config", "set", "key", "some-value"] + param)
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["config", "show", "key"] + param)
    assert "some-value\n" == result.output

    result = runner.invoke(cli, ["config", "remove", "key"] + param)
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["config", "show", "key"] + param)
    assert "some-value" not in result.output


def test_local_config_committed(client, runner, data_repository, global_config_dir):
    """Test local configuration update is committed only when it is changed."""
    commit_sha_before = client.repo.head.object.hexsha

    result = runner.invoke(cli, ["config", "set", "local-key", "value"])
    assert 0 == result.exit_code
    commit_sha_after = client.repo.head.object.hexsha
    assert commit_sha_after != commit_sha_before

    # Adding the same config should not create a new commit
    commit_sha_before = client.repo.head.object.hexsha

    result = runner.invoke(cli, ["config", "set", "local-key", "value"])
    assert 0 == result.exit_code
    commit_sha_after = client.repo.head.object.hexsha
    assert commit_sha_after == commit_sha_before

    # Adding a global config should not create a new commit
    result = runner.invoke(cli, ["config", "set", "global-key", "value", "--global"])
    assert 0 == result.exit_code
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
    assert 0 == result.exit_code

    result = runner.invoke(cli, ["config", "set", config_key, "value"])
    assert 2 == result.exit_code
    assert f"Configuration {config_key} cannot be modified." in result.output

    result = runner.invoke(cli, ["config", "remove", config_key])
    assert 2 == result.exit_code
    assert f"Configuration {config_key} cannot be modified." in result.output
