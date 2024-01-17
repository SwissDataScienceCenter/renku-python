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
"""Integration tests for ``renku clone`` command."""

from pathlib import Path

import pytest

from renku.command.clone import project_clone_command
from renku.core.login import store_token
from renku.core.util.contexts import chdir
from renku.infrastructure.repository import Repository
from renku.ui.cli import cli
from tests.utils import format_result_exception, retry_failed


@pytest.mark.integration
@retry_failed
@pytest.mark.parametrize("url", ["https://gitlab.dev.renku.ch/renku-testing/project-9"])
def test_clone(runner, monkeypatch, url):
    """Test cloning of a Renku repo and existence of required settings."""
    import renku.core.lfs

    with runner.isolated_filesystem() as project_path:
        result = runner.invoke(cli, ["clone", url, project_path])
        assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
        assert (Path(project_path) / "Dockerfile").exists()

        # Check Git hooks are installed
        result = runner.invoke(cli, ["githooks", "install"])
        assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
        assert "Hook already exists." in result.output

        result = runner.invoke(cli, ["migrate", "--strict"])
        assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

        # Check Git LFS is enabled
        with monkeypatch.context() as monkey:
            # Pretend that git-lfs is not installed.
            monkey.setattr(renku.core.lfs, "storage_installed", lambda: False)
            # Repo is using external storage but it's not installed.
            result = runner.invoke(cli, ["run", "touch", "output"])

            assert "External storage is not configured" in result.output
            assert 1 == result.exit_code, result.output + str(result.stderr_bytes)


@pytest.mark.integration
@retry_failed
@pytest.mark.parametrize("url", ["https://gitlab.dev.renku.ch/renku-testing/project-9"])
def test_clone_with_config(tmp_path, url):
    """Test cloning of a Renku repo and existence of required settings."""
    with chdir(tmp_path):
        repository, _ = (
            project_clone_command()
            .build()
            .execute(url, config={"user.name": "sam", "user.email": "s@m.i", "filter.lfs.custom": "0"})
        ).output

        assert "master" == repository.active_branch.name
        assert 0 == repository.get_configuration().get_value("filter.lfs", "custom")


@pytest.mark.integration
@retry_failed
@pytest.mark.parametrize("url", ["https://gitlab.dev.renku.ch/renku-testing/project-9"])
def test_clone_checkout_rev(tmp_path, url):
    """Test cloning of a repo checking out a rev with static config."""
    with chdir(tmp_path):
        repository, _ = (
            project_clone_command()
            .build()
            .execute(
                url,
                config={"user.name": "sam", "user.email": "s@m.i", "filter.lfs.custom": "0"},
                checkout_revision="97f907e1a3f992d4acdc97a35df73b8affc917a6",
            )
        ).output

        assert "97f907e1a3f992d4acdc97a35df73b8affc917a6" == str(repository.head.commit)
        assert 0 == repository.get_configuration().get_value("filter.lfs", "custom")


@pytest.mark.integration
@retry_failed
@pytest.mark.parametrize("rev,detached", [("test-branch", False), ("my-tag", True)])
def test_clone_checkout_revs(tmp_path, rev, detached):
    """Test cloning of a Renku repo checking out a rev."""
    with chdir(tmp_path):
        repository, _ = (
            project_clone_command()
            .build()
            .execute("https://gitlab.dev.renku.ch/renku-python-integration-tests/no-renku.git", checkout_revision=rev)
        ).output

        if detached:
            # NOTE: cloning a tag sets head to the commit of the tag, get tag that the head commit points to
            assert rev == repository.run_git_command("describe", "--tags", repository.head.commit)
        else:
            assert rev == repository.head.reference.name


@pytest.mark.integration
@pytest.mark.parametrize("path,expected_path", [("", "project-9"), (".", "."), ("new-name", "new-name")])
@retry_failed
def test_clone_uses_project_name(runner, path, expected_path):
    """Test renku clone uses project name as target-path by default."""
    remote = "https://gitlab.dev.renku.ch/renku-testing/project-9"

    with runner.isolated_filesystem() as project_path:
        result = runner.invoke(cli, ["clone", remote] + ([path] if path else []))
        assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)
        assert (Path(project_path) / expected_path / "Dockerfile").exists()


@pytest.mark.integration
@retry_failed
def test_clone_private_project_error(runner):
    """Test renku clone prints proper error message when a project is private."""
    remote = "git@dev.renku.ch:mohammad.alisafaee/test-private-project.git"

    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["clone", remote, ""])

        assert 0 != result.exit_code
        assert "Please make sure you have the correct access rights" in result.output
        assert "and the repository exists." in result.output


def test_clone_does_not_change_remote_when_no_credentials(fake_home, mocker, git_repository, runner):
    """Test cloning doesn't replace remote URL if no credentials for the hostname exists."""

    def clone_from(url, *_, **__):
        git_repository.run_git_command("remote", "set-url", "origin", url)
        return git_repository

    mocker.patch("renku.infrastructure.repository.Repository.clone_from", clone_from)

    result = runner.invoke(cli, ["clone", "https://gitlab.dev.renku.ch/renku-testing/project-9"])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    with chdir(git_repository.path):
        repository = Repository()

        assert 1 == len(repository.remotes)
        assert {"origin"} == {remote.name for remote in repository.remotes}
        assert repository.remotes["origin"].url.startswith("https://gitlab.dev.renku.ch/renku-testing")


def test_clone_changes_remote_when_credentials(fake_home, mocker, git_repository, runner):
    """Test cloning replaces remote URL if credentials exists for the hostname."""
    store_token("gitlab.dev.renku.ch", "1234")

    def clone_from(url, *_, **__):
        git_repository.run_git_command("remote", "set-url", "origin", url)
        return git_repository

    mocker.patch("renku.infrastructure.repository.Repository.clone_from", clone_from)

    result = runner.invoke(cli, ["clone", "https://gitlab.dev.renku.ch/renku-testing/project-9"])
    assert 0 == result.exit_code, format_result_exception(result) + str(result.stderr_bytes)

    with chdir(git_repository.path):
        repository = Repository()

        assert 2 == len(repository.remotes)
        assert {"origin", "renku-backup-origin"} == {remote.name for remote in repository.remotes}
        assert repository.remotes["origin"].url.startswith("https://dev.renku.ch/repos/renku-testing")
