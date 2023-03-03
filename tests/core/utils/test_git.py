#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""Test git utility functions."""

from typing import Optional

import pytest

from renku.core.util.git import get_remote, push_changes
from tests.fixtures.config import IT_PROTECTED_REMOTE_REPO_URL, IT_REMOTE_NON_RENKU_REPO_URL
from tests.utils import retry_failed, write_and_commit_file


@pytest.mark.parametrize(
    "name, url",
    [
        [None, IT_PROTECTED_REMOTE_REPO_URL],
        ["origin", IT_PROTECTED_REMOTE_REPO_URL],
        ["second-remote", IT_REMOTE_NON_RENKU_REPO_URL],
    ],
)
def test_get_remote(git_repository_with_multiple_remotes, name: Optional[str], url: Optional[str]):
    """Test getting remote of a git repository."""
    remote = get_remote(git_repository_with_multiple_remotes, name=name)
    assert remote is not None
    assert url == remote.url
    remote = get_remote(git_repository_with_multiple_remotes, url=url)
    assert remote is not None
    assert url == remote.url


def test_get_non_existing_remote(git_repository_with_multiple_remotes):
    """Test getting non-existing remotes."""
    assert get_remote(git_repository_with_multiple_remotes, name="non-existing") is None

    assert get_remote(git_repository_with_multiple_remotes, url="non-existing") is None


@pytest.mark.integration
@retry_failed
def test_push_to_protected_branch(protected_git_repository):
    """Test pushing to a protected branch creates a new branch and resets the protected branch."""
    protected_git_repository.checkout("master")  # master branch is protected
    commit_sha_before = protected_git_repository.active_branch.commit.hexsha

    write_and_commit_file(protected_git_repository, "new-file", "some content")
    commit_sha_after = protected_git_repository.head.commit.hexsha

    pushed_branch = push_changes(protected_git_repository)

    assert "master" == protected_git_repository.active_branch.name
    assert commit_sha_before == protected_git_repository.active_branch.commit.hexsha
    assert "master" != pushed_branch

    branch = protected_git_repository.branches[pushed_branch]
    assert commit_sha_after == branch.commit.hexsha
    assert f"origin/{branch.name}" == branch.remote_branch.name


@pytest.mark.integration
def test_push_to_protected_branch_with_no_reset(protected_git_repository):
    """Test pushing to a protected branch creates a new branch and keeps the protected branch."""
    protected_git_repository.checkout("master")  # master branch is protected
    write_and_commit_file(protected_git_repository, "new-file", "some content")
    commit_sha_after = protected_git_repository.head.commit.hexsha

    pushed_branch = push_changes(protected_git_repository, reset=False)

    assert "master" == protected_git_repository.active_branch.name
    assert commit_sha_after == protected_git_repository.active_branch.commit.hexsha
    assert "master" != pushed_branch


@pytest.mark.integration
@pytest.mark.serial
def test_push_to_diverged_branch(protected_git_repository, mocker):
    """Test pushing to a branch that has conflict with local branch."""
    write_and_commit_file(protected_git_repository, "new-file", "some content")
    pushed_branch = push_changes(protected_git_repository)
    protected_git_repository.checkout(pushed_branch)
    commit_sha_before = protected_git_repository.active_branch.commit.hexsha

    # Create a conflicting commit
    protected_git_repository.reset("HEAD~", hard=True)
    write_and_commit_file(protected_git_repository, "new-file", "some other content")
    commit_sha_after = protected_git_repository.head.commit.hexsha

    mocker.patch("renku.core.util.communication.confirm", lambda *_, **__: False)

    new_pushed_branch = push_changes(protected_git_repository)

    assert pushed_branch == protected_git_repository.active_branch.name
    assert commit_sha_before == protected_git_repository.active_branch.commit.hexsha
    assert pushed_branch != new_pushed_branch

    branch = protected_git_repository.branches[new_pushed_branch]
    assert commit_sha_after == branch.commit.hexsha
    assert f"origin/{branch.name}" == branch.remote_branch.name
