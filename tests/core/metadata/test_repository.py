# -*- coding: utf-8 -*-
#
# Copyright 2018-2021 - Swiss Data Science Center (SDSC)
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
"""Test Repository."""
from pathlib import Path

import pytest

from renku.core import errors
from renku.core.metadata.repository import Repository

FIRST_COMMIT_SHA = "d44be0700e7ad1d062544763fd55c6ccb6f456e1"
LAST_COMMIT_SHA = "8853e0c1112e512c36db9cc76faff560b655e5d5"  # HEAD


def test_repository_create_from_sub_directories(git_repository):
    """Test creating a repository from a sub-directory has correct path."""
    repository = Repository(git_repository.path / "data", search_parent_directories=True)

    assert git_repository.path == repository.path


def test_repository_cannot_create_from_non_git(git_repository):
    """Test cannot create a repository from a directory that doesn't have ``.git``."""
    with pytest.raises(errors.GitError):
        Repository(git_repository.path / "data")


def test_repository_get_head_commit(git_repository):
    """Test get head commit of a repository."""
    commit = git_repository.head.commit

    assert LAST_COMMIT_SHA == commit.hexsha
    assert 1 == len(commit.parents)
    assert {"A", "D", "E", "F", "G", "data", "data/X"} == {o.path for o in commit.tree.values()}
    assert "changes\n" == commit.message


def test_repository_get_commit(git_repository):
    """Test getting a commit by its sha/reference."""
    commit_1 = git_repository.get_commit(revision=LAST_COMMIT_SHA)
    commit_2 = git_repository.get_commit(revision=LAST_COMMIT_SHA[:7])
    commit_3 = git_repository.get_commit(revision="HEAD")
    commit_4 = git_repository.get_commit(revision="master")

    assert LAST_COMMIT_SHA == commit_1.hexsha
    assert commit_1 == commit_2 == commit_3 == commit_4


@pytest.mark.parametrize("reference", ["invalid-ref", f"{LAST_COMMIT_SHA}666"])
def test_repository_get_non_existing_commit(git_repository, reference):
    """Test getting an invalid reference raises an error."""
    with pytest.raises(errors.GitCommitNotFoundError):
        git_repository.get_commit(revision=reference)


def test_repository_get_changes_in_a_commit(git_repository):
    """Test getting changes in a commit with multiple change types."""
    commit = git_repository.get_commit(revision="8853e0c")

    changes = {c.a_path: c for c in commit.get_changes()}

    assert "M" == changes["A"].change_type
    assert "A" == changes["A"].b_path
    assert not changes["A"].added
    assert not changes["A"].deleted

    assert changes["B"].deleted
    assert "B" == changes["B"].b_path

    assert "R" == changes["C"].change_type
    assert "data/X" == changes["C"].b_path
    assert not changes["C"].added
    assert not changes["C"].deleted
    assert not changes["A"].added


def test_repository_get_changes_in_the_first_commit(git_repository):
    """Test getting changes in the first commit."""
    commit = git_repository.get_commit(revision="d44be07")

    changes = {c.a_path: c for c in commit.get_changes()}

    assert "A" == changes["A"].change_type
    assert changes["A"].added
    assert not changes["A"].deleted
    assert "A" == changes["A"].b_path

    assert "A" == changes["B"].change_type
    assert changes["B"].added
    assert not changes["B"].deleted
    assert "B" == changes["B"].b_path


def test_repository_iterate_commits(git_repository):
    """Test iterating commits in a repository."""
    commits = list(git_repository.iterate_commits())

    assert 8 == len(commits)
    assert LAST_COMMIT_SHA == commits[0].hexsha
    assert FIRST_COMMIT_SHA == commits[7].hexsha

    commits = list(git_repository.iterate_commits(reverse=True))

    assert FIRST_COMMIT_SHA == commits[0].hexsha
    assert LAST_COMMIT_SHA == commits[7].hexsha


def test_repository_iterate_commits_with_revision(git_repository):
    """Test iterating commits in a repository with revision."""
    commits = list(git_repository.iterate_commits(revision="HEAD~"))

    assert 7 == len(commits)
    assert "9c4c6804e2f9d834f125dc253026c71c1d26adf6" == commits[0].hexsha  # One before last commit

    commits = list(git_repository.iterate_commits(revision="HEAD~.."))

    assert 1 == len(commits)
    assert LAST_COMMIT_SHA == commits[0].hexsha

    commits = list(git_repository.iterate_commits(revision=f"{FIRST_COMMIT_SHA}..a150977"))

    assert 2 == len(commits)
    assert "a150977a3d2f454e5964ce7ce41e36f34e978086" == commits[0].hexsha  # Third commit
    assert "1a5d1c7c1a6b9e32fd12da81d3ec0a1fd1872c38" == commits[1].hexsha  # Second commit


def test_repository_iterate_commits_with_path(git_repository):
    """Test iterating commits in a repository with path."""
    commits = list(git_repository.iterate_commits("A", "F"))

    assert 3 == len(commits)
    assert [f"{LAST_COMMIT_SHA[:7]}", "556dc54", f"{FIRST_COMMIT_SHA[:7]}"] == [c.hexsha[:7] for c in commits]


def test_repository_no_active_branch_when_detached(git_repository):
    """Test getting active branch when a repository is in detached HEAD state."""
    git_repository.run_git_command("checkout", "HEAD~")

    assert git_repository.active_branch is None


def test_repository_get_active_branch(git_repository):
    """Test getting active branch in a repository."""
    assert "master" == git_repository.active_branch.name


@pytest.mark.parametrize("path", ["A", Path("A")])
def test_hash_objects(git_repository, path):
    """Test hashing objects using different methods."""
    committed_object_hash = "e2466bab1aeb2df4e21c9b594c3249a75db2c263"

    assert committed_object_hash == git_repository.get_object_hash(path, revision="HEAD")
    assert committed_object_hash == git_repository.get_object_hash(git_repository.path / path, revision="HEAD")
    assert committed_object_hash == Repository.hash_objects([path])[0]
    assert committed_object_hash == Repository.hash_objects([git_repository.path / path])[0]


def test_hash_modified_objects(git_repository):
    """Test hashing modified objects that are not committed yet."""
    (git_repository.path / "A").write_text("modified")

    committed_object_hash = "e2466bab1aeb2df4e21c9b594c3249a75db2c263"
    modified_object_hash = "d84012fbd8415354de6b29158b6e5e17c4fda70b"

    # NOTE: get_object_hash returns hash of the object in the repository if revision is specified and returns the
    # current object's hash if revision is None
    assert committed_object_hash == git_repository.get_object_hash("A", revision="HEAD")
    assert modified_object_hash == git_repository.get_object_hash("A", revision=None)
    assert modified_object_hash == Repository.hash_objects(["A"])[0]

    # NOTE: Returned results are the same if object is staged
    git_repository.add("A")

    assert committed_object_hash == git_repository.get_object_hash("A", revision="HEAD")
    assert modified_object_hash == git_repository.get_object_hash("A")
    assert modified_object_hash == Repository.hash_objects(["A"])[0]


def test_hash_deleted_objects(git_repository):
    """Test hashing deleted objects."""
    assert git_repository.get_object_hash("B", revision="HEAD") is None
    assert git_repository.get_object_hash("B") is None

    with pytest.raises(errors.GitCommandError):
        Repository.hash_objects(["B"])[0]


def test_hash_directories(git_repository):
    """Test hashing tree objects."""
    (git_repository.path / "X").mkdir()
    (git_repository.path / "X" / "A").write_text("modified")

    assert git_repository.get_object_hash("X", revision="HEAD") is None
    assert git_repository.get_object_hash("X") is None

    with pytest.raises(errors.GitCommandError):
        Repository.hash_objects(["X"])[0]

    # NOTE: When staging a directory then the hash can be calculated
    git_repository.add("X")

    directory_hash = git_repository.get_object_hash("X", revision="HEAD")

    assert directory_hash is not None
    assert directory_hash == git_repository.get_object_hash("X")

    # NOTE: Hash of the committed directory is the same as the staged hash
    git_repository.commit("Committed X")

    assert directory_hash == git_repository.get_object_hash("X", revision="HEAD")
    assert directory_hash == git_repository.get_object_hash("X")

    with pytest.raises(errors.GitCommandError):
        Repository.hash_objects(["X"])[0]
