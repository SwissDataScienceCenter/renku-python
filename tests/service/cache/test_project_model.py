import pytest

from renku.ui.service.cache.models.project import DETACHED_HEAD_FOLDER_PREFIX, NO_BRANCH_FOLDER, Project


@pytest.mark.parametrize(
    "commit_sha,branch,expected_folder",
    [
        (None, None, NO_BRANCH_FOLDER),
        ("commit_sha", None, f"{DETACHED_HEAD_FOLDER_PREFIX}commit_sha"),
        (None, "branch", "branch"),
        (None, "master", "master"),
    ],
)
def test_project_model_path(commit_sha, branch, expected_folder):
    project = Project(name="name", slug="slug", commit_sha=commit_sha, user_id="user_id", owner="owner", branch=branch)
    assert project.abs_path.stem == expected_folder
