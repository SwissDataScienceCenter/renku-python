from typing import cast

import pytest
from marshmallow.exceptions import ValidationError

from renku.ui.service.cache.projects import ProjectManagementCache, User


@pytest.mark.parametrize(
    "commit_sha,branch,exception",
    [
        (None, None, None),
        ("commit_sha", None, None),
        (None, "branch", None),
        (None, "master", None),
        ("commit_sha", "master", ValidationError),
    ],
)
def test_make_project(mock_redis, commit_sha, branch, exception):
    cache = ProjectManagementCache()
    user = User(user_id="user_id")
    project_data = {
        "slug": "slug",
        "name": "name",
        "owner": "owner",
        "branch": branch,
        "commit_sha": commit_sha,
    }
    if exception is not None:
        with pytest.raises(exception):
            cache.make_project(user=user, project_data=project_data, persist=True)
    else:
        cache.make_project(user=user, project_data=project_data, persist=True)
        projects = list(cache.get_projects(user))
        assert len(projects) == 1
        if commit_sha is None:
            assert projects[0].commit_sha == ""
        else:
            assert projects[0].commit_sha == commit_sha
        if branch is None:
            assert projects[0].branch == ""
        else:
            assert projects[0].branch == branch
