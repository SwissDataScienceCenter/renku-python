# -*- coding: utf-8 -*-
#
# Copyright 2018-2021- Swiss Data Science Center (SDSC)
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
"""Git regex tests."""

import os

import pytest

from renku.core import errors
from renku.core.models.git import GitURL


@pytest.mark.parametrize(
    "fields",
    [
        {
            "href": "https://example.com/repo.git",
            "protocol": "https",
            "hostname": "example.com",
            "name": "repo",
            "pathname": "repo.git",
        },
        {
            "href": "https://example.com/repo",
            "protocol": "https",
            "hostname": "example.com",
            "name": "repo",
            "pathname": "repo",
        },
        {
            "href": "https://example.com/owner/repo.git",
            "protocol": "https",
            "hostname": "example.com",
            "name": "repo",
            "pathname": "owner/repo.git",
            "owner": "owner",
        },
        {
            "href": "https://example.com:1234/repo.git",
            "protocol": "https",
            "hostname": "example.com",
            "name": "repo",
            "pathname": "repo.git",
            "port": "1234",
        },
        {
            "href": "https://example.com:1234/owner/repo.git",
            "protocol": "https",
            "hostname": "example.com",
            "name": "repo",
            "pathname": "owner/repo.git",
            "owner": "owner",
            "port": "1234",
        },
        {
            "href": "https://example.com:1234/gitlab/owner/repo.git",
            "protocol": "https",
            "hostname": "example.com",
            "name": "repo",
            "pathname": "gitlab/owner/repo.git",
            "owner": "owner",
            "port": "1234",
        },
        {
            "href": "https://example.com:1234/gitlab/group/owner/repo.git",
            "protocol": "https",
            "hostname": "example.com",
            "name": "repo",
            "pathname": "gitlab/group/owner/repo.git",
            "owner": "group/owner",
            "port": "1234",
        },
        {
            "href": "https://example.com/gro.up/owner.name/repo.name.git",
            "protocol": "https",
            "hostname": "example.com",
            "pathname": "gro.up/owner.name/repo.name.git",
            "owner": "gro.up/owner.name",
            "name": "repo.name",
        },
        {
            "href": "git+https://example.com:1234/owner/repo.git",
            "protocol": "git+https",
            "hostname": "example.com",
            "name": "repo",
            "pathname": "owner/repo.git",
            "owner": "owner",
            "port": "1234",
        },
        {
            "href": "git+ssh://example.com:1234/owner/repo.git",
            "protocol": "git+ssh",
            "hostname": "example.com",
            "name": "repo",
            "pathname": "owner/repo.git",
            "owner": "owner",
            "port": "1234",
        },
        {
            "href": "git+ssh://user:pass@example.com:1234/owner/repo.git",
            "protocol": "git+ssh",
            "hostname": "example.com",
            "name": "repo",
            "pathname": "owner/repo.git",
            "owner": "owner",
            "port": "1234",
            "username": "user",
            "password": "pass",
        },
        {
            "href": "ssh://user:pass@example.com/~user/owner/repo.git",
            "protocol": "ssh",
            "hostname": "example.com",
            "name": "repo",
            "pathname": "~user/owner/repo.git",
            "owner": "owner",
            "username": "user",
            "password": "pass",
        },
        pytest.param(
            {
                "href": "git@example.com/repo.git",
                "protocol": "ssh",
                "hostname": "example.com",
                "name": "repo",
                "pathname": "repo.git",
                "username": "git",
            },
            marks=pytest.mark.xfail(raises=errors.ConfigurationError, strict=True),
        ),
        pytest.param(
            {
                "href": "git@example.com/owner/repo.git",
                "protocol": "ssh",
                "hostname": "example.com",
                "name": "repo",
                "pathname": "owner/repo.git",
                "owner": "owner",
                "username": "git",
            },
            marks=pytest.mark.xfail(raises=errors.ConfigurationError, strict=True),
        ),
        {
            "href": "git@example.com:repo.git",
            "protocol": "ssh",
            "hostname": "example.com",
            "name": "repo",
            "pathname": "repo.git",
            "username": "git",
        },
        {
            "href": "git@example.com:owner/repo.git",
            "protocol": "ssh",
            "hostname": "example.com",
            "name": "repo",
            "pathname": "owner/repo.git",
            "owner": "owner",
            "username": "git",
        },
        {
            "href": "git@example.com:prefix/owner/repo.git",
            "protocol": "ssh",
            "hostname": "example.com",
            "name": "repo",
            "pathname": "prefix/owner/repo.git",
            "owner": "prefix/owner",
            "username": "git",
        },
        {"href": "/path/to/repo", "pathname": "/path/to/repo",},
        {"href": "file:///path/to/repo", "pathname": "/path/to/repo",},
        {"href": "../relative/path/to/repo", "pathname": "../relative/path/to/repo",},
        {"href": "file://../relative/path/to/repo", "pathname": "../relative/path/to/repo",},
        pytest.param(
            {
                "href": "https://example.com:1234:repo.git",
                "protocol": "https",
                "hostname": "example.com",
                "port": "1234",
                "name": "repo",
                "pathname": "repo.git",
            },
            marks=pytest.mark.xfail(raises=errors.ConfigurationError, strict=True),
        ),
        pytest.param(
            {
                "href": "https://example.com:1234:owner/repo.git",
                "protocol": "https",
                "hostname": "example.com",
                "port": "1234",
                "name": "repo",
                "pathname": "repo.git",
                "owner": "owner",
            },
            marks=pytest.mark.xfail(raises=errors.ConfigurationError, strict=True),
        ),
        pytest.param(
            {
                "href": "git@example.com:1234:owner/repo.git",
                "protocol": "ssh",
                "hostname": "example.com",
                "port": "1234",
                "name": "repo",
                "pathname": "repo.git",
                "owner": "owner",
            },
            marks=pytest.mark.xfail(raises=errors.ConfigurationError, strict=True),
        ),
        {
            "href": "git@example.com:1234/prefix/owner/repo.git",
            "username": "git",
            "protocol": "ssh",
            "hostname": "example.com",
            "name": "repo",
            "pathname": "1234/prefix/owner/repo.git",
            "owner": "1234/prefix/owner",
        },
        {
            "href": "https://example.com:1234/gitlab/owner/repo.git",
            "protocol": "https",
            "hostname": "example.com",
            "name": "repo",
            "pathname": "gitlab/owner/repo.git",
            "owner": "owner",
            "port": "1234",
            "env": "https://example.com:1234/gitlab/",
        },
        {
            "href": "https://example.com:1234/gitlab/owner/repo.git",
            "protocol": "https",
            "hostname": "example.com",
            "name": "repo",
            "pathname": "gitlab/owner/repo.git",
            "owner": "owner",
            "port": "1234",
            "env": "https://example.com/gitlab/",
        },
        {
            "href": "https://example.com/gitlab/owner/repo.git",
            "protocol": "https",
            "hostname": "example.com",
            "name": "repo",
            "pathname": "gitlab/owner/repo.git",
            "owner": "owner",
            "env": "https://example.com/gitlab/",
        },
        {
            "href": "https://gitlab.example.com/owner/repo.git",
            "protocol": "https",
            "hostname": "gitlab.example.com",
            "name": "repo",
            "pathname": "owner/repo.git",
            "owner": "owner",
            "env": "https://gitlab.example.com/",
        },
        {
            "href": "https://gitlab.example.com:1234/owner/repo.git",
            "protocol": "https",
            "hostname": "gitlab.example.com",
            "name": "repo",
            "pathname": "owner/repo.git",
            "owner": "owner",
            "port": "1234",
            "env": "https://gitlab.example.com:1234/",
        },
    ],
)
def test_valid_href(fields):
    """Test the various repo regexes."""
    fields.pop("protocols", None)
    gitlab_env = fields.pop("env", None)

    if gitlab_env:
        os.environ["GITLAB_BASE_URL"] = gitlab_env

    assert GitURL(**fields) == GitURL.parse(fields["href"])
