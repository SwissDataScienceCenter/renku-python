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
"""Test os utilities."""

import pytest

from renku.core.util.os import matches


@pytest.mark.parametrize(
    "path, pattern, should_match",
    [
        ["path", "path", True],
        ["path", "path/", True],
        ["path", "path*", True],
        ["path", "pat", False],
        ["/path", "path", False],
        ["path", "/path", False],
        ["path/in/sub-dir", "path", True],
        ["path/in/sub-dir", "path*", True],
        ["path/in/sub-dir", "path/", True],
        ["path/in/sub-dir", "path/*", True],
        ["path/in/sub-dir", "**/sub-dir", True],
        ["path/in/sub-dir", "sub-dir", False],
        ["path/in/sub-dir", "**/in", True],
        ["path/in/sub-dir", "path/some-other-dir", False],
        ["path/in/sub-dir", "path/**/in", False],
        ["path/in/sub-dir", "**/sub-dir/**", False],
        ["path/in/sub-dir", "in", False],
    ],
)
def test_path_match(path, pattern, should_match):
    """Test ``matches`` utility function that checks if a path matches a given pattern."""
    assert matches(path=path, pattern=pattern) is should_match
