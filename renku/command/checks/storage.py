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
"""Check for large files in Git history."""

from renku.command.util import WARNING
from renku.core.lfs import check_external_storage, check_lfs_migrate_info


def check_lfs_info(**_):
    """Checks if files in history should be in LFS.

    Args:
        _: keyword arguments.

    Returns:
        Tuple of whether project structure is valid, if an automated fix is available and string of found problems.
    """
    if not check_external_storage():
        return True, False, None

    files = check_lfs_migrate_info()

    if not files:
        return True, False, None

    message = (
        WARNING
        + "Git history contains large files - consider moving them "
        + "to external storage like git LFS using 'renku storage migrate'\n\t"
        + "\n\t".join(files)
        + "\n"
    )

    return False, False, message
