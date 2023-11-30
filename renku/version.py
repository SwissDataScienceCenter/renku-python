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
"""Version information for Renku."""

import re
from typing import Optional, cast

try:
    from importlib.metadata import distribution, version
except ImportError:
    from importlib_metadata import distribution, version  # type: ignore

__version__ = cast(str, version("renku"))
__template_version__ = "0.7.1"
__minimum_project_version__ = "2.8.0"


def is_release(version: Optional[str] = None):
    """Check if current version is a release semver."""
    if not version:
        version = __version__

    if re.match(r"\d+.\d+.\d+(rc\d+)?$", version):
        return True
    return False


def _get_distribution_url():
    try:
        url = distribution("renku").metadata["Home-page"]
    except Exception:
        url = None

    return "https://github.com/swissdatasciencecenter/renku-python" if not url else url


version_url = f"{_get_distribution_url()}/tree/v{__version__}"
