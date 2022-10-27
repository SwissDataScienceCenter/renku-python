# -*- coding: utf-8 -*-
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
"""Version information for Renku."""

import re

try:
    from importlib.metadata import distribution
except ImportError:
    from importlib_metadata import distribution  # type: ignore

__version__ = "1.8.1"
__template_version__ = "0.3.1"
__minimum_project_version__ = "1.7.0"


def is_release():
    """Check if current version is a release semver."""
    if re.match(r"\d+.\d+.\d+$", __version__):
        return True
    return False


def _get_distribution_url():
    try:
        d = distribution("renku")
        return d.metadata["Home-page"]
    except Exception:
        return "https://github.com/swissdatasciencecenter/renku-python"


version_url = f"{_get_distribution_url()}/tree/v{__version__}"
