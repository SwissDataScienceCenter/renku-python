# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
"""Test various utilities."""

import os

from renku.core.utils.urls import get_host


def test_hostname():
    """Test host is set correctly in a different Renku domain."""
    renku_domain = os.environ.get("RENKU_DOMAIN")
    try:
        os.environ["RENKU_DOMAIN"] = "alternative-domain"

        assert "alternative-domain" == get_host(None)
    finally:
        if renku_domain:
            os.environ["RENKU_DOMAIN"] = renku_domain
        else:
            del os.environ["RENKU_DOMAIN"]
