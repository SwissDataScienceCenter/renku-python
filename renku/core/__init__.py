# -*- coding: utf-8 -*-
#
# Copyright 2017-2020- Swiss Data Science Center (SDSC)
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
"""Renku core."""
import logging
import os

import requests
from requests.adapters import TimeoutSauce

RENKU_REQUESTS_TIMEOUT_SECONDS = float(
    os.getenv('RENKU_REQUESTS_TIMEOUT_SECONDS', 1200)
)


class CustomTimeout(TimeoutSauce):
    """CustomTimeout for all HTTP requests."""

    def __init__(self, *args, **kwargs):
        """Construct CustomTimeout."""
        if kwargs['connect'] is None:
            kwargs['connect'] = RENKU_REQUESTS_TIMEOUT_SECONDS

        if kwargs['read'] is None:
            kwargs['read'] = RENKU_REQUESTS_TIMEOUT_SECONDS

        super().__init__(*args, **kwargs)


requests.adapters.TimeoutSauce = CustomTimeout
logging.getLogger('filelock').setLevel(logging.ERROR)
