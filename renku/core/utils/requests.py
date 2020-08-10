# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Utility for working with HTTP session."""
from contextlib import contextmanager

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@contextmanager
def retry(total_requests=10, backoff_factor=1, statuses=(500, 502, 503, 504, 429)):
    """Default HTTP session for requests."""
    _session = requests.Session()

    retries = Retry(total=total_requests, backoff_factor=backoff_factor, status_forcelist=list(statuses))

    _session.mount("http://", HTTPAdapter(max_retries=retries))
    _session.mount("https://", HTTPAdapter(max_retries=retries))

    yield _session
