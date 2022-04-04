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
"""Renku service cache configuration."""
import os

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DATABASE = int(os.getenv("REDIS_DATABASE", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

REDIS_NAMESPACE = os.getenv("REDIS_NAMESPACE")

REDIS_IS_SENTINEL = os.environ.get("REDIS_IS_SENTINEL", "") == "true"
REDIS_MASTER_SET = os.environ.get("REDIS_MASTER_SET", "mymaster")
if REDIS_IS_SENTINEL:
    from redis.sentinel import Sentinel

    sentinel = Sentinel(
        [(REDIS_HOST, REDIS_PORT)],
        sentinel_kwargs={"password": REDIS_PASSWORD},
    )
    REDIS_HOST, REDIS_PORT = sentinel.discover_master(REDIS_MASTER_SET)
