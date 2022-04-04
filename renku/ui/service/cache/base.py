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
"""Renku service cache management."""
import json
import os

import redis
from redis import RedisError
from walrus import Database

from renku.ui.service.cache.config import REDIS_DATABASE, REDIS_HOST, REDIS_NAMESPACE, REDIS_PASSWORD, REDIS_PORT


class BaseCache:
    """Cache management."""

    config_ = {
        "host": REDIS_HOST,
        "port": REDIS_PORT,
        "db": REDIS_DATABASE,
        "password": REDIS_PASSWORD,
        "retry_on_timeout": True,
        "health_check_interval": int(os.getenv("CACHE_HEALTH_CHECK_INTERVAL", 60)),
    }

    cache = redis.Redis(**config_)  # type: ignore
    model_db = Database(**config_)
    namespace = REDIS_NAMESPACE

    def set_record(self, name, key, value):
        """Insert a record to hash set."""
        if isinstance(value, dict):
            value = json.dumps(value)

        self.cache.hset(name, key, value)

    def invalidate_key(self, name, key):
        """Invalidate a cache `key` in users hash set."""
        try:
            self.cache.hdel(name, key)
        except RedisError:
            pass

    def get_record(self, name, key):
        """Return record values from hash set."""
        result = self.cache.hget(name, key)
        if result:
            return json.loads(result.decode("utf-8"))

    def get_all_records(self, name):
        """Return all record values from hash set."""
        return [json.loads(record.decode("utf-8")) for record in self.cache.hgetall(name).values()]

    def scan_iter(self, pattern):
        """Scan keys to return all user cached elements."""
        return self.cache.scan_iter(match=pattern)

    def hash_table(self, hash_table):
        """Return hash table."""
        return self.cache.hgetall(hash_table)
