# -*- coding: utf-8 -*-
#
# Copyright 2017 Swiss Data Science Center
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
"""Utility functions."""

import time
from functools import wraps


def decode_bytes(func):
    """Function wrapper that always returns string."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        res = func()
        if isinstance(res, str):
            return res
        else:
            return res.decode()

    return wrapper


def resource_available(func):
    """
    Function wrapper to catch that something is not available.

    Example:

    while not resource_available(get_logs()):
        # this loop continues until the logs are available
        pass

    logs = get_logs()

    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            time.sleep(0.2)
            return False

    return wrapper


def join_url(*args):
    """Join together url strings."""
    return '/'.join(s.strip('/') for s in args)
