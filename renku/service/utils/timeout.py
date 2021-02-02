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
"""Utilities for renku service controllers."""
import signal

from renku.service.errors import RenkuOpTimeoutError


def timeout(fn, fn_args=None, fn_kwargs=None, timeout_duration=3600, default=None):
    """Execute provided function or timeout."""
    fn_args = fn_args or ()
    fn_kwargs = fn_kwargs or {}

    def signal_handler(signum, frame):
        raise RenkuOpTimeoutError()

    # NOTE: Set the timeout handler.
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(timeout_duration)

    try:
        result = fn(*fn_args, **fn_kwargs)
    except RenkuOpTimeoutError:
        result = default
    finally:
        signal.alarm(0)

    return result
