# -*- coding: utf-8 -*-
#
# Copyright 2017-2019 - Swiss Data Science Center (SDSC)
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
"""Get and set Renku repository or global options."""
from renku.core.management.config import CONFIG_LOCAL_PATH

from .client import pass_local_client


def _split_section_and_key(key):
    """Return a tuple with config section and key."""
    parts = key.split('.')
    if len(parts) > 1:
        return 'renku "{0}"'.format(parts[0]), '.'.join(parts[1:])
    return 'renku', key


@pass_local_client(clean=False, commit=True, commit_only=CONFIG_LOCAL_PATH)
def update_config(client, key, value, local_only, global_only):
    """Manage configuration options."""
    is_write = value is not None
    section, key = _split_section_and_key(key)
    if is_write:
        client.set_value(section, key, value, global_only=global_only)
        return value
    else:
        return client.get_value(
            section, key, local_only=local_only, global_only=global_only
        )
