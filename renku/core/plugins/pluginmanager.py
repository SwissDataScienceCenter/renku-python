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
"""pluggy Plugin setup."""
from functools import lru_cache

import pluggy

from renku.core.plugins import implementations as default_implementations
from renku.core.plugins import run as run_hook_specs


@lru_cache(None)
def get_plugin_manager():
    """The ``pluggy`` plugin manager."""
    pm = pluggy.PluginManager('renku')
    pm.add_hookspecs(run_hook_specs)
    pm.load_setuptools_entrypoints('renku')

    for cls in default_implementations.__dict__.values():
        if not isinstance(cls, type):
            continue
        pm.register(cls())
    return pm
