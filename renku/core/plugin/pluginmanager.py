# -*- coding: utf-8 -*-
#
# Copyright 2017-2022- Swiss Data Science Center (SDSC)
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


@lru_cache(None)
def get_plugin_manager():
    """The ``pluggy`` plugin manager."""
    from renku.core.plugin import dataset_provider
    from renku.core.plugin import implementations as default_implementations
    from renku.core.plugin import provider as provider_hook_specs
    from renku.core.plugin import run as run_hook_specs
    from renku.core.plugin import workflow as workflow_hook_specs
    from renku.core.plugin import workflow_file_parser

    pm = pluggy.PluginManager("renku")
    pm.add_hookspecs(dataset_provider)
    pm.add_hookspecs(provider_hook_specs)
    pm.add_hookspecs(run_hook_specs)
    pm.add_hookspecs(workflow_file_parser)
    pm.add_hookspecs(workflow_hook_specs)
    pm.load_setuptools_entrypoints("renku")

    for cls in default_implementations.__dict__.values():
        if not isinstance(cls, type):
            continue
        try:
            pm.register(cls())
        except TypeError:
            pm.register(cls)

    return pm
