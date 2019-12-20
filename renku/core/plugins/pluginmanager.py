# -*- coding: utf-8 -*-
#
# Copyright 2017-2019- Swiss Data Science Center (SDSC)
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
import pluggy

import renku.core.plugins.process_run as process_run_hook_specs
from renku.core.plugins.implementations.cmdline_tool_annotations import CmdlineToolAnnotations


def get_plugin_manager():
    """The ``pluggy`` plugin manager."""
    pm = pluggy.PluginManager('renku')
    pm.add_hookspecs(process_run_hook_specs)
    pm.load_setuptools_entrypoints('renku')
    pm.register(CmdlineToolAnnotations())
    return pm
