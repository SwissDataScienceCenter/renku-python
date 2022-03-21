# -*- coding: utf-8 -*-
#
# Copyright 2017-2021- Swiss Data Science Center (SDSC)
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
"""Renku plugin implementations."""

from renku.core.management.session.docker import DockerSessionProvider
from renku.core.management.workflow.converters.cwl import CWLExporter
from renku.core.management.workflow.providers.cwltool import CWLToolProvider

__all__ = []

session_providers = [DockerSessionProvider]
workflow_exporters = [CWLExporter]
workflow_providers = [CWLToolProvider]

try:
    from renku.core.management.workflow.providers.toil import ToilProvider

    workflow_providers.append(ToilProvider)
except ImportError:
    pass
