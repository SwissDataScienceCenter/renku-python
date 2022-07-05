# -*- coding: utf-8 -*-
#
# Copyright 2018-2022- Swiss Data Science Center (SDSC)
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
"""Utility functions for plugins."""


def supported_formats():
    """Deferred import as plugins are slow."""
    from renku.core.plugin.workflow import supported_formats

    return supported_formats()


def available_workflow_providers():
    """Deferred import as plugins are slow."""
    from renku.core.plugin.provider import available_workflow_providers

    return available_workflow_providers()


def get_supported_session_providers_names():
    """Deferred import as plugins are slow."""
    from renku.core.plugin.session import get_supported_session_providers

    return [p.get_name() for p in get_supported_session_providers()]
