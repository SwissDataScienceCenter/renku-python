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
"""Plugin hooks for renku workflow customization."""
from pathlib import Path
from typing import Callable, List, Optional, Tuple

import pluggy

from renku.core import errors
from renku.core.models.workflow.converters import IWorkflowConverter
from renku.core.models.workflow.plan import Plan

hookspec = pluggy.HookspecMarker("renku")


@hookspec
def workflow_format() -> Tuple[IWorkflowConverter, List[str]]:
    """Plugin Hook for ``workflow export`` call.

    Can be used to export renku workflows in different formats.

    :returns: A tuple of the plugin itself and the output formats it supports.
              NOTE: a plugin can support multiple formats.
    """
    pass


@hookspec(firstresult=True)
def workflow_convert(workflow: Plan, basedir: Path, output: Optional[Path], output_format: Optional[str]) -> str:
    """Plugin Hook for ``workflow export`` call.

    Can be used to export renku workflows in different formats.

    :param workflow: A ``Plan`` object that describes the given workflow.
    :param basedir: .
    :param output: The output file, which will contain the workflow.
    :param output_format: Output format supported by the given plugin.
    :returns: The string representation of the given Plan in the specific
              workflow format.
    """
    pass


def supported_formats() -> List[str]:
    """Returns the currently available workflow language format types."""
    from renku.core.plugins.pluginmanager import get_plugin_manager

    pm = get_plugin_manager()
    supported_formats = pm.hook.workflow_format()
    return [format for fs in supported_formats for format in fs[1]]


def workflow_converter(format: str) -> Callable[[Plan, Path, Optional[Path], Optional[str]], str]:
    """Returns a workflow converter function for a given format if available."""
    from renku.core.plugins.pluginmanager import get_plugin_manager

    pm = get_plugin_manager()
    supported_formats = pm.hook.workflow_format()
    export_plugins = list(map(lambda x: x[0], supported_formats))
    converter = list(map(lambda x: x[0], filter(lambda x: format in x[1], supported_formats)))
    if not any(converter):
        raise errors.ParameterError(f"The specified workflow exporter format '{format}' is not available.")
    elif len(converter) > 1:
        raise errors.ConfigurationError(
            f"The specified format '{format}' is supported by more than one export plugins!"
        )
    export_plugins.remove(converter[0])
    return pm.subset_hook_caller("workflow_convert", export_plugins)
