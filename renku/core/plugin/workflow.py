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
"""Plugin hooks for renku workflow customization."""

from pathlib import Path
from typing import List, Optional, Tuple

import pluggy

from renku.core import errors
from renku.domain_model.workflow.converters import IWorkflowConverter
from renku.domain_model.workflow.plan import Plan

try:
    from typing_extensions import Protocol  # NOTE: Required for Python 3.7 compatibility
except ImportError:
    from typing import Protocol  # type: ignore


hookspec = pluggy.HookspecMarker("renku")


@hookspec
def workflow_format() -> Tuple[IWorkflowConverter, List[str]]:  # type: ignore[empty-body]
    """Plugin Hook for ``workflow export`` call.

    Can be used to export renku workflows in different formats.

    Returns:
        Tuple[IWorkflowConverter,List[str]]: A tuple of the plugin itself and the output formats it supports.
            A plugin can support multiple formats.

    """
    pass


@hookspec(firstresult=True)
def workflow_convert(  # type: ignore[empty-body]
    workflow: Plan,
    basedir: Path,
    output: Optional[Path],
    output_format: Optional[str],
    resolve_paths: bool,
    nest_workflows: bool,
) -> str:
    """Plugin Hook for ``workflow export`` call.

    Can be used to export renku workflows in different formats.

    Args:
        workflow(Plan): A ``Plan`` object that describes the given workflow.
        basedir(Path): The base output directory.
        output(Optional[Path]): The output file, which will contain the workflow.
        output_format(Optional[str]): Output format supported by the given plugin.

    Returns:
        str: The string representation of the given Plan in the specific
            workflow format.

    """
    pass


def get_supported_formats() -> List[str]:
    """Returns the currently available workflow language format types.

    Returns:
        List of supported export formats.
    """
    from renku.core.plugin.pluginmanager import get_plugin_manager

    pm = get_plugin_manager()
    supported_formats = pm.hook.workflow_format()
    return [format for fs in supported_formats for format in fs[1]]


class WorkflowConverterProtocol(Protocol):
    """Typing protocol to specify type of the workflow converter hook."""

    def __call__(
        self,
        workflow: Plan,
        basedir: Path,
        output: Optional[Path] = None,
        output_format: Optional[str] = None,
        resolve_paths: Optional[bool] = None,
        nest_workflows: Optional[bool] = None,
    ) -> str:
        """Dummy method to let mypy know the type of the hook implementation."""
        raise NotImplementedError()


def workflow_converter(format: str) -> WorkflowConverterProtocol:
    """Returns a workflow converter function for a given format if available.

    Args:
        format(str): The format to convert to.

    Returns:
        The conversion plugin callable.
    """
    from renku.core.plugin.pluginmanager import get_plugin_manager

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
    return pm.subset_hook_caller(name="workflow_convert", remove_plugins=export_plugins)
