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
"""Plugin hooks for workflow file parsers."""

from pathlib import Path
from typing import TYPE_CHECKING, List, Tuple, Union

import pluggy

from renku.core import errors
from renku.core.interface.workflow_file_parser import IWorkflowFileParser

if TYPE_CHECKING:
    from renku.core.workflow.model.workflow_file import WorkflowFile


hookspec = pluggy.HookspecMarker("renku")


@hookspec
def workflow_file_parser() -> Tuple[IWorkflowFileParser, str]:
    """Plugin Hook to get workflow file parsers.

    Returns:
        Tuple[IWorkflowFileParser,str]: A tuple of the parser itself and its name.
    """
    raise NotImplementedError


@hookspec(firstresult=True)
def parse(path: Union[Path, str]) -> "WorkflowFile":
    """Plugin Hook for parsing workflow files.

    Args:
        path(Union[Path, str]): Path to the workflow file to parse.

    Returns:
        WorkflowFile: The parsed workflow file instance.
    """
    raise NotImplementedError


def get_available_workflow_file_parsers() -> List[str]:
    """Returns the currently available workflow file parsers.

    Returns:
        The list of available parsers.
    """
    from renku.core.plugin.pluginmanager import get_plugin_manager

    pm = get_plugin_manager()
    providers = pm.hook.workflow_file_parser()
    return [p[1] for p in providers]


def read_workflow_file(path: Union[Path, str], parser: str = "renku") -> "WorkflowFile":
    """Read a given workflow file using the selected parser.

    Args:
        path(Union[Path, str]): Path to the workflow file.
        parser(str): The workflow parser engine to be used (Default value = "renku").

    Returns:
        WorkflowFile: The parsed workflow file.
    """
    from renku.core.plugin.pluginmanager import get_plugin_manager

    pm = get_plugin_manager()
    parsers = pm.hook.workflow_file_parser()
    selected_parsers = [p for p in parsers if p[1] == parser]

    if not selected_parsers:
        raise errors.ParameterError(f"The specified workflow parser '{parser}' is not available.")
    elif len(parsers) > 1:
        raise errors.ParameterError(f"Multiple parsers found for '{parser}': {selected_parsers}.")

    parsers.remove(selected_parsers[0])
    parse_function = pm.subset_hook_caller("parse", remove_plugins=[p[0] for p in parsers])

    return parse_function(path=path)
