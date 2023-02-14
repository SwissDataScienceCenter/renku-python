# -*- coding: utf-8 -*-
#
# Copyright 2017-2022 - Swiss Data Science Center (SDSC)
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
"""Project business logic."""

from typing import Dict, List, Optional, Union, cast

from pydantic import validate_arguments

from renku.command.command_builder import inject
from renku.command.view_model.project import ProjectViewModel
from renku.core.interface.project_gateway import IProjectGateway
from renku.core.util.metadata import construct_creator
from renku.core.util.util import NO_VALUE, NoValueType
from renku.domain_model.project_context import project_context
from renku.domain_model.provenance.agent import Person


@inject.autoparams()
@validate_arguments(config=dict(arbitrary_types_allowed=True))
def edit_project(
    description: Optional[Union[str, NoValueType]],
    creator: Union[Dict, str, NoValueType],
    keywords: Optional[Union[List[str], NoValueType]],
    custom_metadata: Optional[Union[Dict, List[Dict], NoValueType]],
    custom_metadata_source: Optional[Union[str, NoValueType]],
    project_gateway: IProjectGateway,
):
    """Edit dataset metadata.

    Args:
        description(Union[Optional[str], NoValueType]): New description.
        creator(Union[Dict, str, NoValueType]): New creators.
        keywords(Union[Optional[List[str]]): New keywords.
        custom_metadata(Union[Optional[Dict, List[Dict]]): Custom JSON-LD metadata.
        custom_metadata_source(Optional[str]): Custom metadata source.
        project_gateway(IProjectGateway): Injected project gateway.

    Returns:
        Tuple of fields that were updated and dictionary of warnings.
    """
    possible_updates = {
        "creator": creator,
        "description": description,
        "keywords": keywords,
        "custom_metadata": custom_metadata,
    }

    no_email_warnings: Optional[Union[Dict, str]] = None
    parsed_creator: Optional[Union[NoValueType, Person]] = NO_VALUE

    if creator is not NO_VALUE:
        parsed_creator, no_email_warnings = construct_creator(cast(Union[Dict, str], creator), ignore_email=True)

    updated = {k: v for k, v in possible_updates.items() if v is not NO_VALUE}

    if updated:
        project = project_gateway.get_project()
        project.update_metadata(
            creator=parsed_creator,
            description=description,
            keywords=keywords,
            custom_metadata=custom_metadata,
            custom_metadata_source=custom_metadata_source,
        )
        project_gateway.update_project(project)

    return updated, no_email_warnings


def show_project() -> ProjectViewModel:
    """Show project metadata.

    Returns:
        Project view model.
    """
    return ProjectViewModel.from_project(project_context.project)
