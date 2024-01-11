# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
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

import os
import shutil
from typing import Dict, List, Optional, Union, cast

from pydantic import ConfigDict, validate_call

from renku.command.command_builder import inject
from renku.command.view_model.project import ProjectViewModel
from renku.core import errors
from renku.core.image import ImageObjectRequest
from renku.core.interface.project_gateway import IProjectGateway
from renku.core.util.metadata import construct_creator
from renku.core.util.os import get_relative_path
from renku.domain_model.constant import NO_VALUE, NoValueType
from renku.domain_model.dataset import ImageObjectRequestJson
from renku.domain_model.project_context import project_context
from renku.domain_model.provenance.agent import Person


@inject.autoparams()
@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def edit_project(
    description: Optional[Union[str, NoValueType]],
    creator: Union[Dict, str, NoValueType],
    keywords: Optional[Union[List[str], NoValueType]],
    custom_metadata: Optional[Union[Dict, List[Dict], NoValueType]],
    custom_metadata_source: Optional[Union[str, NoValueType]],
    image_request: Optional[Union[ImageObjectRequest, NoValueType]],
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
        image_request(Optional[ImageObjectRequest]): Project's image.

    Returns:
        Tuple of fields that were updated and dictionary of warnings.
    """
    possible_updates = {
        "creator": creator,
        "description": description,
        "keywords": keywords,
        "custom_metadata": custom_metadata,
        "image": (
            image_request
            if image_request is NO_VALUE or image_request is None
            else ImageObjectRequestJson().dump(image_request)
        ),
    }

    no_email_warnings: Optional[Union[Dict, str]] = None
    parsed_creator: Optional[Union[NoValueType, Person]] = NO_VALUE

    if creator is not NO_VALUE:
        parsed_creator, no_email_warnings = construct_creator(cast(Union[Dict, str], creator), ignore_email=True)

    if image_request is None:
        delete_project_image()
    elif image_request is not NO_VALUE:
        set_project_image(image_request=image_request)  # type: ignore

    updated = {k: v for k, v in possible_updates.items() if v is not NO_VALUE}

    if updated:
        project = project_gateway.get_project()
        # NOTE: No need to pass ``image`` here since we already copied/deleted the file and updated the project
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


def set_project_image(image_request: Optional[ImageObjectRequest]) -> None:
    """Download and set a project's images.

    Args:
        image_request(Optional[ImageObjectRequest]): The image to set.
    """
    if image_request is None:
        return

    # NOTE: Projects can have maximum one image
    image_request.position = 0

    image_object = image_request.to_image_object(owner_id=project_context.project.id)

    project_image = project_context.project_image_pathname

    # NOTE: Do nothing if the new path is the same as the old one
    if project_image.resolve() != image_object.content_url:
        # NOTE: Always delete the old image in case the image wasn't mirrored in the project
        delete_project_image()

        if not image_object.is_remote:
            project_image.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(image_object.content_url, project_context.project_image_pathname)

            image_object.content_url = get_relative_path(project_image, base=project_context.path)  # type: ignore

    project_context.project.image = image_object


def delete_project_image() -> None:
    """Delete project image in a project."""
    try:
        os.remove(project_context.project_image_pathname)
    except FileNotFoundError:
        pass
    except OSError as e:
        raise errors.ImageError(f"Cannot delete project image '{project_context.project_image_pathname}': {e}") from e
    else:
        project_context.project.image = None
