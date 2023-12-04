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
"""Renku project/dataset image management."""

import os
import urllib
from pathlib import Path
from typing import List, Optional, Union, cast
from urllib.request import urlretrieve

import filetype

from renku.core import errors
from renku.core.constant import FILESYSTEM_ROOT
from renku.core.util.os import is_subpath
from renku.core.util.urls import is_remote
from renku.domain_model.image import ImageObject
from renku.domain_model.project_context import project_context


class ImageObjectRequest:
    """Model for passing image information."""

    def __init__(
        self,
        content_url: str,
        position: int = 0,
        mirror_locally: bool = True,
        safe_image_paths: Optional[List[str]] = None,
    ) -> None:
        self.content_url = content_url
        self.position = position
        self.mirror_locally = mirror_locally
        self.safe_image_paths: List[Union[str, Path]] = cast(List[Union[str, Path]], safe_image_paths) or []

    def to_image_object(self, owner_id: str) -> ImageObject:
        """Convert request model to ``ImageObject`` and download the image if requested and return its path."""
        self.safe_image_paths.append(project_context.path)

        if is_remote(self.content_url):
            if not self.mirror_locally:
                return ImageObject(
                    content_url=self.content_url,
                    position=self.position,
                    id=ImageObject.generate_id(owner_id=owner_id, position=self.position),
                )

            # NOTE: Download the image
            try:
                tmp_path, _ = urlretrieve(self.content_url)
            except urllib.error.URLError as e:
                raise errors.ImageError(f"Cannot download image with url {self.content_url}: {e}") from e

            path = Path(tmp_path)
        else:
            path = Path(self.content_url).resolve()

            if not os.path.exists(path):
                raise errors.ImageError(f"Image with local path '{self.content_url}' not found")
            # NOTE: Prevent path traversal or usage of non-image files
            elif (FILESYSTEM_ROOT in self.safe_image_paths and filetype.guess(path) is None) or not any(
                is_subpath(path, base=p) for p in self.safe_image_paths
            ):
                raise errors.ImageError(f"'{self.content_url}' isn't a valid image file")

        return ImageObject(
            content_url=path.as_posix(),
            position=self.position,
            id=ImageObject.generate_id(owner_id=owner_id, position=self.position),
        )
