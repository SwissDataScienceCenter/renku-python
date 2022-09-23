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
"""Renku management dataset request models."""

import imghdr
import os
import shutil
import urllib
from pathlib import Path
from typing import List, Optional, Union, cast
from urllib.request import urlretrieve

from renku.core import errors
from renku.domain_model.dataset import Dataset, ImageObject
from renku.domain_model.project_context import project_context


class ImageRequestModel:
    """Model for passing image information to dataset use-cases."""

    def __init__(
        self,
        content_url: str,
        position: int,
        mirror_locally: bool = False,
        safe_image_paths: Optional[List[str]] = None,
    ) -> None:
        self.content_url = content_url
        self.position = position
        self.mirror_locally = mirror_locally
        self.safe_image_paths: List[Union[str, Path]] = cast(List[Union[str, Path]], safe_image_paths) or []

    def to_image_object(self, dataset: Dataset) -> ImageObject:
        """Convert request model to ``ImageObject``."""
        image_type = None
        self.safe_image_paths.append(project_context.path)

        image_folder = project_context.dataset_images_path / dataset.initial_identifier
        image_folder.mkdir(exist_ok=True, parents=True)

        if urllib.parse.urlparse(self.content_url).netloc:
            # NOTE: absolute url
            if not self.mirror_locally:
                return ImageObject(
                    content_url=self.content_url,
                    position=self.position,
                    id=ImageObject.generate_id(dataset_id=dataset.id, position=self.position),
                )

            # NOTE: mirror the image locally
            try:
                path, _ = urlretrieve(self.content_url)
            except urllib.error.URLError as e:
                raise errors.DatasetImageError(f"Dataset image with url {self.content_url} couldn't be mirrored") from e

            image_type = imghdr.what(path)
            if image_type:
                image_type = f".{image_type}"

            self.content_url = path
            self.safe_image_paths.append(Path(path).parent)

        path = self.content_url
        if not os.path.isabs(path):
            path = os.path.normpath(os.path.join(project_context.path, path))

        if not os.path.exists(path) or not any(
            os.path.commonprefix([path, p]) == str(p) for p in self.safe_image_paths
        ):
            # NOTE: make sure files exists and prevent path traversal
            raise errors.DatasetImageError(f"Dataset image with relative path {self.content_url} not found")

        if not path.startswith(str(image_folder)):
            # NOTE: only copy dataset image if it's not in .renku/datasets/<id>/images/ already
            if image_type:
                ext = image_type
            else:
                _, ext = os.path.splitext(self.content_url)

            img_path = image_folder / f"{self.position}{ext}"
            shutil.copy(path, img_path)
        else:
            img_path = Path(path)

        return ImageObject(
            content_url=str(img_path.relative_to(project_context.path)),
            position=self.position,
            id=ImageObject.generate_id(dataset_id=dataset.id, position=self.position),
        )
