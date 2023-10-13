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
"""Image model."""

from pathlib import Path
from typing import Union
from urllib.parse import urlparse

from renku.core.util.urls import get_path, is_remote
from renku.infrastructure.immutable import Slots


class ImageObject(Slots):
    """Represents a schema.org ``ImageObject``."""

    __slots__ = ("content_url", "id", "position")

    id: str
    content_url: str
    position: int

    def __init__(self, *, content_url: Union[str, Path], id: str, position: int):
        id = get_path(id)
        super().__init__(content_url=str(content_url), position=position, id=id)

    @staticmethod
    def generate_id(owner_id: str, position: int) -> str:
        """Generate @id field."""
        return f"{owner_id}/images/{position}"

    @property
    def is_absolute(self):
        """Whether content_url is an absolute or relative url."""
        return bool(urlparse(self.content_url).netloc)

    @property
    def is_remote(self) -> bool:
        """Return True if the URI isn't on the local filesystem."""
        return is_remote(self.content_url)
