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
"""Project view model."""

import json
from datetime import datetime
from typing import List, Optional

from renku.domain_model.project import Project
from renku.domain_model.provenance.agent import Person


class ProjectViewModel:
    """A view model for a ``Project``."""

    def __init__(
        self,
        id: str,
        name: Optional[str],
        creator: Person,
        created: datetime,
        description: Optional[str],
        agent: Optional[str],
        annotations: Optional[str],
        template_info: str,
        keywords: Optional[List[str]],
    ):
        self.id = id
        self.name = name
        self.creator = creator
        self.creator_str = creator.full_identity
        self.created = created
        self.created_str = created.isoformat()
        self.description = description
        self.agent = agent
        self.annotations = annotations
        self.template_info = template_info
        self.keywords = keywords
        self.keywords_str = ", ".join(keywords) if keywords else ""

    @classmethod
    def from_project(cls, project: Project):
        """Create view model from ``Project``.

        Args:
            project(Project): The project to convert.

        Returns:
            View model for project.
        """
        template_info = ""

        if project.template_metadata.template_source:
            if project.template_metadata.template_source == "renku":
                template_info = (
                    f"{project.template_metadata.template_id} ({project.template_metadata.template_version})"
                )
            else:
                template_info = (
                    f"{project.template_metadata.template_source}@"
                    f"{project.template_metadata.template_ref}: {project.template_metadata.template_id}"
                )

        return cls(
            id=project.id,
            name=project.name,
            creator=project.creator,
            created=project.date_created,
            description=project.description,
            agent=project.agent_version,
            annotations=json.dumps([{"id": a.id, "body": a.body, "source": a.source} for a in project.annotations])
            if project.annotations
            else None,
            template_info=template_info,
            keywords=project.keywords,
        )
