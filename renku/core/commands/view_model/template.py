# -*- coding: utf-8 -*-
#
# Copyright 2017-2021 - Swiss Data Science Center (SDSC)
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
"""Template view model."""

from typing import List, Optional

from renku.core.management.template.template import FileAction, RenderedTemplate
from renku.core.models.template import SourceTemplate, TemplateParameter


class TemplateViewModel:
    """A view model for a ``Template``."""

    def __init__(
        self,
        description: str,
        icon: Optional[str],
        id: str,
        immutable_files: Optional[List],
        name: str,
        reference: Optional[str],
        source: str,
        variables: List[TemplateParameter],
        version: str,
        versions: List[str],
    ):
        self.description: str = description
        self.icon = icon
        self.id: str = id
        self.immutable_files: List[str] = immutable_files
        self.name: str = name
        self.reference = reference
        self.source = source
        self.variables: List[TemplateParameter] = variables
        self.version = version
        self.versions = versions

    @classmethod
    def from_template(cls, template: SourceTemplate) -> "TemplateViewModel":
        """Create view model from ``Template``."""
        return cls(
            source=template.source,
            reference=template.reference,
            version=template.version,
            id=template.id,
            name=template.name,
            description=template.description,
            variables=template.variables,
            icon=template.icon,
            immutable_files=template.immutable_files,
            versions=template.get_all_versions(),
        )


class TemplateChangeViewModel:
    """A view model for resulting changes from a template set/update."""

    def __init__(
        self,
        id: str,
        source: str,
        reference: Optional[str],
        version: str,
        appends: List[str],
        creates: List[str],
        deletes: List[str],
        keeps: List[str],
        overwrites: List[str],
    ):
        self.id: str = id
        self.source = source
        self.reference = reference
        self.version = version
        self.appends = appends
        self.creates = creates
        self.deletes = deletes
        self.keeps = keeps
        self.overwrites = overwrites

    @classmethod
    def from_template(cls, template: RenderedTemplate) -> "TemplateChangeViewModel":
        """Create view model from ``Template``."""
        appends = [k for k, v in template.actions.items() if v == FileAction.APPEND]
        creates = [k for k, v in template.actions.items() if v in (FileAction.CREATE, FileAction.RECREATE)]
        deletes = [k for k, v in template.actions.items() if v == FileAction.DELETED]
        keeps = [
            k
            for k, v in template.actions.items()
            if v in (FileAction.IGNORE_IDENTICAL, FileAction.IGNORE_UNCHANGED_REMOTE, FileAction.KEEP)
        ]
        overwrites = [k for k, v in template.actions.items() if v == FileAction.OVERWRITE]

        return cls(
            id=template.source_template.id,
            source=template.source_template.source,
            reference=template.source_template.reference,
            version=template.source_template.version,
            appends=appends,
            creates=creates,
            deletes=deletes,
            keeps=keeps,
            overwrites=overwrites,
        )
