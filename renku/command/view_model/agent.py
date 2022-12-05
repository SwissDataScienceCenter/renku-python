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
"""Agent view model."""


from typing import Optional

from renku.domain_model.provenance.agent import Person


class PersonViewModel:
    """View model for ``Person``."""

    def __init__(self, name: str, email: str, affiliation: Optional[str]) -> None:
        self.name = name
        self.email = email
        self.affiliation = affiliation

    @classmethod
    def from_person(cls, person: Person):
        """Create view model from ``Person``.

        Args:
            person(Person): The person to convert.
        Returns:
            View model for person
        """
        return cls(name=person.name, email=person.email, affiliation=person.affiliation)

    def __str__(self) -> str:
        email = affiliation = ""

        if self.email:
            email = f" <{self.email}>"

        if self.affiliation:
            affiliation = f" [{self.affiliation}]"

        return f"{self.name}{email}{affiliation}"
