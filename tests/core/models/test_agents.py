# -*- coding: utf-8 -*-
#
# Copyright 2018-2020 - Swiss Data Science Center (SDSC)
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
"""Test agents."""
import pytest

from renku.core.models.provenance.agents import Person


@pytest.mark.parametrize(
    'value,has_name,has_email,has_affiliation', [
        ('John Doe<john.doe@mail.ch>[Some Affiliation]', True, True, True),
        (
            '  John Doe  <  john.doe@mail.ch  >  [  Some Affiliation  ]', True,
            True, True
        ),
        ('  John Doe  <  john.doe@mail.ch  >  [  ]', True, True, False),
        ('  John Doe  <  john.doe@mail.ch  >  ', True, True, False),
        (
            '  John Doe  <  john.doe@mail.ch  >  Some Affiliation ', True,
            True, False
        ),
        (
            '  <  john.doe@mail.ch  > [  Some Affiliation  ] ', False, True,
            True
        ),
        ('  <> [  Some Affiliation  ] ', False, False, True),
        ('  [  Some Affiliation  ] ', False, False, True),
        ('  <  john.doe@mail.ch  > [  ] ', False, True, False),
    ]
)
def test_construct_person(value, has_name, has_email, has_affiliation):
    """Test construct person from string."""
    p = Person.from_string(value)

    if has_name:
        assert 'John Doe' == p.name
    else:
        assert '' == p.name
    if has_email:
        assert 'john.doe@mail.ch' == p.email
    else:
        assert p.email is None
    if has_affiliation:
        assert 'Some Affiliation' == p.affiliation
    else:
        assert p.affiliation is None
