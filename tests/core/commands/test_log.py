# -*- coding: utf-8 -*-
#
# Copyright 2017-2021- Swiss Data Science Center (SDSC)
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
"""Renku log command tests."""


from datetime import datetime, timedelta

from renku.core.commands.log import _log
from renku.core.commands.view_model.log import LogType
from renku.core.models.provenance.activity import Activity, Association
from renku.core.models.provenance.agent import Person, SoftwareAgent


def test_log_activities(mocker):
    """Test getting activity viewmodels on log."""
    agents = [
        Person(id="mailto:john.doe@example.com", email="john.doe@example.com", name="John Doe"),
        SoftwareAgent(name="renku 99.1.1", id="<https://github.com/swissdatasciencecenter/renku-python/tree/v0.16.1>"),
    ]

    plan1 = mocker.MagicMock()
    plan1.to_argv.return_value = ["touch", "A"]
    plan1.copy.return_value = plan1

    plan2 = mocker.MagicMock()
    plan2.to_argv.return_value = ["cp", "A", "B"]
    plan2.copy.return_value = plan2

    plan3 = mocker.MagicMock()
    plan3.to_argv.return_value = ["cp", "B", "C"]
    plan3.copy.return_value = plan3

    previous_id = Activity.generate_id()
    previous = Activity(
        id=previous_id,
        started_at_time=datetime.utcnow() - timedelta(hours=1, seconds=15),
        ended_at_time=datetime.utcnow() - timedelta(hours=1, seconds=10),
        association=Association(id=Association.generate_id(previous_id), plan=plan1),
        agents=agents,
    )

    intermediate_id = Activity.generate_id()
    intermediate = Activity(
        id=intermediate_id,
        started_at_time=datetime.utcnow() - timedelta(hours=1, seconds=10),
        ended_at_time=datetime.utcnow() - timedelta(hours=1, seconds=5),
        association=Association(id=Association.generate_id(intermediate_id), plan=plan2),
        agents=agents,
    )

    following_id = Activity.generate_id()
    following = Activity(
        id=following_id,
        started_at_time=datetime.utcnow() - timedelta(hours=1, seconds=5),
        ended_at_time=datetime.utcnow() - timedelta(hours=1),
        association=Association(id=Association.generate_id(following_id), plan=plan3),
        agents=agents,
    )

    activity_gateway = mocker.MagicMock()
    activity_gateway.get_all_activities.return_value = [previous, intermediate, following]
    full_agents = [a.full_identity for a in agents]
    result = _log(activity_gateway, workflows_only=True)
    assert 3 == len(result)
    assert all(log.type == LogType.ACTIVITY for log in result)
    assert result[0].date == previous.ended_at_time
    assert result[1].date == intermediate.ended_at_time
    assert result[2].date == following.ended_at_time
    assert result[0].agents == full_agents
    assert result[1].agents == full_agents
    assert result[2].agents == full_agents
    assert result[0].description == "touch A"
    assert result[1].description == "cp A B"
    assert result[2].description == "cp B C"
