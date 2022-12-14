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
"""Test plugins for the ``workflow`` command."""
import os
from subprocess import Popen
from typing import List, Union

import pytest

from renku.core.plugin import pluginmanager as pluginmanager
from renku.ui.cli import cli
from tests.fixtures.repository import RenkuProject


def test_renku_workflow_exporter_hook(monkeypatch, dummy_workflow_exporter_hook, runner, project):
    """Tests that the renku workflow export plugin hook on ``Plan`` is called."""
    pm = pluginmanager.get_plugin_manager()
    pm.register(dummy_workflow_exporter_hook)

    with monkeypatch.context() as m:
        m.setattr(pluginmanager, "get_plugin_manager", lambda: pm)

        cmd = ["echo", "test"]
        run_name = "run_1"
        result = runner.invoke(cli, ["run", "--no-output", "--name", run_name] + cmd)
        assert 0 == result.exit_code

        result = runner.invoke(cli, ["workflow", "export", "--format", "dummy", run_name])
        assert "dummy" in result.output


@pytest.mark.parametrize(
    "create_workflow_commands,generated_files,workflow_name",
    [
        (
            [
                'renku run --name step1 -- echo "some\nrandom\ncontent" > input1',
                "renku run --name step2 -- cat input1 > output1",
                "renku workflow compose combined1 step1 step2",
                "renku run --name step3 -- cat output1 > output2",
                "renku run --name step4 -- cat output2 > output3",
                "renku workflow compose combined2 step3 step4",
                "renku workflow compose combined_all combined1 combined2",
            ],
            ["input1", "output1", "output2", "output3"],
            "combined_all",
        ),
        (
            [
                'renku run --name step1 -- echo "some\nrandom\ncontent" > input1',
                "renku run --name step2 -- cat input1 > output1",
                "renku run --name step3 -- cat output1 > output2",
                "renku run --name step4 -- cat output2 > output3",
            ],
            ["input1"],
            "step1",
        ),
        (
            [
                'renku run --name step1 -- echo "some\nrandom\ncontent" > input1',
                "renku run --name step2 -- cat input1 > output1",
                "renku run --name step3 -- cat output1 > output2",
                "renku run --name step4 -- cat output2 > output3",
            ],
            ["output3"],
            "step4",
        ),
    ],
)
def test_cwl_workflow_export(
    create_workflow_commands: Union[str, List[str]], generated_files, workflow_name, project: RenkuProject, run_shell
):
    """Check that an exported CWL workflow can fully recreate saved outputs/inputs."""
    for command in create_workflow_commands:
        ps: Popen = run_shell(command, work_dir=project.path, return_ps=True)
        _ = ps.communicate()
        assert ps.returncode == 0

    assert not project.repository.is_dirty(untracked_files=True)

    ps = run_shell(
        f"renku workflow export --format cwl {workflow_name} > test.cwl", work_dir=project.path, return_ps=True
    )
    _ = ps.communicate()
    assert ps.returncode == 0

    for i in generated_files:
        ipath = project.path / i
        assert ipath.exists()
        os.remove(ipath)

    ps = run_shell("cwltool test.cwl", work_dir=project.path, return_ps=True)
    _ = ps.communicate()
    assert ps.returncode == 0

    os.remove(project.path / "test.cwl")
    assert not project.repository.is_dirty(untracked_files=True)
