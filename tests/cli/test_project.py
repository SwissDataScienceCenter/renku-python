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
"""Test ``project`` command."""

import json

from renku.domain_model.provenance.agent import Person
from renku.infrastructure.gateway.project_gateway import ProjectGateway
from renku.ui.cli import cli
from tests.utils import format_result_exception


def test_project_show(runner, project, subdirectory):
    """Check showing project metadata."""
    result = runner.invoke(cli, ["project", "show"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Id:" in result.output
    assert "Name:" in result.output
    assert "Creator:" in result.output
    assert "Renku Version:" in result.output


def test_project_edit(runner, project, subdirectory, with_injection):
    """Check project metadata editing."""
    (project.path / "README.md").write_text("Make repo dirty.")

    creator = "Forename Surname [Affiliation]"

    metadata = {
        "@id": "https://example.com/annotation1",
        "@type": "https://schema.org/specialType",
        "https://schema.org/specialProperty": "some_unique_value",
    }
    metadata_path = project.path / "metadata.json"
    metadata_path.write_text(json.dumps(metadata))

    commit_sha_before = project.repository.head.commit.hexsha

    result = runner.invoke(
        cli,
        [
            "project",
            "edit",
            "-d",
            " new description ",
            "-c",
            creator,
            "--metadata",
            str(metadata_path),
            "-k",
            "keyword1",
            "-k",
            "keyword2",
        ],
    )

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Successfully updated: creator, description, keywords, custom_metadata." in result.output
    assert "Warning: No email or wrong format for: Forename Surname" in result.output

    assert project.repository.is_dirty(untracked_files=True)
    commit_sha_after = project.repository.head.commit.hexsha
    assert commit_sha_before != commit_sha_after

    with with_injection():
        project_gateway = ProjectGateway()
        project = project_gateway.get_project()

    assert " new description " == project.description
    assert isinstance(project.creator, Person)
    assert "Forename Surname" == project.creator.name
    assert "Affiliation" == project.creator.affiliation
    assert metadata == project.annotations[0].body
    assert {"keyword1", "keyword2"} == set(project.keywords)

    result = runner.invoke(cli, ["project", "show"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Id:" in result.output
    assert "Name:" in result.output
    assert "Creator:" in result.output
    assert "Renku Version:" in result.output
    assert "Keywords:" in result.output

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)


def test_project_edit_no_change(runner, project):
    """Check project metadata editing does not commit when there is no change."""
    (project.path / "README.md").write_text("Make repo dirty.")

    commit_sha_before = project.repository.head.commit.hexsha

    result = runner.invoke(cli, ["project", "edit"], catch_exceptions=False)

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Nothing to update." in result.output

    commit_sha_after = project.repository.head.commit.hexsha
    assert commit_sha_after == commit_sha_before
    assert project.repository.is_dirty(untracked_files=True)


def test_project_edit_unset(runner, project, subdirectory, with_injection):
    """Check project metadata editing."""
    (project.path / "README.md").write_text("Make repo dirty.")

    creator = "Forename Surname [Affiliation]"

    metadata = {
        "@id": "https://example.com/annotation1",
        "@type": "https://schema.org/specialType",
        "https://schema.org/specialProperty": "some_unique_value",
    }
    metadata_path = project.path / "metadata.json"
    metadata_path.write_text(json.dumps(metadata))

    result = runner.invoke(
        cli,
        [
            "project",
            "edit",
            "-d",
            " new description ",
            "-c",
            creator,
            "--metadata",
            str(metadata_path),
            "-k",
            "keyword1",
            "-k",
            "keyword2",
        ],
    )

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Successfully updated: creator, description, keywords, custom_metadata." in result.output
    assert "Warning: No email or wrong format for: Forename Surname" in result.output

    commit_sha_before = project.repository.head.commit.hexsha

    result = runner.invoke(
        cli,
        ["project", "edit", "-u", "keywords", "-u", "metadata"],
    )

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Successfully updated: keywords, custom_metadata." in result.output

    assert project.repository.is_dirty(untracked_files=True)
    commit_sha_after = project.repository.head.commit.hexsha
    assert commit_sha_before != commit_sha_after

    with with_injection():
        project_gateway = ProjectGateway()
        project = project_gateway.get_project()

    assert not project.annotations
    assert not project.keywords

    result = runner.invoke(cli, ["project", "show"])

    assert 0 == result.exit_code, format_result_exception(result)
    assert "Id:" in result.output
    assert "Name:" in result.output
    assert "Creator:" in result.output
    assert "Renku Version:" in result.output
    assert "Keywords:" in result.output

    result = runner.invoke(cli, ["graph", "export", "--format", "json-ld", "--strict"])
    assert 0 == result.exit_code, format_result_exception(result)
