# -*- coding: utf-8 -*-
#
# Copyright 2021 Swiss Data Science Center (SDSC)
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
"""Renku common fixtures."""

import shutil
import textwrap
from pathlib import Path

import pytest

from renku.version import __version__ as renku_version


@pytest.fixture
def template():
    """Yield template data."""
    template = {
        "url": "https://github.com/SwissDataScienceCenter/renku-project-template",
        "id": "python-minimal",
        "index": 1,
        "ref": "master",
        # TODO: Add template parameters here once parameters are added to the template.
        "metadata": {},
        "default_metadata": {
            "__template_source__": "renku",
            "__template_ref__": "master",
            "__template_id__": "python-minimal",
            "__namespace__": "",
            "__repository__": "",
            "__project_description__": "nodesc",
            "__project_slug__": "",
            "__renku_version__": renku_version,
        },
    }

    yield template


@pytest.fixture()
def project_init(template):
    """Yield template data."""
    data = {
        "test_project": "test-new-project",
        "test_project_alt": "test-new-project-2",
        "main_branch": "main",
    }

    commands = {
        "init": ["init", "."],
        "init_test": ["init", data["test_project"]],
        "init_alt": ["init", data["test_project_alt"]],
        "init_custom": [
            "init",
            "--template-ref",
            template["ref"],
            "--template-id",
            "python-minimal",
            data["test_project"],
        ],
        "init_custom_template": (
            "https://dev.renku.ch/gitlab/renku-python-integration-tests/core-it-template-variable-test-project"
        ),
        "remote": ["--template-source", template["url"], "--template-ref", template["ref"]],
        "id": ["--template-id", template["id"]],
        "index": ["--template-index", template["index"]],
        "force": ["--force"],
        "parameters": ["--parameter", "p1=v1", "--parameter", "p2=v2"],
        "parameters_equal_missing": ["--parameter", "p3:v3"],
        "parameters_equal_early": ["--parameter", "=p4v3"],
        "initial_branch_main": ["--initial-branch", "main"],
        "confirm": len(set(template["metadata"].keys())) * "\n",
    }

    yield data, commands


@pytest.fixture
def template_update(tmpdir, local_client, mocker, monkeypatch, template, client_database_injection_manager):
    """Create a mocked template for updates."""

    def _template_update(immutable_files=None, docker=False, after_template_version="0.0.2"):
        """Fetches an updatable template with various options."""
        from renku.core.commands.init import create_from_template
        from renku.core.management.template.template import EmbeddedTemplates
        from renku.core.models.template import TEMPLATE_MANIFEST, TemplatesManifest

        try:
            import importlib_resources
        except ImportError:
            import importlib.resources as importlib_resources

        # NOTE: get template
        tempdir = tmpdir.mkdir("template")
        temppath = Path(tempdir) / "local"

        ref = importlib_resources.files("renku") / "templates"
        with importlib_resources.as_file(ref) as template_local:
            shutil.copytree(str(template_local), str(temppath))

        manifest = TemplatesManifest.from_path(temppath / TEMPLATE_MANIFEST).get_raw_content()
        template_path = temppath / manifest[0]["folder"]

        if docker:
            monkeypatch.setattr("renku.__version__", "0.0.1")

            # TODO: remove this once the renku template contains RENKU_VERSION
            dockerfile_path = template_path / "Dockerfile"
            dockerfile = dockerfile_path.read_text()
            dockerfile_path.write_text(f"ARG RENKU_VERSION=0.0.1\n{dockerfile}")

        local_client.init_repository()

        # NOTE: init project from template
        with client_database_injection_manager(local_client):
            create_from_template(
                template_path,
                local_client,
                "name",
                {**template["default_metadata"], **template["metadata"]},
                template_version="0.0.1",
                immutable_template_files=immutable_files or [],
            )

        project_files = [
            f
            for f in local_client.path.glob("**/*")
            if ".git" not in str(f)
            and ".renku/metadata" not in str(f)
            and not str(f).endswith(".renku/template_checksums.json")
        ]

        template_files = []
        for project_file in project_files:
            expected_file = template_path / project_file.relative_to(local_client.path)
            template_files.append(expected_file)
            assert expected_file.exists()

        fetch_template = mocker.patch("renku.core.management.template.template.fetch_templates_source")
        source = EmbeddedTemplates(path=temppath, source="renku", reference=None, version=after_template_version)
        fetch_template.return_value = source

        return {
            "template_files": template_files,
            "project_files": project_files,
            "manifest": manifest,
            "manifest_path": temppath,
            "template_path": template_path,
        }

    yield _template_update


@pytest.fixture
def source_template(tmp_path):
    """Yield a dummy Template."""
    from renku.core.models.template import SourceTemplate

    (tmp_path / "{{ __name__ }}.dummy").touch()
    (tmp_path / "README.md").write_text("""A Renku project: {{ __project_description__ }}\n""")
    (tmp_path / "Dockerfile").write_text(
        textwrap.dedent(
            """
            ARG RENKU_BASE_IMAGE=renku/renkulab-r:4.1.2-0.11.0
            FROM ${RENKU_BASE_IMAGE}
            # Docker content
            ARG RENKU_VERSION={{ __renku_version__ | default("0.42.0") }}
            # More content
            """
        )
    )

    yield SourceTemplate(
        id="dummy",
        name="Dummy Template",
        description="A dummy template",
        parameters={},
        icon="",
        immutable_files=[],
        allow_update=True,
        source="dummy",
        reference="1.0.0",
        version="7598ddf356e28c80747f93ce97a55a69082b5cf1",
        path=tmp_path,
        templates_source=None,  # TODO
    )
