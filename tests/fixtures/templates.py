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
from typing import List, Optional, Tuple

import pytest
from packaging.version import Version

from renku.version import __version__ as renku_version


@pytest.fixture
def template_metadata():
    """Default template metadata."""
    yield {
        "__template_source__": "renku",
        "__template_ref__": "master",
        "__template_id__": "python-minimal",
        "__namespace__": "",
        "__repository__": "",
        "__project_description__": "no description",
        "__project_slug__": "",
        "__renku_version__": renku_version,
    }


@pytest.fixture
def template(template_metadata):
    """Yield template data."""
    template = {
        "url": "https://github.com/SwissDataScienceCenter/renku-project-template",
        "id": "python-minimal",
        "index": 1,
        "ref": "master",
        # TODO: Add template parameters here once parameters are added to the template.
        "metadata": {},
        "default_metadata": template_metadata,
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
def templates_source(tmp_path, monkeypatch):
    """A dummy TemplatesSource."""
    from renku.core import errors
    from renku.core.models.template import Template, TemplatesSource

    templates_source_root = tmp_path / "templates_source"
    dummy_template_root = templates_source_root / "dummy"

    dummy_template_root.mkdir(parents=True, exist_ok=True)

    (templates_source_root / "manifest.yaml").write_text(
        textwrap.dedent(
            """
            - id: dummy
              name: Dummy Template
              description: A dummy template
              immutable_template_files:
                - immutable.file
            """
        )
    )

    (dummy_template_root / "{{ __name__ }}.dummy").touch()
    (dummy_template_root / "README.md").write_text("""A Renku project: {{ __project_description__ }}\n""")
    (dummy_template_root / "immutable.file").write_text("immutable content")
    (dummy_template_root / ".gitignore").write_text(".swp")
    (dummy_template_root / "Dockerfile").write_text(
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

    class DummyTemplatesSource(TemplatesSource):
        """Base class for Renku template sources."""

        def __init__(self, path, source, reference, version):
            super().__init__(path=path, source=source, reference=reference, version=version)
            self._versions = [Version(version)]

        @classmethod
        def fetch(cls, source: Optional[str], reference: Optional[str]) -> "TemplatesSource":
            raise NotImplementedError

        @property
        def versions(self):
            """Return all available versions."""
            return self._versions

        @versions.setter
        def versions(self, versions: List[str]):
            """Set all available version."""
            self._versions = [Version(v) for v in versions]

        def is_update_available(self, id: str, reference: Optional[str], version: Optional[str]) -> Tuple[bool, str]:
            """Return True if an update is available along with the latest version of a template."""
            latest_version = self.get_latest_version(id=id, reference=reference, version=version)

            return latest_version != version, latest_version

        def get_all_versions(self, id) -> List[str]:
            """Return all available versions for a template id."""
            return [str(v) for v in self.versions]

        def get_latest_version(self, id: str, reference: Optional[str], version: Optional[str]) -> Optional[str]:
            """Return latest version number of a template."""
            _ = self.get_template(id=id, reference=reference)
            return str(max(self.versions))

        def get_template(self, id, reference: Optional[str]) -> Optional[Template]:
            """Return a template at a specific reference."""
            try:
                return next(t for t in self.templates if t.id == id)
            except StopIteration:
                raise errors.TemplateNotFoundError(f"The template with id '{id}' is not available.")

        def update(self, id, version, content="# modification"):
            """Update all files of a template."""
            template = self.get_template(id=id, reference=None)

            for relative_path in template.get_files():
                path = template.path / relative_path
                path.write_text(f"{path.read_text()}\n{content}")

            self._versions.append(Version(version))

    dummy_templates_source = DummyTemplatesSource(
        path=templates_source_root, source="dummy", reference="1.0.0", version="1.0.0"
    )

    with monkeypatch.context() as monkey:
        import renku.core.management.template.usecase

        def mocked_fetch_templates_source(source: Optional[str], reference: Optional[str]):
            return dummy_templates_source

        monkey.setattr(renku.core.management.template.usecase, "fetch_templates_source", mocked_fetch_templates_source)

        yield dummy_templates_source


@pytest.fixture
def source_template(templates_source):
    """A dummy Template."""
    from renku.core.models.template import Template

    template: Template = templates_source.get_template(id="dummy", reference=None)

    yield template


@pytest.fixture
def rendered_template(source_template, template_metadata):
    """A dummy RenderedTemplate."""
    from renku.core.models.template import TemplateMetadata

    rendered_template = source_template.render(metadata=TemplateMetadata.from_dict(template_metadata))

    yield rendered_template


@pytest.fixture
def client_with_template(client, rendered_template, templates_source, client_database_injection_manager):
    """A client with a dummy template."""
    from renku.core.management.template.template import (
        FileAction,
        copy_template_metadata_to_client,
        copy_template_to_client,
    )

    with client_database_injection_manager(client):
        actions = {f: FileAction.OVERWRITE for f in rendered_template.get_files()}

        copy_template_to_client(rendered_template=rendered_template, client=client, actions=actions)
        copy_template_metadata_to_client(rendered_template=rendered_template, client=client, project=client.project)

        client.template_files = [client.path / f for f in rendered_template.get_files()]

        client.repository.add(all=True)
        client.repository.commit("Set a dummy template")

    yield client


@pytest.fixture
def rendered_template_with_update(tmp_path, rendered_template):
    """An updated RenderedTemplate.

    This fixture modifies 3 files in the template: ``immutable.file``, ``.gitignore``, ``Dockerfile``.
    """
    from renku.core.models.template import RenderedTemplate

    updated_template_root = tmp_path / "rendered_template_with_update"

    shutil.copytree(str(rendered_template.path), str(updated_template_root))

    (updated_template_root / "immutable.file").write_text("updated immutable content")
    (updated_template_root / ".gitignore").write_text(".swp\n.idea")
    dockerfile = updated_template_root / "Dockerfile"
    dockerfile.write_text(f"{dockerfile.read_text()}\n# Updated Dockerfile")

    updated_rendered_template = RenderedTemplate(
        path=updated_template_root, template=rendered_template.template, metadata=rendered_template.metadata
    )

    yield updated_rendered_template
