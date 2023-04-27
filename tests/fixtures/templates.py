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
from typing import Generator, List, Optional

import pytest

from renku.core.template.template import EmbeddedTemplates, FileAction, RepositoryTemplates, copy_template_to_project
from renku.domain_model.project_context import project_context
from renku.domain_model.template import (
    RenderedTemplate,
    Template,
    TemplateMetadata,
    TemplateParameter,
    TemplatesManifest,
)
from renku.infrastructure.repository import Repository
from renku.version import __version__ as renku_version
from tests.fixtures.repository import RenkuProject


@pytest.fixture
def template_metadata():
    """Default template metadata."""
    yield {
        "__template_source__": "renku",
        "__template_ref__": renku_version,
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
            "https://gitlab.dev.renku.ch/renku-python-integration-tests/core-it-template-variable-test-project"
        ),
        "remote": ["--template-source", template["url"], "--template-ref", template["ref"]],
        "id": ["--template-id", template["id"]],
        "force": ["--force"],
        "parameters": ["--parameter", "p1=v1", "--parameter", "p2=v2"],
        "parameters_equal_missing": ["--parameter", "p3:v3"],
        "parameters_equal_early": ["--parameter", "=p4v3"],
        "initial_branch_main": ["--initial-branch", "main"],
        "confirm": len(set(template["metadata"].keys())) * "\n",
    }

    yield data, commands


@pytest.fixture
def templates_source_root(tmp_path) -> Path:
    """Root of Dummy TemplatesSource."""
    return tmp_path / "templates_source"


@pytest.fixture
def source_template(templates_source_root) -> Template:
    """A dummy Template."""
    dummy_template_root = templates_source_root / "dummy"

    (dummy_template_root / ".renku").mkdir(parents=True, exist_ok=True)

    (dummy_template_root / "{{ __name__ }}.dummy").touch()
    (dummy_template_root / "README.md").write_text("""A Renku project: {{ __project_description__ }}\n""")
    (dummy_template_root / "immutable.file").write_text("immutable content")
    (dummy_template_root / ".gitignore").write_text(".swp")
    (dummy_template_root / ".renku" / "renku.ini").touch()
    (dummy_template_root / "requirements.txt").touch()
    (dummy_template_root / "Dockerfile").write_text(
        textwrap.dedent(
            """
            ARG RENKU_BASE_IMAGE=renku/renkulab-r:4.1.2-0.11.0
            FROM ${RENKU_BASE_IMAGE}
            # Docker content
            ARG RENKU_VERSION={{ __renku_version__ | default("0.42.0") }}
            # More content
            ########################################################
            #        Renku-specific section - DO NOT MODIFY        #
            ########################################################

            FROM ${RENKU_BASE_IMAGE} as builder

            RUN specific commands

            ########################################################
            #              End Renku-specific section              #
            ########################################################
            # More content
            """
        )
    )

    return Template(
        id="dummy",
        aliases=[],
        name="Dummy Template",
        description="A dummy template",
        source="dummy",
        reference="1.0.0",
        version="1.0.0",
        allow_update=True,
        parameters={},
        immutable_files=["immutable.file"],
        path=dummy_template_root,
        icon="",
        templates_source=None,
        ssh_supported=False,
    )


@pytest.fixture(params=["renku", "repository"])
def templates_source(request, monkeypatch, templates_source_root, source_template):
    """A dummy TemplatesSource."""
    manifest = templates_source_root / "manifest.yaml"

    manifest.write_text(
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

    def update_template_files(templates_source, id, content, parameters):
        template = templates_source.get_template(id=id, reference=None)

        if template is None or template.path is None:
            return

        for relative_path in template.get_files():
            path = template.path / relative_path

            if relative_path == "Dockerfile":
                path.write_text(path.read_text().replace("RUN specific commands", "RUN updated specific commands"))
            else:
                path.write_text(f"{path.read_text()}\n{content}")

        template.parameters = parameters or []

    def rename_template(templates_source, id: str, new_name: str):
        shutil.copytree(templates_source.path / "dummy", templates_source.path / new_name)

        manifest.write_text(
            textwrap.dedent(
                f"""
                - id: {new_name}
                  name: Dummy Template
                  description: A dummy template
                  immutable_template_files:
                    - immutable.file
                  aliases: [{id}]
                """
            )
        )
        # NOTE: Reload manifest file
        templates_source.manifest = TemplatesManifest.from_path(manifest, skip_validation=True)

    class DummyRenkuTemplatesSource(EmbeddedTemplates):
        """Base class for Renku template sources."""

        @classmethod
        def fetch(cls, source: Optional[str], reference: Optional[str]) -> "DummyRenkuTemplatesSource":
            return cls(path=templates_source_root, source="renku", reference=reference, version=reference)

        def update(self, id, version, content="# modification", parameters: Optional[List[TemplateParameter]] = None):
            """Update all files of a template."""
            update_template_files(self, id, content, parameters)
            self.version = self.reference = version

        def rename(self, id: str, new_name: str, version: str):
            """Rename a template."""
            rename_template(templates_source=self, id=id, new_name=new_name)
            self.version = self.reference = version

    class DummyRepositoryTemplatesSource(RepositoryTemplates):
        """Base class for Renku template sources."""

        @classmethod
        def fetch(cls, source: Optional[str], reference: Optional[str]) -> "DummyRepositoryTemplatesSource":
            repository = Repository.initialize(templates_source_root)
            repository.add(all=True)
            repository.commit("dummy template", no_verify=True)
            repository.tags.add(reference)

            return cls(
                path=templates_source_root,
                source=f"file://{templates_source_root}",
                reference=reference,
                version=repository.head.commit.hexsha,
                repository=repository,
            )

        def update(self, id, version, content="# modification", parameters: Optional[List[TemplateParameter]] = None):
            """Update all files of a template."""
            update_template_files(self, id, content, parameters)
            self.update_templates(f"Update {id} template", version)

        def rename(self, id: str, new_name: str, version: str):
            """Rename a template."""
            rename_template(templates_source=self, id=id, new_name=new_name)
            self.update_templates(f"Rename {id} template", version)

        def update_templates(self, message, version):
            self.repository.add(all=True)
            self.repository.commit(message, no_verify=True)
            self.repository.tags.add(version)

            self.version = self.repository.head.commit.hexsha
            self.reference = version

    if not request.param or request.param == "renku":
        dummy_templates_source = DummyRenkuTemplatesSource.fetch("renku", reference="1.0.0")
    elif request.param == "repository":
        dummy_templates_source = DummyRepositoryTemplatesSource.fetch("renku", reference="1.0.0")
    else:
        raise ValueError(f"Invalid TemplatesSource value: {request.param}")

    with monkeypatch.context() as monkey:
        import renku.core.template.usecase

        def mocked_fetch_templates_source(*_, **__):
            return dummy_templates_source

        monkey.setattr(renku.core.template.usecase, "fetch_templates_source", mocked_fetch_templates_source)

        yield dummy_templates_source


@pytest.fixture
def rendered_template(source_template, template_metadata):
    """A dummy RenderedTemplate."""
    rendered_template = source_template.render(metadata=TemplateMetadata.from_dict(template_metadata))

    yield rendered_template


@pytest.fixture
def project_with_template(project, rendered_template, with_injection) -> Generator[RenkuProject, None, None]:
    """A project with a dummy template."""
    with with_injection():
        actions = {f: FileAction.OVERWRITE for f in rendered_template.get_files()}
        project_object = project_context.project

        copy_template_to_project(rendered_template=rendered_template, project=project_object, actions=actions)

        project_object.template_files = [str(project_context.path / f) for f in rendered_template.get_files()]

    project.repository.add(all=True)
    project.repository.commit("Set a dummy template", no_verify=True)

    yield project


@pytest.fixture
def rendered_template_with_update(tmp_path, rendered_template):
    """An updated RenderedTemplate that modifies some template files."""
    updated_template_root = tmp_path / "rendered_template_with_update"

    shutil.copytree(str(rendered_template.path), str(updated_template_root))

    (updated_template_root / "immutable.file").write_text("updated immutable content")
    (updated_template_root / ".gitignore").write_text(".swp\n.idea")
    (updated_template_root / "requirements.txt").write_text("changed\n")
    dockerfile = updated_template_root / "Dockerfile"
    dockerfile.write_text(dockerfile.read_text().replace("RUN specific commands", "RUN updated specific commands"))
    (updated_template_root / "README.md").write_text("""Updated README: {{ __project_description__ }}\n""")

    updated_rendered_template = RenderedTemplate(
        path=updated_template_root, template=rendered_template.template, metadata=rendered_template.metadata
    )

    yield updated_rendered_template
