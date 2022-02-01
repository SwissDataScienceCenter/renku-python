# -*- coding: utf-8 -*-
#
# Copyright 2019-2021 - Swiss Data Science Center (SDSC)
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
"""Template tests."""

import pytest

from renku.core import errors
from renku.core.management.migrate import migrate
from renku.core.management.template.template import fetch_templates_source
from renku.core.models.template import TEMPLATE_MANIFEST

try:
    import importlib_resources
except ImportError:
    import importlib.resources as importlib_resources

ref = importlib_resources.files("renku") / "templates"
with importlib_resources.as_file(ref) as path:
    template_local = path


TEMPLATES_URL = "https://github.com/SwissDataScienceCenter/renku-project-template"


@pytest.mark.integration
@pytest.mark.parametrize("reference", [None, "master", "0.3.0"])
@pytest.mark.skip
def test_template_fetch_from_git(reference):
    """Test fetching a template from git."""
    templates_source = fetch_templates_source(source=TEMPLATES_URL, reference=reference)

    assert reference == templates_source.reference
    assert (templates_source.path / TEMPLATE_MANIFEST).exists()

    for template in templates_source.templates:
        assert template.path.exists()


def test_template_fetch_invalid_git_url():
    """Test fetching a template from an invalid git url."""
    with pytest.raises(errors.InvalidTemplateError):
        fetch_templates_source(source="invalid-url", reference=None)


@pytest.mark.integration
def test_template_fetch_invalid_git_reference():
    """Test fetching a template from an invalid reference."""
    with pytest.raises(errors.InvalidTemplateError):
        fetch_templates_source(source=TEMPLATES_URL, reference="invalid-ref")


def test_update_from_template(local_client, template_update, client_database_injection_manager):
    """Test repository update from a template."""
    local_client.init_repository()

    res = template_update()
    project_files = res["project_files"]
    template_files = res["template_files"]

    project_files_before = {p: p.read_text() for p in project_files if not p.is_dir()}

    for p in template_files:
        if p.is_dir():
            continue
        p.write_text(f"{p.read_text()}\nmodified")

    with client_database_injection_manager(local_client):
        migrate(skip_docker_update=True)

    for p in project_files:
        if p.is_dir():
            continue
        content = project_files_before[p]
        new_content = p.read_text()
        assert content != new_content


def test_update_from_template_with_modified_files(local_client, template_update, client_database_injection_manager):
    """Test repository update from a template with modified local files."""
    local_client.init_repository()

    res = template_update()
    project_files = res["project_files"]
    template_files = res["template_files"]

    project_files_before = {p: p.read_text() for p in project_files if not p.is_dir()}

    for p in template_files:
        if p.is_dir():
            continue
        p.write_text(f"{p.read_text()}\nmodified")

    # NOTE: modify local file
    modified_file = next(f for f in project_files if str(f).endswith("README.md"))
    modified_local_content = modified_file.read_text() + "\nlocal modification"
    modified_file.write_text(modified_local_content)

    # NOTE: delete local file
    deleted_file = next(f for f in project_files if str(f).endswith("README.md"))
    deleted_file.unlink()

    with client_database_injection_manager(local_client):
        migrate(skip_docker_update=True)

    for p in project_files:
        if p.is_dir():
            continue
        if p == deleted_file:
            assert not p.exists()
            continue

        content = project_files_before[p]
        new_content = p.read_text()

        if p == modified_file:
            assert modified_local_content == new_content
        else:
            assert content != new_content


def test_update_from_template_with_immutable_modified_files(
    local_client, mocker, template_update, client_database_injection_manager
):
    """Test repository update from a template with modified local immutable files."""
    local_client.init_repository()

    res = template_update(immutable_files=["README.md"])
    project_files = res["project_files"]
    template_files = res["template_files"]

    for p in template_files:
        if p.is_dir():
            continue
        p.write_text(f"{p.read_text()}\nmodified")

    # NOTE: modify local file
    modified_file = next(f for f in project_files if str(f).endswith("README.md"))
    modified_local_content = modified_file.read_text() + "\nlocal modification"
    modified_file.write_text(modified_local_content)

    with pytest.raises(
        errors.TemplateUpdateError, match=r"Can't update template as immutable template file .* has local changes."
    ), client_database_injection_manager(local_client):
        migrate()


def test_update_from_template_with_immutable_deleted_files(
    local_client, mocker, template_update, client_database_injection_manager
):
    """Test repository update from a template with deleted local immutable files."""
    local_client.init_repository()

    res = template_update(immutable_files=["README.md"])
    project_files = res["project_files"]
    template_files = res["template_files"]

    for p in template_files:
        if p.is_dir():
            continue
        p.write_text(f"{p.read_text()}\nmodified")

    # NOTE: modify local file
    deleted_file = next(f for f in project_files if str(f).endswith("README.md"))
    deleted_file.unlink()

    with pytest.raises(
        errors.TemplateUpdateError, match=r"Can't update template as immutable template file .* has local changes."
    ), client_database_injection_manager(local_client):
        migrate()


def test_update_template_dockerfile(local_client, monkeypatch, template_update, client_database_injection_manager):
    """Test repository Dockerfile update."""
    local_client.init_repository()

    template_update(docker=True, after_template_version="0.0.1")

    monkeypatch.setattr("renku.__version__", "0.0.2")

    with client_database_injection_manager(local_client):
        migrate()

    dockerfile = (local_client.path / "Dockerfile").read_text()
    assert "0.0.2" in dockerfile


def test_update_from_template_with_new_variable(
    local_client, mocker, template_update, client_database_injection_manager
):
    """Test repository update from a template with a new template variable required."""
    local_client.init_repository()

    res = template_update()
    manifest = res["manifest"]
    manifest_path = res["manifest_path"]
    template_files = res["template_files"]

    # NOTE: Add new template variable
    manifest[0]["variables"]["__new_arbitrary_template_value__"] = {"description": "new var"}
    fetch_template = mocker.patch("renku.core.utils.templates.fetch_templates_source")
    fetch_template.return_value = (manifest, manifest_path, "renku", "0.0.2")

    for p in template_files:
        if p.is_dir():
            continue
        p.write_text(f"{p.read_text()}\nmodified")

    with pytest.raises(
        errors.TemplateUpdateError, match=r".*Can't update template, it now requires variable.*"
    ), client_database_injection_manager(local_client):
        migrate()
