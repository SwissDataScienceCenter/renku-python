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
"""Project initialization tests."""

import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

import pkg_resources
import pytest

from renku.core import errors
from renku.core.commands.init import (
    TEMPLATE_MANIFEST,
    create_from_template,
    fetch_template_from_git,
    read_template_manifest,
    validate_template,
)
from renku.core.management.config import RENKU_HOME
from renku.core.management.migrate import migrate
from tests.utils import raises

template_local = Path(pkg_resources.resource_filename("renku", "templates"))


@pytest.mark.integration
def test_fetch_template_from_git(template):
    """Test fetching a template.

    It fetches a template from remote and verifies that the manifest
    file is there.
    """
    # ? Can't parametrize with fixtures yet
    # ? REF: https://docs.pytest.org/en/stable/proposals/parametrize_with_fixtures.html
    parametrize = [
        {"url": template["url"], "ref": template["ref"], "error": None},
        {"url": "fake", "ref": template["ref"], "error": errors.GitError},
        {"url": template["url"], "ref": "fake", "error": errors.GitError},
    ]
    for parameters in parametrize:
        with TemporaryDirectory() as tempdir:
            with raises(parameters["error"]):
                manifest_file, _ = fetch_template_from_git(parameters["url"], parameters["ref"], Path(tempdir))
                assert Path(tempdir) / TEMPLATE_MANIFEST == manifest_file
                assert manifest_file.exists()


def test_read_template_manifest():
    """Test reading template manifest file.

    It creates a fake manifest file and it verifies it's read property.
    """
    with TemporaryDirectory() as tempdir:
        template_file = Path(tempdir) / TEMPLATE_MANIFEST

        # error on missing template file
        with raises(errors.InvalidTemplateError):
            manifest = read_template_manifest(Path(tempdir), checkout=False)

        template_file.touch(exist_ok=True)
        # error on invalid template file
        with raises(errors.InvalidTemplateError):
            manifest = read_template_manifest(Path(tempdir), checkout=False)

        template_file.write_text(
            "-\n"
            "  folder: first\n"
            "  name: Basic Project 1\n"
            "  description: Description 1\n"
            "  variables: {}\n"
            "-\n"
            "  folder: second\n"
            "  name: Basic Project 2\n"
            "  description: Description 2\n"
            "  variables:\n"
            "    custom: Custom Value\n"
        )

        manifest = read_template_manifest(Path(tempdir), checkout=False)
        assert 2 == len(manifest)
        assert "first" == manifest[0]["folder"]
        assert "second" == manifest[1]["folder"]
        assert "Basic Project 1" == manifest[0]["name"]
        assert "Description 2" == manifest[1]["description"]

        variables1 = manifest[0]["variables"]
        variables2 = manifest[1]["variables"]
        assert 0 == len(variables1)
        assert 1 == len(variables2)
        assert 1 == len(variables2.keys())
        assert "custom" in variables2.keys()
        assert "Custom Value" == variables2["custom"]

        template_file.write_text("-\n" "  folder: first\n" "  description: Description 1\n")
        with raises(errors.InvalidTemplateError):
            manifest = read_template_manifest(Path(tempdir), checkout=False)

        template_file.write_text("-\n" "  name: Basic Project 2\n" "  description: Description 2\n")
        with raises(errors.InvalidTemplateError):
            manifest = read_template_manifest(Path(tempdir), checkout=False)


@pytest.mark.integration
def test_fetch_template_from_git_and_read_manifest(template):
    """Test template fetch and manifest reading.

    It fetches a local template, reads the manifest, checkouts the
    template folders and verify they exist.
    """
    with TemporaryDirectory() as tempdir:
        template_path = Path(tempdir)
        fetch_template_from_git(template["url"], template["ref"], template_path)
        manifest = read_template_manifest(template_path, checkout=True)
        for template in manifest:
            template_folder = template_path / template["folder"]
            assert template_folder.exists()


def test_validate_template():
    """Test template validation.

    It validates each local template.
    """
    with TemporaryDirectory() as tempdir:
        temppath = Path(tempdir)
        # file error
        with raises(errors.InvalidTemplateError):
            validate_template(temppath)

        # folder error
        shutil.rmtree(str(tempdir))
        renku_dir = temppath / RENKU_HOME
        renku_dir.mkdir(parents=True)
        with raises(errors.InvalidTemplateError):
            validate_template(temppath)

        # valid template
        shutil.rmtree(str(tempdir))
        shutil.copytree(str(template_local), str(tempdir))
        manifest = read_template_manifest(Path(tempdir))
        for template in manifest:
            template_folder = temppath / template["folder"]
            assert validate_template(template_folder) is True


def test_create_from_template(local_client, template):
    """Test repository creation from a template.

    It creates a renku projects from one of the local templates and it verifies
    the data are properly copied to the new renku project folder.
    """
    with TemporaryDirectory() as tempdir:
        temppath = Path(tempdir) / "local"
        shutil.copytree(str(template_local), str(temppath))
        manifest = read_template_manifest(temppath)
        template_path = temppath / manifest[0]["folder"]
        create_from_template(
            template_path, local_client, "name", {**template["default_metadata"], **template["metadata"]}
        )
        template_files = [
            f
            for f in local_client.path.glob("**/*")
            if ".git" not in str(f)
            and not str(f).endswith(".renku/metadata.yml")
            and not str(f).endswith(".renku/template_checksums.json")
        ]
        for template_file in template_files:
            expected_file = template_path / template_file.relative_to(local_client.path)
            assert expected_file.exists()


def test_template_filename(local_client, template):
    """Test using a template with dynamic filenames.
    """
    with TemporaryDirectory() as tempdir:
        template_folder = Path(tempdir) / "first"

        template_folder.mkdir(parents=True)

        template_file = template_folder / "{{ name }}.r"
        template_file.write_text("{{ name }}")

        (local_client.path / ".renku").mkdir()

        create_from_template(template_folder, local_client, name="test", metadata=template["default_metadata"])

        assert (local_client.path / "test.r").exists()


def test_update_from_template(local_client, template_update):
    """Test repository update from a template."""
    res = template_update()
    project_files = res["project_files"]
    template_files = res["template_files"]

    project_files_before = {p: p.read_text() for p in project_files if not p.is_dir()}

    for p in template_files:
        if p.is_dir():
            continue
        p.write_text(f"{p.read_text()}\nmodified")

    migrate(local_client, skip_docker_update=True)

    for p in project_files:
        if p.is_dir():
            continue
        content = project_files_before[p]
        new_content = p.read_text()
        assert content != new_content


def test_update_from_template_with_modified_files(local_client, template_update):
    """Test repository update from a template with modified local files."""
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

    migrate(local_client, skip_docker_update=True)

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


def test_update_from_template_with_immutable_modified_files(local_client, mocker, template_update):
    """Test repository update from a template with modified local immutable files."""
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
    ):
        migrate(local_client)


def test_update_from_template_with_immutable_deleted_files(local_client, mocker, template_update):
    """Test repository update from a template with deleted local immutable files."""
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
    ):
        migrate(local_client)


def test_update_template_dockerfile(local_client, mocker, template_update):
    """Test repository Dockerfile update."""
    template_update(docker=True, after_template_version="0.0.1")

    import renku

    mocker.patch.object(renku, "__version__", "0.0.2")

    migrate(local_client)

    dockerfile = (local_client.path / "Dockerfile").read_text()
    assert "0.0.2" in dockerfile


def test_update_from_template_with_new_variable(local_client, mocker, template_update):
    """Test repository update from a template with a new template variable required."""
    res = template_update()
    manifest = res["manifest"]
    manifest_path = res["manifest_path"]
    template_files = res["template_files"]

    # NOTE: Add new template variable
    manifest[0]["variables"]["__new_arbitrary_template_value__"] = None
    fetch_template = mocker.patch("renku.core.commands.init.fetch_template")
    fetch_template.return_value = (manifest, manifest_path, "renku", "0.0.2")

    for p in template_files:
        if p.is_dir():
            continue
        p.write_text(f"{p.read_text()}\nmodified")

    with pytest.raises(errors.TemplateUpdateError, match=r".*Can't update template, it now requires variable.*"):
        migrate(local_client)
