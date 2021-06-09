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

import os
import shutil
from pathlib import Path

import pytest

from renku.version import __version__ as renku_version


@pytest.fixture
def directory_tree_files():
    """List of files for ``directory_tree`` fixture."""
    return ["file1", os.path.join("dir1", "file2"), os.path.join("dir1", "file3")]


@pytest.fixture()
def directory_tree(tmp_path, directory_tree_files):
    """Create a test directory tree."""
    # initialize
    base = tmp_path / "directory_tree"
    for path in directory_tree_files:
        path = base / path
        path.parent.mkdir(parents=True, exist_ok=True)

        if str(path).endswith("file1"):
            path.write_text("123")
        elif str(path).endswith("file2"):
            path.write_text("456")
        elif str(path).endswith("file3"):
            path.write_text("789")

    return base


@pytest.fixture
def data_repository(directory_tree):
    """Create a test repo."""
    from git import Actor, Repo

    # NOTE: Initialize step.
    repo = Repo.init(str(directory_tree))

    # NOTE: Add a file step.
    repo.index.add([str(directory_tree / "file1")])
    repo.index.commit("test commit", author=Actor("me", "me@example.com"))

    # NOTE: Commit changes to the same file with a different user.
    directory_tree.joinpath("file1").write_text("5678")
    repo.index.add([str(directory_tree / "file1")])
    repo.index.commit("test commit", author=Actor("me2", "me2@example.com"))

    # NOTE: Commit a second file.
    repo.index.add([str(directory_tree / "dir1" / "file2")])
    repo.index.commit("test commit", author=Actor("me", "me@example.com"))

    # NOTE: Return the repo.
    return repo


@pytest.fixture()
def template():
    """Yield template data."""
    template = {
        "url": "https://github.com/SwissDataScienceCenter/renku-project-template",
        "id": "python-minimal",
        "index": 1,
        "ref": "master",
        "metadata": {"description": "nodesc"},
        "default_metadata": {
            "__template_source__": "renku",
            "__template_ref__": "master",
            "__template_id__": "python-minimal",
            "__namespace__": "",
            "__repository__": "",
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
    }

    commands = {
        "init": ["init", "."],
        "init_test": ["init", data["test_project"]],
        "init_alt": ["init", data["test_project_alt"]],
        "remote": ["--template-source", template["url"], "--template-ref", template["ref"]],
        "id": ["--template-id", template["id"]],
        "index": ["--template-index", template["index"]],
        "force": ["--force"],
        "list": ["--list-templates"],
        "parameters": ["--parameter", "p1=v1", "--parameter", "p2=v2"],
        "parameters_equal_missing": ["--parameter", "p3:v3"],
        "parameters_equal_early": ["--parameter", "=p4v3"],
        "confirm": len(set(template["metadata"].keys())) * "\n",
    }

    yield data, commands


@pytest.fixture
def template_update(tmpdir, local_client, mocker, template):
    """Create a mocked template for updates."""

    def _template_update(immutable_files=None, docker=False, after_template_version="0.0.2"):
        """Fetches an updatable template with various options."""
        import pkg_resources

        from renku.core.commands.init import create_from_template, read_template_manifest

        template_local = Path(pkg_resources.resource_filename("renku", "templates"))

        # NOTE: get template
        tempdir = tmpdir.mkdir("template")
        temppath = Path(tempdir) / "local"

        shutil.copytree(str(template_local), str(temppath))
        manifest = read_template_manifest(temppath)
        template_path = temppath / manifest[0]["folder"]

        if docker:
            import renku

            mocker.patch.object(renku, "__version__", return_value="0.0.1")

            # TODO: remove this once the renku template contains RENKU_VERSION
            dockerfile_path = template_path / "Dockerfile"
            dockerfile = dockerfile_path.read_text()
            dockerfile_path.write_text(f"ARG RENKU_VERSION=0.0.1\n{dockerfile}")

        local_client.init_repository()

        # NOTE: init project from template
        create_from_template(
            template_path,
            local_client,
            "name",
            {**template["default_metadata"], **template["metadata"]},
            template_version="0.0.1",
            immutable_template_files=immutable_files or [],
            automated_update=True,
        )

        project_files = [
            f
            for f in local_client.path.glob("**/*")
            if ".git" not in str(f)
            and not str(f).endswith(".renku/metadata.yml")
            and not str(f).endswith(".renku/template_checksums.json")
        ]

        template_files = []
        for project_file in project_files:
            expected_file = template_path / project_file.relative_to(local_client.path)
            template_files.append(expected_file)
            assert expected_file.exists()

        fetch_template = mocker.patch("renku.core.commands.init.fetch_template")
        fetch_template.return_value = (manifest, temppath, "renku", after_template_version)

        return {
            "template_files": template_files,
            "project_files": project_files,
            "manifest": manifest,
            "manifest_path": temppath,
            "template_path": template_path,
        }

    yield _template_update


@pytest.fixture
def no_lfs_size_limit(client):
    """Configure environment track all files in LFS independent of size."""
    client.set_value("renku", "lfs_threshold", "0b")
    client.repo.git.add(".renku/renku.ini")
    client.repo.index.commit("update renku.ini")

    yield client


@pytest.fixture
def large_file(tmp_path, client):
    """A file larger than the minimum LFS file size."""
    path = tmp_path / "large-file"
    with open(path, "w") as file_:
        file_.seek(client.minimum_lfs_file_size)
        file_.write("some data")

    yield path
