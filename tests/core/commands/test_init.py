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

from renku.core.commands.init import create_from_template
from renku.core.models.template import TEMPLATE_MANIFEST, TemplatesManifest

try:
    import importlib_resources
except ImportError:
    import importlib.resources as importlib_resources

ref = importlib_resources.files("renku") / "templates"
with importlib_resources.as_file(ref) as path:
    template_local = path


def test_create_from_template(local_client, template, injected_local_client_with_database):
    """Test repository creation from a template.

    It creates a renku projects from one of the local templates and it verifies
    the data are properly copied to the new renku project folder.
    """
    local_client.init_repository()

    with TemporaryDirectory() as tempdir:
        temppath = Path(tempdir) / "local"
        shutil.copytree(str(template_local), str(temppath))
        manifest = TemplatesManifest.from_path(temppath / TEMPLATE_MANIFEST).get_raw_content()
        template_path = temppath / manifest[0]["folder"]
        create_from_template(
            template_path, local_client, "name", {**template["default_metadata"], **template["metadata"]}
        )
        template_files = [
            f
            for f in local_client.path.glob("**/*")
            if ".git" not in str(f)
            and ".renku/metadata" not in str(f)
            and not str(f).endswith(".renku/template_checksums.json")
        ]
        for template_file in template_files:
            expected_file = template_path / template_file.relative_to(local_client.path)
            assert expected_file.exists()


# TODO Move this to templates
def test_template_filename(local_client, template, injected_local_client_with_database):
    """Test using a template with dynamic filenames."""
    local_client.init_repository()

    with TemporaryDirectory() as tempdir:
        template_folder = Path(tempdir) / "first"

        template_folder.mkdir(parents=True)

        template_file = template_folder / "{{ name }}.r"
        template_file.write_text("{{ name }}")

        (local_client.path / ".renku").mkdir()

        create_from_template(template_folder, local_client, name="test", metadata=template["default_metadata"])

        assert (local_client.path / "test.r").exists()
