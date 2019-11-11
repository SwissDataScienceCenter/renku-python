# -*- coding: utf-8 -*-
#
# Copyright 2019 - Swiss Data Science Center (SDSC)
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
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from renku.core.commands.init import create_from_template, \
    fetch_remote_template, validate_template
from renku.core.management.config import RENKU_HOME

TEMPLATE_URL = (
    'https://github.com/SwissDataScienceCenter/renku-project-template'
)
TEMPLATE_FOLDER = 'python-minimal'
TEMPLATE_BRANCH = 'master'
METADATA = {'name': 'myname', 'description': 'nodesc'}
FAKE = 'NON_EXISTING'


def raises(error):
    """Wrapper around pytest.raises to support None."""
    if error:
        return pytest.raises(error)
    else:

        @contextmanager
        def not_raises():
            try:
                yield
            except Exception as e:
                raise e

        return not_raises()


@pytest.mark.parametrize(
    'url, folder, branch, result, error',
    [(TEMPLATE_URL, TEMPLATE_FOLDER, TEMPLATE_BRANCH, True, None),
     (TEMPLATE_URL, FAKE, TEMPLATE_BRANCH, None, ValueError),
     (TEMPLATE_URL, TEMPLATE_FOLDER, FAKE, None, ValueError)],
)
@pytest.mark.integration
def test_fetch_remote_template(url, folder, branch, result, error):
    """Test remote templates are correctly fetched."""
    with TemporaryDirectory() as tempdir:
        with raises(error):
            temp_folder = fetch_remote_template(
                url, folder, branch, Path(tempdir)
            )
            temp_path = Path(temp_folder)
            assert temp_path.is_dir()
            assert ((temp_path / 'Dockerfile').is_file())


@pytest.mark.integration
def test_validate_template():
    """Test template validation."""
    with TemporaryDirectory() as tempdir:
        # file error
        with raises(ValueError):
            validate_template(Path(tempdir))

        # folder error
        shutil.rmtree(tempdir)
        renku_dir = Path(tempdir, RENKU_HOME)
        renku_dir.mkdir(parents=True)
        with raises(ValueError):
            validate_template(Path(tempdir))

        # valid template
        shutil.rmtree(tempdir)
        template_folder = fetch_remote_template(
            TEMPLATE_URL, TEMPLATE_FOLDER, TEMPLATE_BRANCH, Path(tempdir)
        )
        assert validate_template(template_folder) is True


@pytest.mark.integration
def test_create_from_template(local_client):
    """Test repository creation from a template."""
    with TemporaryDirectory() as tempdir:
        template_folder = fetch_remote_template(
            TEMPLATE_URL, TEMPLATE_FOLDER, TEMPLATE_BRANCH, Path(tempdir)
        )
        create_from_template(
            template_folder, local_client, METADATA['name'],
            METADATA['description']
        )

        template_files = [
            f for f in local_client.path.glob('**/*') if '.git' not in str(f)
        ]
        for template_file in template_files:
            expected_file = template_folder / template_file.relative_to(
                local_client.path
            )
            assert expected_file.exists()
