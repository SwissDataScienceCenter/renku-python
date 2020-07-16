# -*- coding: utf-8 -*-
#
# Copyright 2019-2020 - Swiss Data Science Center (SDSC)
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
from tests.utils import raises

from renku.core import errors
from renku.core.commands.init import TEMPLATE_MANIFEST, create_from_template, \
    fetch_template, read_template_manifest, validate_template
from renku.core.management.config import RENKU_HOME

TEMPLATE_URL = (
    'https://github.com/SwissDataScienceCenter/renku-project-template'
)
TEMPLATE_ID = 'python-minimal'
TEMPLATE_INDEX = 1
TEMPLATE_REF = '0.1.11'
METADATA = {'description': 'nodesc'}
FAKE = 'NON_EXISTING'
NAME = 'myname'

template_local = Path(pkg_resources.resource_filename('renku', 'templates'))


@pytest.mark.parametrize(
    'url, ref, result, error', [(TEMPLATE_URL, TEMPLATE_REF, True, None),
                                (FAKE, TEMPLATE_REF, None, errors.GitError),
                                (TEMPLATE_URL, FAKE, None, errors.GitError)]
)
@pytest.mark.integration
def test_fetch_template(url, ref, result, error):
    """Test fetching a template.

    It fetches a template from remote and verifies that the manifest
    file is there.
    """
    with TemporaryDirectory() as tempdir:
        with raises(error):
            manifest_file = fetch_template(url, ref, Path(tempdir))
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
            '-\n'
            '  folder: first\n'
            '  name: Basic Project 1\n'
            '  description: Description 1\n'
            '  variables: {}\n'
            '-\n'
            '  folder: second\n'
            '  name: Basic Project 2\n'
            '  description: Description 2\n'
            '  variables:\n'
            '    custom: Custom Value\n'
        )

        manifest = read_template_manifest(Path(tempdir), checkout=False)
        assert 2 == len(manifest)
        assert 'first' == manifest[0]['folder']
        assert 'second' == manifest[1]['folder']
        assert 'Basic Project 1' == manifest[0]['name']
        assert 'Description 2' == manifest[1]['description']

        variables1 = manifest[0]['variables']
        variables2 = manifest[1]['variables']
        assert 0 == len(variables1)
        assert 1 == len(variables2)
        assert 1 == len(variables2.keys())
        assert 'custom' in variables2.keys()
        assert 'Custom Value' == variables2['custom']

        template_file.write_text(
            '-\n'
            '  folder: first\n'
            '  description: Description 1\n'
        )
        with raises(errors.InvalidTemplateError):
            manifest = read_template_manifest(Path(tempdir), checkout=False)

        template_file.write_text(
            '-\n'
            '  name: Basic Project 2\n'
            '  description: Description 2\n'
        )
        with raises(errors.InvalidTemplateError):
            manifest = read_template_manifest(Path(tempdir), checkout=False)


@pytest.mark.integration
def test_fetch_template_and_read_manifest():
    """Test template fetch and manifest reading.

    It fetches a local template, reads the manifest, checkouts the
    template folders and verify they exist.
    """
    with TemporaryDirectory() as tempdir:
        template_path = Path(tempdir)
        fetch_template(TEMPLATE_URL, TEMPLATE_REF, template_path)
        manifest = read_template_manifest(template_path, checkout=True)
        for template in manifest:
            template_folder = template_path / template['folder']
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
            template_folder = temppath / template['folder']
            assert validate_template(template_folder) is True


def test_create_from_template(local_client):
    """Test repository creation from a template.

    It creates a renku projects from one of the local templates and it verifies
    the data are properly copied to the new renku project folder.
    """
    with TemporaryDirectory() as tempdir:
        temppath = Path(tempdir) / 'local'
        shutil.copytree(str(template_local), str(temppath))
        manifest = read_template_manifest(temppath)
        template_path = temppath / manifest[0]['folder']
        create_from_template(template_path, local_client, NAME, METADATA)
        template_files = [
            f
            for f in local_client.path.glob('**/*') if '.git' not in str(f) and
            not str(f).endswith('.renku/metadata.yml')
        ]
        for template_file in template_files:
            expected_file = template_path / template_file.relative_to(
                local_client.path
            )
            assert expected_file.exists()


def test_template_filename(local_client):
    """Test using a template with dynamic filenames.
    """
    with TemporaryDirectory() as tempdir:
        template_folder = Path(tempdir) / 'first'

        template_folder.mkdir(parents=True)

        template_file = template_folder / '{{ name }}.r'
        template_file.write_text('{{ name }}')

        (local_client.path / '.renku').mkdir()

        create_from_template(template_folder, local_client, name='test')

        assert (local_client.path / 'test.r').exists()
