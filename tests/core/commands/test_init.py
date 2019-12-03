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
from git.exc import GitCommandError

from renku.core.commands.init import TEMPLATE_MANIFEST, create_from_template, \
    fetch_template, read_template_manifest, validate_template
from renku.core.management.config import RENKU_HOME

TEMPLATE_URL = (
    'https://github.com/SwissDataScienceCenter/renku-project-template'
)
TEMPLATE_NAME = 'Basic Python Project'
TEMPLATE_REF = '0.1.4'
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
    'url, ref, result, error', [(TEMPLATE_URL, TEMPLATE_REF, True, None),
                                (FAKE, TEMPLATE_REF, None, GitCommandError),
                                (TEMPLATE_URL, FAKE, None, GitCommandError)]
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
            assert manifest_file == Path(tempdir) / TEMPLATE_MANIFEST
            assert manifest_file.exists()


def test_read_template_manifest():
    """Test reading template manifest file.

    It creates a fake manifest file and it verifies it's read propery.
    """
    with TemporaryDirectory() as tempdir:
        template_file = Path(tempdir) / TEMPLATE_MANIFEST
        with template_file.open('w') as fp:
            fp.writelines([
                '-\n', '  folder: first\n', '  name: Basic Project 1\n',
                '  description: Description 1\n', '-\n', '  folder: second\n',
                '  name: Basic Project 2\n', '  description: Description 2\n'
            ])

        manifest = read_template_manifest(Path(tempdir), checkout=False)
        assert len(manifest) == 2
        assert manifest[0]['folder'] == 'first'
        assert manifest[1]['folder'] == 'second'
        assert manifest[0]['name'] == 'Basic Project 1'
        assert manifest[1]['description'] == 'Description 2'


@pytest.mark.integration
def test_fetch_template_and_read_manifest():
    """Test template fetch and manifest reading.

    It fetches a remote template, reads the manifest, checkouts the
    template folders and verify they exist.
    """
    with TemporaryDirectory() as tempdir:
        template_path = Path(tempdir)
        fetch_template(TEMPLATE_URL, TEMPLATE_REF, template_path)
        manifest = read_template_manifest(template_path, checkout=True)
        for template in manifest:
            template_folder = template_path / template['folder']
            assert template_folder.exists()


@pytest.mark.integration
def test_validate_template():
    """Test template validation.

    It fetches a remote template, reads the manifest, checkouts the
    template folders and validates each template.
    """
    with TemporaryDirectory() as tempdir:
        temppath = Path(tempdir)
        # file error
        with raises(ValueError):
            validate_template(temppath)

        # folder error
        shutil.rmtree(tempdir)
        renku_dir = temppath / RENKU_HOME
        renku_dir.mkdir(parents=True)
        with raises(ValueError):
            validate_template(temppath)

        # valid template
        shutil.rmtree(tempdir)
        fetch_template(TEMPLATE_URL, TEMPLATE_REF, temppath)
        manifest = read_template_manifest(Path(tempdir), checkout=True)
        for template in manifest:
            template_folder = temppath / template['folder']
            assert validate_template(template_folder) is True


@pytest.mark.integration
def test_create_from_template(local_client):
    """Test repository creation from a template.

    It fetches and checkout a template, it creates a renku projects from
    one of the template and it verifies the data are properly copied to
    the new renku project folder.
    """
    with TemporaryDirectory() as tempdir:
        temppath = Path(tempdir)
        fetch_template(TEMPLATE_URL, TEMPLATE_REF, temppath)
        manifest = read_template_manifest(temppath, checkout=True)
        template_path = temppath / manifest[0]['folder']
        create_from_template(
            template_path, local_client, METADATA['name'],
            METADATA['description']
        )
        template_files = [
            f for f in local_client.path.glob('**/*') if '.git' not in str(f)
        ]
        for template_file in template_files:
            expected_file = template_path / template_file.relative_to(
                local_client.path
            )
            assert expected_file.exists()


@pytest.mark.integration
def test_setup_download_templates():
    print('********** DOWNLOAD TEMPLATES 2')
    from renku.core.commands.init import fetch_template, \
        read_template_manifest

    with TemporaryDirectory() as tempdir:
        # download and extract template data
        temppath = Path(tempdir)
        print('downloading Renku templates...')
        fetch_template(TEMPLATE_URL, TEMPLATE_REF, temppath)
        read_template_manifest(temppath, checkout=True)

        # copy templates
        current_path = Path.cwd()
        template_path = current_path / 'renku_data'  # / 'templates'
        if template_path.exists():
            shutil.rmtree(str(template_path))
        shutil.copytree(
            temppath, template_path, ignore=shutil.ignore_patterns('.git')
        )
        # return [(
        #     str(current_path / 'renku_data'), [
        #         str(current.relative_to(current_path / 'renku_data'))
        #         for current in list((current_path / 'renku_data').rglob('*'))
        #         if current.is_file()
        #     ]
        # )]
        ret = [(
            'templates', [
                str(current.relative_to(template_path))
                for current in list((template_path).rglob('*'))
                if current.is_file()
            ]
        )]
        assert ret
