# -*- coding: utf-8 -*-
#
# Copyright 2018-2020- Swiss Data Science Center (SDSC)
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
"""Test ``init`` command."""
from pathlib import Path

import pytest
from tests.core.commands.test_init import TEMPLATE_ID, TEMPLATE_INDEX, \
    TEMPLATE_REF, TEMPLATE_URL

from renku.cli import cli
from renku.cli.init import create_template_sentence

INIT = ['init', 'test-new-project', '--template-id', TEMPLATE_ID]
INIT_REMOTE = [
    '--template-source', TEMPLATE_URL, '--template-ref', TEMPLATE_REF
]
INIT_FORCE = ['--force']
INIT_VARIABLES = ['--template-variables']
INIT_INDEX = ['init', 'test-new-project-2', '--template-index', TEMPLATE_INDEX]
INIT_ID = ['--template-id', TEMPLATE_ID]
LIST_TEMPLATES = ['--list-templates']


def test_template_selection_helpers():
    templates = [{
        'name': 'Template Python',
        'folder': 'folder_python',
        'description': 'Description Python'
    }, {
        'name': 'Template R',
        'folder': 'folder_R',
        'description': 'Description R'
    }]
    instructions = 'Please choose a template by typing the index'
    sentence = create_template_sentence(templates)
    stripped_sentence = ' '.join(sentence.split())
    assert (
        '1 folder_python Template Python: Description Python'
    ) in stripped_sentence
    assert '2 folder_R Template R: Description R' in stripped_sentence
    assert instructions not in stripped_sentence
    full_sentence = create_template_sentence(templates, True)
    assert instructions in full_sentence


def test_list_templates(isolated_runner):
    """Test listing templates."""
    new_project = Path('test-new-project')
    assert not new_project.exists()
    result = isolated_runner.invoke(cli, INIT + LIST_TEMPLATES)
    assert 0 == result.exit_code
    assert not new_project.exists()
    assert TEMPLATE_ID in result.output


def test_init(isolated_runner):
    """Test project initialization from template."""
    # create the project
    new_project = Path('test-new-project')
    assert not new_project.exists()
    result = isolated_runner.invoke(cli, INIT)
    assert 0 == result.exit_code
    assert new_project.exists()
    assert (new_project / '.renku').exists()
    assert (new_project / '.renku' / 'renku.ini').exists()
    assert (new_project / '.renku' / 'metadata.yml').exists()

    # try to re-create in the same folder
    result_re = isolated_runner.invoke(cli, INIT)
    assert 0 != result_re.exit_code

    # force re-create in the same folder
    result_re = isolated_runner.invoke(cli, INIT + INIT_FORCE)
    assert 0 == result.exit_code
    assert new_project.exists()
    assert (new_project / '.renku').exists()
    assert (new_project / '.renku' / 'renku.ini').exists()
    assert (new_project / '.renku' / 'metadata.yml').exists()

    # init using index instead of id
    new_project_2 = Path('test-new-project-2')
    result = isolated_runner.invoke(cli, INIT_INDEX)
    assert 0 == result.exit_code
    assert new_project_2.exists()
    assert (new_project_2 / '.renku').exists()
    assert (new_project_2 / '.renku' / 'renku.ini').exists()
    assert (new_project_2 / '.renku' / 'metadata.yml').exists()

    # verify both init lead to the same result
    template_files = [
        f for f in new_project.glob('**/*') if '.git' not in str(f)
    ]
    for template_file in template_files:
        expected_file = new_project_2 / template_file.relative_to(new_project)
        assert expected_file.exists()

    # verify providing both index and id fails
    result = isolated_runner.invoke(cli, INIT_INDEX + INIT_ID + INIT_FORCE)
    assert 2 == result.exit_code
    assert (
        'Use either --template-id or --template-index, not both'
    ) in result.output


def test_init_force_in_empty_dir(isolated_runner):
    """Run init --force in empty directory."""
    new_project = Path('test-new-project')
    assert not new_project.exists()
    result = isolated_runner.invoke(cli, INIT + INIT_FORCE)
    assert 0 == result.exit_code


def test_init_force_in_dirty_dir(isolated_runner):
    """Run init --force in dirty directory."""
    new_project = Path('test-new-project')
    assert not new_project.exists()

    new_project.mkdir(parents=True)
    random_file = new_project / 'random_file.txt'
    with random_file.open('w') as dest:
        dest.writelines(['random text'])
    assert random_file.exists()

    result = isolated_runner.invoke(cli, INIT + INIT_FORCE)
    assert random_file.exists()
    assert 0 == result.exit_code


def test_init_on_cloned_repo(isolated_runner, data_repository):
    """Run init --force in directory containing another repo."""
    new_project = Path('test-new-project')
    import shutil
    shutil.copytree(str(data_repository.working_dir), str(new_project))
    assert new_project.exists()

    # try to create in a dirty folder
    result = isolated_runner.invoke(cli, INIT)
    assert 0 != result.exit_code

    # force re-create in the same folder
    result = isolated_runner.invoke(cli, INIT + INIT_FORCE)
    assert 0 == result.exit_code
    assert new_project.exists()
    assert (new_project / '.renku').exists()
    assert (new_project / '.renku' / 'renku.ini').exists()
    assert (new_project / '.renku' / 'metadata.yml').exists()


@pytest.mark.integration
def test_init_remote(isolated_runner):
    """Test project initialization from a remote template."""
    # create the project
    new_project = Path('test-new-project')
    assert not new_project.exists()
    result = isolated_runner.invoke(cli, INIT + INIT_REMOTE)
    assert 0 == result.exit_code
    assert new_project.exists()
    assert (new_project / '.renku').exists()
    assert (new_project / '.renku' / 'renku.ini').exists()
    assert (new_project / '.renku' / 'metadata.yml').exists()


def test_init_with_variables(isolated_runner):
    """Test project initialization using custom metadata."""
    # create the project
    new_project = Path('test-new-project')
    assert not new_project.exists()
    metadata = 'not_dictionary'
    result = isolated_runner.invoke(cli, INIT + INIT_VARIABLES + [metadata])
    assert 0 != result.exit_code
    metadata = '{"correct": "dictionary"}'
    result = isolated_runner.invoke(cli, INIT + INIT_VARIABLES + [metadata])
    assert 0 == result.exit_code
