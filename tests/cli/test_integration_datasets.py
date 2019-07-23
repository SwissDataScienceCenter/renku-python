# -*- coding: utf-8 -*-
#
# Copyright 2017-2019 - Swiss Data Science Center (SDSC)
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
"""Integration tests for dataset command."""
import os

import git
import pytest

from renku import cli


@pytest.mark.integration
def test_dataset_import_real_doi(runner, project):
    """Test dataset import for existing DOI."""
    result = runner.invoke(
        cli.cli, ['dataset', 'import', '10.5281/zenodo.597964'], input='y'
    )
    assert 0 == result.exit_code
    assert 'OK' in result.output

    result = runner.invoke(cli.cli, ['dataset'])
    assert 0 == result.exit_code
    assert 'pyndl_naive_discriminat_v064' in result.output
    assert 'K.Sering,M.Weitz,D.Künstle,L.Schneider' in result.output


@pytest.mark.parametrize(
    'doi', [
        ('10.5281/zenodo.597964', 'y'),
        ('10.5281/zenodo.3236928', 'n'),
        ('10.5281/zenodo.2671633', 'n'),
        ('10.5281/zenodo.3237420', 'n'),
        ('10.5281/zenodo.3236928', 'n'),
        ('10.5281/zenodo.3188334', 'y'),
        ('10.5281/zenodo.3236928', 'n'),
        ('10.5281/zenodo.2669459', 'n'),
        ('10.5281/zenodo.2371189', 'n'),
        ('10.5281/zenodo.2651343', 'n'),
        ('10.5281/zenodo.1467859', 'n'),
        ('10.5281/zenodo.3240078', 'n'),
        ('10.5281/zenodo.3240053', 'n'),
        ('10.5281/zenodo.3240010', 'n'),
        ('10.5281/zenodo.3240012', 'n'),
        ('10.5281/zenodo.3240006', 'n'),
        ('10.5281/zenodo.3239996', 'n'),
        ('10.5281/zenodo.3239256', 'n'),
        ('10.5281/zenodo.3237813', 'n'),
        ('10.5281/zenodo.3239988', 'y'),
        ('10.5281/zenodo.3239986', 'n'),
        ('10.5281/zenodo.3239984', 'n'),
        ('10.5281/zenodo.3239982', 'n'),
        ('10.5281/zenodo.3239980', 'n'),
        ('10.5281/zenodo.3188334', 'y'),
    ]
)
@pytest.mark.integration
def test_dataset_import_real_param(doi, runner, project):
    """Test dataset import and check metadata parsing."""
    result = runner.invoke(
        cli.cli, ['dataset', 'import', doi[0]], input=doi[1]
    )
    assert 0 == result.exit_code

    if 'y' == doi[1]:
        assert 'OK' in result.output

    result = runner.invoke(cli.cli, ['dataset'])
    assert 0 == result.exit_code


@pytest.mark.integration
def test_dataset_import_real_doi_warnings(runner, project):
    """Test dataset import for existing DOI and dataset"""
    result = runner.invoke(
        cli.cli, ['dataset', 'import', '10.5281/zenodo.597964'], input='y'
    )
    assert 0 == result.exit_code
    assert 'Warning: Newer version found.' in result.output
    assert 'OK'

    result = runner.invoke(
        cli.cli, ['dataset', 'import', '10.5281/zenodo.597964'], input='y\ny'
    )
    assert 0 == result.exit_code
    assert 'Warning: Newer version found.' in result.output
    assert 'Warning: This dataset already exists.' in result.output
    assert 'OK' in result.output

    result = runner.invoke(
        cli.cli, ['dataset', 'import', '10.5281/zenodo.597964'], input='y\nn'
    )
    assert 0 == result.exit_code
    assert 'Warning: Newer version found.' in result.output
    assert 'Warning: This dataset already exists.' in result.output
    assert 'OK' not in result.output

    result = runner.invoke(cli.cli, ['dataset'])
    assert 0 == result.exit_code
    assert 'pyndl_naive_discriminat_v064' in result.output
    assert 'K.Sering,M.Weitz,D.Künstle,L.Schneider' in result.output


@pytest.mark.integration
def test_dataset_import_fake_doi(runner, project):
    """Test error raising for non-existing DOI."""
    result = runner.invoke(
        cli.cli, ['dataset', 'import', '10.5281/zenodo.5979642342'], input='y'
    )
    assert 2 == result.exit_code
    assert 'URI not found.' in result.output


@pytest.mark.integration
def test_dataset_import_real_http(runner, project):
    """Test dataset import through HTTPS."""
    result = runner.invoke(
        cli.cli, ['dataset', 'import', 'https://zenodo.org/record/2621208'],
        input='y'
    )
    assert 0 == result.exit_code
    assert 'OK' in result.output


@pytest.mark.integration
def test_dataset_import_fake_http(runner, project):
    """Test dataset import through HTTPS."""
    result = runner.invoke(
        cli.cli, ['dataset', 'import', 'https://zenodo.org/record/2621201248'],
        input='y'
    )
    assert 2 == result.exit_code
    assert 'URI not found.' in result.output


@pytest.mark.integration
def test_dataset_export_upload_file(
    runner, project, tmpdir, client, zenodo_sandbox
):
    """Test successful uploading of a file to Zenodo deposit."""
    result = runner.invoke(cli.cli, ['dataset', 'create', 'my-dataset'])
    assert 0 == result.exit_code
    assert 'OK' in result.output

    # create data file
    new_file = tmpdir.join('datafile.csv')
    new_file.write('1,2,3')

    # add data to dataset
    result = runner.invoke(
        cli.cli, ['dataset', 'add', 'my-dataset',
                  str(new_file)]
    )
    assert 0 == result.exit_code

    with client.with_dataset(name='my-dataset') as dataset:
        dataset.description = 'awesome dataset'
        dataset.creator[0].affiliation = 'eth'

    data_repo = git.Repo(str(project))
    data_repo.git.add(update=True)
    data_repo.index.commit('metadata updated')

    result = runner.invoke(
        cli.cli, ['dataset', 'export', 'my-dataset', 'zenodo']
    )

    assert 0 == result.exit_code
    assert 'Exported to:' in result.output
    assert 'zenodo.org/deposit' in result.output


@pytest.mark.integration
def test_dataset_export_upload_multiple(
    runner, project, tmpdir, client, zenodo_sandbox
):
    """Test successful uploading of a files to Zenodo deposit."""
    result = runner.invoke(cli.cli, ['dataset', 'create', 'my-dataset'])
    assert 0 == result.exit_code
    assert 'OK' in result.output

    # create data file
    paths = []
    for i in range(3):
        new_file = tmpdir.join('file_{0}'.format(i))
        new_file.write(str(i))
        paths.append(str(new_file))

    # add data
    result = runner.invoke(
        cli.cli,
        ['dataset', 'add', 'my-dataset'] + paths,
        catch_exceptions=False,
    )
    assert 0 == result.exit_code

    with client.with_dataset(name='my-dataset') as dataset:
        dataset.description = 'awesome dataset'
        dataset.creator[0].affiliation = 'eth'

    data_repo = git.Repo(str(project))
    data_repo.git.add(update=True)
    data_repo.index.commit('metadata updated')

    result = runner.invoke(
        cli.cli, ['dataset', 'export', 'my-dataset', 'zenodo']
    )

    assert 0 == result.exit_code
    assert 'Exported to:' in result.output
    assert 'zenodo.org/deposit' in result.output


@pytest.mark.integration
def test_dataset_export_upload_failure(runner, project, tmpdir, client):
    """Test failed uploading of a file to Zenodo deposit."""
    result = runner.invoke(cli.cli, ['dataset', 'create', 'my-dataset'])
    assert 0 == result.exit_code
    assert 'OK' in result.output

    # create data file
    new_file = tmpdir.join('datafile.csv')
    new_file.write('1,2,3')

    # add data to dataset
    result = runner.invoke(
        cli.cli, ['dataset', 'add', 'my-dataset',
                  str(new_file)]
    )
    assert 0 == result.exit_code

    result = runner.invoke(
        cli.cli, ['dataset', 'export', 'my-dataset', 'zenodo']
    )
    assert 2 == result.exit_code
    assert 'metadata.creators.0.affiliation' in result.output
    assert 'metadata.description' in result.output


@pytest.mark.integration
def test_dataset_export_published_url(
    runner, project, tmpdir, client, zenodo_sandbox
):
    """Test publishing of dataset."""
    result = runner.invoke(cli.cli, ['dataset', 'create', 'my-dataset'])
    assert 0 == result.exit_code
    assert 'OK' in result.output

    # create data file
    new_file = tmpdir.join('datafile.csv')
    new_file.write('1,2,3')

    # add data to dataset
    result = runner.invoke(
        cli.cli, ['dataset', 'add', 'my-dataset',
                  str(new_file)]
    )
    assert 0 == result.exit_code

    with client.with_dataset(name='my-dataset') as dataset:
        dataset.description = 'awesome dataset'
        dataset.creator[0].affiliation = 'eth'

    data_repo = git.Repo(str(project))
    data_repo.git.add(update=True)
    data_repo.index.commit('metadata updated')

    result = runner.invoke(
        cli.cli, ['dataset', 'export', 'my-dataset', 'zenodo', '--publish']
    )

    assert 0 == result.exit_code
    assert 'Exported to:' in result.output
    assert 'zenodo.org/record' in result.output


@pytest.mark.integration
def test_export_dataset_wrong_provider(
    runner, project, tmpdir, client, zenodo_sandbox
):
    """Test non-existing provider."""
    result = runner.invoke(cli.cli, ['dataset', 'create', 'my-dataset'])
    assert 0 == result.exit_code
    assert 'OK' in result.output

    # create data file
    new_file = tmpdir.join('datafile.csv')
    new_file.write('1,2,3')

    # add data to dataset
    result = runner.invoke(
        cli.cli, ['dataset', 'add', 'my-dataset',
                  str(new_file)]
    )
    assert 0 == result.exit_code

    result = runner.invoke(
        cli.cli, ['dataset', 'export', 'my-dataset', 'notzenodo']
    )
    assert 2 == result.exit_code
    assert 'Unknown provider.' in result.output


@pytest.mark.integration
def test_dataset_export(runner, client, project):
    """Check dataset not found exception raised."""
    result = runner.invoke(
        cli.cli, ['dataset', 'export', 'doesnotexists', 'somewhere']
    )
    assert 2 == result.exit_code
    assert 'Dataset not found.' in result.output


@pytest.mark.integration
def test_export_dataset_unauthorized(
    runner, project, client, tmpdir, zenodo_sandbox
):
    """Test unauthorized exception raised."""
    client.set_value('zenodo', 'access_token', 'not-a-token')
    result = runner.invoke(cli.cli, ['dataset', 'create', 'my-dataset'])
    assert 0 == result.exit_code
    assert 'OK' in result.output

    # create data file
    new_file = tmpdir.join('datafile.csv')
    new_file.write('1,2,3')

    # add data to dataset
    result = runner.invoke(
        cli.cli, ['dataset', 'add', 'my-dataset',
                  str(new_file)]
    )
    assert 0 == result.exit_code

    result = runner.invoke(
        cli.cli, ['dataset', 'export', 'my-dataset', 'zenodo']
    )
    assert 2 == result.exit_code
    assert 'Access unauthorized - update access token.' in result.output

    secret = client.get_value('zenodo', 'secret')
    assert secret is None


@pytest.mark.integration
@pytest.mark.parametrize(
    'remotes', [
        {
            'url': (
                'https://github.com'
                '/SwissDataScienceCenter/renku-python.git'
            ),
            'filename': 'README.rst',
            'expected_path': 'data/dataset/README.rst'
        },
        {
            'url': (
                'https://gist.githubusercontent.com'
                '/jsam/24e3763fe4912ddb5c3a0fe411002f21'
                '/raw/ac45b51b5d6e20794e2ac73df5e309fa26e2f73a'
                '/gistfile1.txt?foo=bar'
            ),
            'filename': 'gistfile1.txt',
            'expected_path': 'data/dataset/gistfile1.txt'
        },
    ]
)
def test_datasets_remote_import(
    remotes, data_file, data_repository, runner, project, client
):
    """Test importing data into a dataset."""
    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'dataset'])
    assert 0 == result.exit_code
    assert 'OK' in result.output

    with client.with_dataset('dataset') as dataset:
        assert dataset.name == 'dataset'

    # add data
    result = runner.invoke(
        cli.cli, ['dataset', 'add', 'dataset',
                  str(data_file)]
    )
    assert 0 == result.exit_code
    assert os.stat(
        os.path.join('data', 'dataset', os.path.basename(str(data_file)))
    )

    # add data from a git repo via http
    result = runner.invoke(
        cli.cli, [
            'dataset', 'add', 'dataset', '--target', remotes['filename'],
            remotes['url']
        ]
    )
    assert 0 == result.exit_code
    assert os.stat(remotes['expected_path'])

    # add data from local git repo
    result = runner.invoke(
        cli.cli, [
            'dataset',
            'add',
            'dataset',
            '-t',
            'dir2/file2',
            os.path.dirname(data_repository.git_dir),
        ]
    )
    assert 0 == result.exit_code


@pytest.mark.integration
@pytest.mark.parametrize(
    'remotes', [
        {
            'url': (
                'https://github.com'
                '/SwissDataScienceCenter/renku-python.git'
            ),
            'filename': 'README.rst',
            'expected_path': 'data/dataset/README.rst'
        },
        {
            'url': (
                'https://gist.githubusercontent.com'
                '/jsam/24e3763fe4912ddb5c3a0fe411002f21'
                '/raw/ac45b51b5d6e20794e2ac73df5e309fa26e2f73a'
                '/gistfile1.txt?foo=bar'
            ),
            'filename': 'gistfile1.txt',
            'expected_path': 'data/dataset/gistfile1.txt'
        },
    ]
)
def test_datasets_import_target(
    remotes, data_file, data_repository, runner, project, client
):
    """Test importing data into a dataset."""
    # create a dataset
    result = runner.invoke(cli.cli, ['dataset', 'create', 'dataset'])
    assert 0 == result.exit_code
    assert 'OK' in result.output

    with client.with_dataset('dataset') as dataset:
        assert dataset.name == 'dataset'

    # add data
    result = runner.invoke(
        cli.cli,
        ['dataset', 'add', 'dataset',
         str(data_file)],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code
    assert os.stat(
        os.path.join('data', 'dataset', os.path.basename(str(data_file)))
    )

    # add data from a git repo via http
    result = runner.invoke(
        cli.cli,
        [
            'dataset', 'add', 'dataset', '--target', remotes['filename'],
            remotes['url']
        ],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code
    assert os.stat(remotes['expected_path'])

    # add data from local git repo
    result = runner.invoke(
        cli.cli,
        [
            'dataset',
            'add',
            'dataset',
            '-t',
            'dir2/file2',
            os.path.dirname(data_repository.git_dir),
        ],
    )
    assert 0 == result.exit_code
