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

from renku.cli import cli


@pytest.mark.parametrize(
    'doi', [{
        'doi': '10.5281/zenodo.2658634',
        'input': 'y',
        'file': 'pyndl_naive_discriminat_v064',
        'creator': 'K.Sering,M.Weitz,D.Künstle,L.Schneider',
        'version': 'v0.6.4'
    }, {
        'doi': '10.7910/DVN/S8MSVF',
        'input': 'y',
        'file': 'hydrogen_mapping_laws_a_1',
        'creator': 'M.Trevor',
        'version': '1'
    }]
)
@pytest.mark.integration
def test_dataset_import_real_doi(runner, project, doi):
    """Test dataset import for existing DOI."""
    result = runner.invoke(
        cli, ['dataset', 'import', doi['doi']], input=doi['input']
    )
    assert 0 == result.exit_code
    assert 'OK' in result.output

    result = runner.invoke(cli, ['dataset'])

    assert 0 == result.exit_code
    assert doi['file'] in result.output
    assert doi['creator'] in result.output

    result = runner.invoke(cli, ['dataset', 'ls-tags', doi['file']])
    assert 0 == result.exit_code
    assert doi['version'] in result.output


@pytest.mark.parametrize(
    'doi', [
        ('10.5281/zenodo.3239980', 'n'),
        ('10.5281/zenodo.3188334', 'y'),
        ('10.7910/DVN/TJCLKP', 'n'),
        ('10.7910/DVN/S8MSVF', 'y'),
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
    ]
)
@pytest.mark.integration
def test_dataset_import_real_param(doi, runner, project):
    """Test dataset import and check metadata parsing."""
    result = runner.invoke(cli, ['dataset', 'import', doi[0]], input=doi[1])

    if 'y' == doi[1]:
        assert 0 == result.exit_code
        assert 'OK' in result.output
    else:
        assert 1 == result.exit_code

    result = runner.invoke(cli, ['dataset'])
    assert 0 == result.exit_code


@pytest.mark.parametrize(
    'doi', [
        ('10.5281/zenodo.3239984', 'n'),
        ('zenodo.org/record/3239986', 'n'),
        ('10.5281/zenodo.3239982', 'n'),
    ]
)
@pytest.mark.integration
def test_dataset_import_uri_404(doi, runner, project):
    """Test dataset import and check that correct exception is raised."""
    result = runner.invoke(cli, ['dataset', 'import', doi[0]], input=doi[1])
    assert 2 == result.exit_code

    result = runner.invoke(cli, ['dataset'])
    assert 0 == result.exit_code


@pytest.mark.integration
def test_dataset_import_real_doi_warnings(runner, project):
    """Test dataset import for existing DOI and dataset"""
    result = runner.invoke(
        cli, ['dataset', 'import', '10.5281/zenodo.1438326'], input='y'
    )
    assert 0 == result.exit_code
    assert 'Warning: Newer version found' in result.output
    assert 'OK'

    result = runner.invoke(
        cli, ['dataset', 'import', '10.5281/zenodo.1438326'], input='y\ny'
    )
    assert 0 == result.exit_code
    assert 'Warning: Newer version found' in result.output
    assert 'Warning: This dataset already exists.' in result.output
    assert 'OK' in result.output

    result = runner.invoke(
        cli, ['dataset', 'import', '10.5281/zenodo.597964'], input='y\n'
    )
    assert 0 == result.exit_code
    assert 'Warning: Newer version found' not in result.output
    assert 'Warning: This dataset already exists.' not in result.output
    assert 'OK' in result.output

    result = runner.invoke(cli, ['dataset'])

    assert 0 == result.exit_code
    assert 'pyndl_naive_discriminat_v064' in result.output
    assert 'K.Sering,M.Weitz,D.Künstle,L.Schneider' in result.output


@pytest.mark.parametrize(
    'doi', [('10.5281/zenodo.5979642342', 'Zenodo'),
            ('10.7910/DVN/S8MSVFXXXX', 'DVN')]
)
@pytest.mark.integration
def test_dataset_import_fake_doi(runner, project, doi):
    """Test error raising for non-existing DOI."""
    result = runner.invoke(cli, ['dataset', 'import', doi[0]], input='y')

    assert 2 == result.exit_code
    assert 'URI not found.' in result.output \
           or 'Provider {} not found'.format(doi[1]) in result.output


@pytest.mark.parametrize(
    'url', [
        'https://zenodo.org/record/2621208',
        (
            'https://dataverse.harvard.edu/dataset.xhtml'
            '?persistentId=doi:10.7910/DVN/S8MSVF'
        )
    ]
)
@pytest.mark.integration
def test_dataset_import_real_http(runner, project, url):
    """Test dataset import through HTTPS."""
    result = runner.invoke(cli, ['dataset', 'import', url], input='y')

    assert 0 == result.exit_code
    assert 'OK' in result.output


@pytest.mark.parametrize(
    'url', [
        'https://zenodo.org/record/2621201248',
        'https://dataverse.harvard.edu/dataset.xhtml' +
        '?persistentId=doi:10.7910/DVN/S8MSVFXXXX'
    ]
)
@pytest.mark.integration
def test_dataset_import_fake_http(runner, project, url):
    """Test dataset import through HTTPS."""
    result = runner.invoke(cli, ['dataset', 'import', url], input='y')

    assert 2 == result.exit_code
    assert 'URI not found.' in result.output


@pytest.mark.integration
def test_dataset_export_upload_file(
    runner, project, tmpdir, client, zenodo_sandbox
):
    """Test successful uploading of a file to Zenodo deposit."""
    result = runner.invoke(cli, ['dataset', 'create', 'my-dataset'])

    assert 0 == result.exit_code
    assert 'OK' in result.output

    # create data file
    new_file = tmpdir.join('datafile.csv')
    new_file.write('1,2,3')

    # add data to dataset
    result = runner.invoke(
        cli, ['dataset', 'add', 'my-dataset',
              str(new_file)]
    )
    assert 0 == result.exit_code

    with client.with_dataset(name='my-dataset') as dataset:
        dataset.description = 'awesome dataset'
        dataset.creator[0].affiliation = 'eth'

    data_repo = git.Repo(str(project))
    data_repo.git.add(update=True)
    data_repo.index.commit('metadata updated')

    result = runner.invoke(cli, ['dataset', 'export', 'my-dataset', 'zenodo'])

    assert 0 == result.exit_code
    assert 'Exported to:' in result.output
    assert 'zenodo.org/deposit' in result.output


@pytest.mark.integration
def test_dataset_export_upload_tag(
    runner, project, tmpdir, client, zenodo_sandbox
):
    """Test successful uploading of a file to Zenodo deposit."""
    result = runner.invoke(cli, ['dataset', 'create', 'my-dataset'])
    assert 0 == result.exit_code
    assert 'OK' in result.output

    # create data file
    new_file = tmpdir.join('datafile.csv')
    new_file.write('1,2,3')

    # add data to dataset
    result = runner.invoke(
        cli, ['dataset', 'add', 'my-dataset',
              str(new_file)]
    )
    assert 0 == result.exit_code

    with client.with_dataset(name='my-dataset') as dataset:
        dataset.description = 'awesome dataset'
        dataset.creator[0].affiliation = 'eth'

    data_repo = git.Repo(str(project))
    data_repo.git.add(update=True)
    data_repo.index.commit('metadata updated')

    # tag dataset
    result = runner.invoke(cli, ['dataset', 'tag', 'my-dataset', '1.0'])
    assert 0 == result.exit_code

    # create data file
    new_file = tmpdir.join('datafile2.csv')
    new_file.write('1,2,3,4')

    # add data to dataset
    result = runner.invoke(
        cli, ['dataset', 'add', 'my-dataset',
              str(new_file)]
    )
    assert 0 == result.exit_code

    # tag dataset
    result = runner.invoke(cli, ['dataset', 'tag', 'my-dataset', '2.0'])
    assert 0 == result.exit_code

    result = runner.invoke(
        cli, ['dataset', 'export', 'my-dataset', 'zenodo'], input='3'
    )

    assert 0 == result.exit_code
    assert 'Exported to:' in result.output
    assert 'zenodo.org/deposit' in result.output
    assert '2/2' in result.output

    result = runner.invoke(
        cli, ['dataset', 'export', 'my-dataset', 'zenodo'], input='2'
    )

    assert 0 == result.exit_code
    assert 'Exported to:' in result.output
    assert 'zenodo.org/deposit' in result.output
    assert '1/1' in result.output

    result = runner.invoke(
        cli, ['dataset', 'export', 'my-dataset', 'zenodo'], input='1'
    )

    assert 0 == result.exit_code
    assert 'Exported to:' in result.output
    assert 'zenodo.org/deposit' in result.output
    assert '2/2' in result.output


@pytest.mark.integration
def test_dataset_export_upload_multiple(
    runner, project, tmpdir, client, zenodo_sandbox
):
    """Test successful uploading of a files to Zenodo deposit."""
    result = runner.invoke(cli, ['dataset', 'create', 'my-dataset'])

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
        cli,
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

    result = runner.invoke(cli, ['dataset', 'export', 'my-dataset', 'zenodo'])

    assert 0 == result.exit_code
    assert 'Exported to:' in result.output
    assert 'zenodo.org/deposit' in result.output


@pytest.mark.integration
def test_dataset_export_upload_failure(runner, project, tmpdir, client):
    """Test failed uploading of a file to Zenodo deposit."""
    result = runner.invoke(cli, ['dataset', 'create', 'my-dataset'])

    assert 0 == result.exit_code
    assert 'OK' in result.output

    # create data file
    new_file = tmpdir.join('datafile.csv')
    new_file.write('1,2,3')

    # add data to dataset
    result = runner.invoke(
        cli, ['dataset', 'add', 'my-dataset',
              str(new_file)]
    )
    assert 0 == result.exit_code

    result = runner.invoke(cli, ['dataset', 'export', 'my-dataset', 'zenodo'])

    assert 2 == result.exit_code
    assert 'metadata.creators.0.affiliation' in result.output
    assert 'metadata.description' in result.output


@pytest.mark.integration
def test_dataset_export_published_url(
    runner, project, tmpdir, client, zenodo_sandbox
):
    """Test publishing of dataset."""
    result = runner.invoke(cli, ['dataset', 'create', 'my-dataset'])

    assert 0 == result.exit_code
    assert 'OK' in result.output

    # create data file
    new_file = tmpdir.join('datafile.csv')
    new_file.write('1,2,3')

    # add data to dataset
    result = runner.invoke(
        cli, ['dataset', 'add', 'my-dataset',
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
        cli, ['dataset', 'export', 'my-dataset', 'zenodo', '--publish']
    )

    assert 0 == result.exit_code
    assert 'Exported to:' in result.output
    assert 'zenodo.org/record' in result.output


@pytest.mark.integration
def test_export_dataset_wrong_provider(
    runner, project, tmpdir, client, zenodo_sandbox
):
    """Test non-existing provider."""
    result = runner.invoke(cli, ['dataset', 'create', 'my-dataset'])

    assert 0 == result.exit_code
    assert 'OK' in result.output

    # create data file
    new_file = tmpdir.join('datafile.csv')
    new_file.write('1,2,3')

    # add data to dataset
    result = runner.invoke(
        cli, ['dataset', 'add', 'my-dataset',
              str(new_file)]
    )
    assert 0 == result.exit_code

    result = runner.invoke(
        cli, ['dataset', 'export', 'my-dataset', 'notzenodo']
    )
    assert 2 == result.exit_code
    assert 'Unknown provider.' in result.output


@pytest.mark.integration
def test_dataset_export(runner, client, project):
    """Check dataset not found exception raised."""
    result = runner.invoke(
        cli, ['dataset', 'export', 'doesnotexists', 'somewhere']
    )

    assert 2 == result.exit_code
    assert 'Dataset is not found.' in result.output


@pytest.mark.integration
def test_export_dataset_unauthorized(
    runner, project, client, tmpdir, zenodo_sandbox
):
    """Test unauthorized exception raised."""
    client.set_value('zenodo', 'access_token', 'not-a-token')
    result = runner.invoke(cli, ['dataset', 'create', 'my-dataset'])
    assert 0 == result.exit_code
    assert 'OK' in result.output

    # create data file
    new_file = tmpdir.join('datafile.csv')
    new_file.write('1,2,3')

    # add data to dataset
    result = runner.invoke(
        cli, ['dataset', 'add', 'my-dataset',
              str(new_file)]
    )
    assert 0 == result.exit_code

    result = runner.invoke(cli, ['dataset', 'export', 'my-dataset', 'zenodo'])

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
    result = runner.invoke(cli, ['dataset', 'create', 'dataset'])

    assert 0 == result.exit_code
    assert 'OK' in result.output

    with client.with_dataset('dataset') as dataset:
        assert dataset.name == 'dataset'

    # add data
    result = runner.invoke(cli, ['dataset', 'add', 'dataset', str(data_file)])

    assert 0 == result.exit_code
    assert os.stat(
        os.path.join('data', 'dataset', os.path.basename(str(data_file)))
    )

    # add data from a git repo via http
    result = runner.invoke(
        cli, [
            'dataset', 'add', 'dataset', '--source', remotes['filename'],
            remotes['url']
        ]
    )
    assert 0 == result.exit_code
    assert os.stat(remotes['expected_path'])

    # add data from local git repo
    result = runner.invoke(
        cli, [
            'dataset',
            'add',
            'dataset',
            '-s',
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
    result = runner.invoke(cli, ['dataset', 'create', 'dataset'])

    assert 0 == result.exit_code
    assert 'OK' in result.output

    with client.with_dataset('dataset') as dataset:
        assert dataset.name == 'dataset'

    # add data
    result = runner.invoke(
        cli,
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
        cli,
        [
            'dataset', 'add', 'dataset', '--source', remotes['filename'],
            remotes['url']
        ],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code
    assert os.stat(remotes['expected_path'])

    # add data from local git repo
    result = runner.invoke(
        cli,
        [
            'dataset',
            'add',
            'dataset',
            '-s',
            'dir2/file2',
            os.path.dirname(data_repository.git_dir),
        ],
    )
    assert 0 == result.exit_code
