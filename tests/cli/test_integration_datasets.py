# -*- coding: utf-8 -*-
#
# Copyright 2017-2020 - Swiss Data Science Center (SDSC)
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
import shutil
import subprocess
from collections import namedtuple
from pathlib import Path
from urllib import parse

import git
import pytest
from flaky import flaky

from renku.cli import cli
from renku.core import errors
from renku.core.commands.clone import project_clone
from renku.core.utils.contexts import chdir


@pytest.mark.parametrize(
    'doi', [{
        'doi': '10.5281/zenodo.2658634',
        'short_name': 'pyndl_naive_discriminat_v064',
        'creator':
            'Konstantin Sering, Marc Weitz, David-Elias Künstle, '
            'Lennart Schneider',
        'version': 'v0.6.4'
    }, {
        'doi': '10.7910/DVN/F4NUMR',
        'short_name': 'replication_data_for_ca_2',
        'creator': 'James Druckman, Martin Kifer, Michael Parkin',
        'version': '2'
    }]
)
@pytest.mark.parametrize(
    'prefix', [
        '', 'doi:', 'doi.org/', 'www.doi.org/', 'dx.doi.org/',
        'http://www.doi.org/', 'https://dx.doi.org/', 'https://doi.org/'
    ]
)
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_dataset_import_real_doi(runner, client, doi, prefix, sleep_after):
    """Test dataset import for existing DOI."""
    uri = prefix + doi['doi']
    result = runner.invoke(cli, ['dataset', 'import', uri], input='y')
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'OK' in result.output + str(result.stderr_bytes)

    result = runner.invoke(cli, ['dataset', '-c', 'short_name,creators'])

    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert doi['short_name'] in result.output
    assert doi['creator'] in result.output

    result = runner.invoke(cli, ['dataset', 'ls-tags', doi['short_name']])
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert doi['version'] in result.output

    with client.with_dataset(doi['short_name']) as dataset:
        assert doi['doi'] in dataset.same_as.url
        assert dataset.identifier in dataset.url
        assert dataset.url == dataset._id


@pytest.mark.parametrize(
    'doi', [
        ('10.5281/zenodo.3239980', 'n'),
        ('10.5281/zenodo.3188334', 'y'),
        ('10.7910/DVN/TJCLKP', 'n'),
        ('10.7910/DVN/F4NUMR', 'y'),
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
        ('10.5281/zenodo.1175627', 'y'),
        ('10.5281/zenodo.3490468', 'y'),
    ]
)
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_dataset_import_real_param(doi, runner, project, sleep_after, client):
    """Test dataset import and check metadata parsing."""
    result = runner.invoke(
        cli, ['dataset', 'import', '--short-name', 'remote', doi[0]],
        input=doi[1]
    )

    if 'y' == doi[1]:
        assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
        assert 'OK' in result.output
        with client.with_dataset('remote') as dataset:
            assert dataset.url == dataset._id
    else:
        assert 1 == result.exit_code

    result = runner.invoke(cli, ['dataset'])
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)


@pytest.mark.parametrize(
    'doi', [
        ('10.5281/zenodo.3239984', 'n'),
        ('zenodo.org/record/3239986', 'n'),
        ('10.5281/zenodo.3239982', 'n'),
    ]
)
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_dataset_import_uri_404(doi, runner, project, sleep_after):
    """Test dataset import and check that correct exception is raised."""
    result = runner.invoke(cli, ['dataset', 'import', doi[0]], input=doi[1])
    assert 2 == result.exit_code, result.output + str(result.stderr_bytes)

    result = runner.invoke(cli, ['dataset'])
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_dataset_import_real_doi_warnings(runner, project, sleep_after):
    """Test dataset import for existing DOI and dataset"""
    result = runner.invoke(
        cli, ['dataset', 'import', '10.5281/zenodo.1438326'], input='y'
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'Warning: Newer version found' in result.output
    assert 'OK' in result.output

    result = runner.invoke(
        cli, ['dataset', 'import', '10.5281/zenodo.1438326'], input='y'
    )
    assert 1 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'Warning: Newer version found' in result.output
    assert 'Error: Dataset exists:' in result.output

    result = runner.invoke(
        cli, ['dataset', 'import', '10.5281/zenodo.597964'], input='y'
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'Warning: Newer version found' not in result.output
    assert 'Error: Dataset exists:' not in result.output
    assert 'OK' in result.output

    result = runner.invoke(cli, ['dataset'])
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'pyndl_naive_discriminat' in result.output


@pytest.mark.parametrize(
    'doi,err', [('10.5281/zenodo.5979642342', 'record not found'),
                ('10.7910/DVN/S8MSVFXXXX', 'provider DVN not found'),
                ('10.5281/zenodo.1494915', 'no files have been found'),
                ('https://zenodo.org/record/2621201248', 'record not found'),
                ((
                    'https://dataverse.harvard.edu/dataset.xhtml'
                    '?persistentId=doi:10.7910/DVN/F4NUMRXXXX'
                ), 'record not found')]
)
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_dataset_import_expected_err(runner, project, doi, err):
    """Test error raising for invalid DOI."""
    result = runner.invoke(cli, ['dataset', 'import', doi], input='y')
    assert 2 == result.exit_code, result.output + str(result.stderr_bytes)
    assert err in result.output


@pytest.mark.parametrize(
    'url', [
        'https://zenodo.org/record/2621208',
        (
            'https://dataverse.harvard.edu/dataset.xhtml'
            '?persistentId=doi:10.7910/DVN/F4NUMR'
        )
    ]
)
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_dataset_import_real_http(runner, project, url, sleep_after):
    """Test dataset import through HTTPS."""
    result = runner.invoke(cli, ['dataset', 'import', '-y', url], input='n')

    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'Do you wish to download this version?' not in result.output
    assert 'OK' in result.output


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_dataset_import_and_extract(runner, project, client, sleep_after):
    """Test dataset import and extract files."""
    url = 'https://zenodo.org/record/2658634'
    result = runner.invoke(
        cli, ['dataset', 'import', '--extract', '--short-name', 'remote', url],
        input='y'
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    with client.with_dataset('remote') as dataset:
        extracted_file = 'data/remote/quantling-pyndl-c34259c/doc/make.bat'
        assert dataset.find_file(extracted_file)


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_dataset_import_different_names(runner, client, sleep_after):
    """Test can import same DOI under different names."""
    doi = '10.5281/zenodo.2658634'
    result = runner.invoke(
        cli, ['dataset', 'import', '--short-name', 'name-1', doi], input='y'
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'OK' in result.output + str(result.stderr_bytes)

    result = runner.invoke(
        cli, ['dataset', 'import', '--short-name', 'name-2', doi], input='y'
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'OK' in result.output


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_dataset_import_ignore_uncompressed_files(
    runner, project, sleep_after
):
    """Test dataset import ignores uncompressed files."""
    url = 'https://zenodo.org/record/3251128'
    result = runner.invoke(
        cli, ['dataset', 'import', '--extract', url], input='y'
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'Gorne_Diaz_database_2019.csv' in result.output


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_dataset_reimport_removed_dataset(runner, project, sleep_after):
    """Test re-importing of deleted datasets works."""
    doi = '10.5281/zenodo.2658634'
    result = runner.invoke(
        cli, ['dataset', 'import', doi, '--short-name', 'my-dataset'],
        input='y'
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    result = runner.invoke(cli, ['dataset', 'rm', 'my-dataset'])
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    result = runner.invoke(
        cli, ['dataset', 'import', doi, '--short-name', 'my-dataset'],
        input='y'
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)


@pytest.mark.integration
@flaky(max_runs=30, min_passes=1)
def test_dataset_import_preserve_names(runner, project, sleep_after):
    """Test import keeps original file names."""
    doi = '10.7910/DVN/F4NUMR'
    result = runner.invoke(cli, ['dataset', 'import', doi], input='y')
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    result = runner.invoke(cli, ['dataset', 'ls-files'])
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'Data Key 2002-2006' in result.output


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
@pytest.mark.parametrize(
    'url', [
        'https://dev.renku.ch/datasets/48299db3-8870-4cbe-a480-f75985c42a62/',
        'https://dev.renku.ch/projects/virginiafriedrich/datasets-test/'
        'datasets/48299db3-8870-4cbe-a480-f75985c42a62'
    ]
)
def test_dataset_import_renku(runner, project, client, url):
    """Test dataset import from Renku projects."""
    result = runner.invoke(cli, ['dataset', 'import', url], input='y')
    assert 0 == result.exit_code
    checksum = '54751585bb81a0bff32727e46e2aabaa0c8d19f8'
    assert checksum in result.output
    size = '66.00'
    assert size in result.output
    assert 'OK' in result.output

    result = runner.invoke(cli, ['dataset', 'ls-files'])
    assert 0 == result.exit_code
    path = 'zhbikes/2019_verkehrszaehlungen_werte_fussgaenger_velo.csv'
    assert path in result.output

    dataset = [d for d in client.datasets.values()][0]
    assert dataset.same_as.url['@id'] == url


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_dataset_import_renku_fail(runner, client, monkeypatch):
    """Test dataset import fails if cannot clone repo."""
    from renku.core.management import LocalClient
    url = 'https://dev.renku.ch/datasets/48299db3-8870-4cbe-a480-f75985c42a62'

    def prepare_git_repo(*_):
        raise errors.GitError

    with monkeypatch.context() as monkey:
        monkey.setattr(LocalClient, 'prepare_git_repo', prepare_git_repo)

        result = runner.invoke(cli, ['dataset', 'import', url], input='y')
        assert 2 == result.exit_code
        assert f'Invalid parameter value for {url}' in result.output
        assert 'Cannot clone remote projects:' in result.output


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
@pytest.mark.parametrize(
    'url,exit_code',
    [('https://dev.renku.ch/projects/virginiafriedrich/datasets-test/', 2),
     (
         'https://dev.renku.ch/projects/virginiafriedrich/datasets-test/'
         'datasets/b9f7b21b-8b00-42a2-976a-invalid', 2
     ), ('https://dev.renku.ch/datasets/10.5281%2Fzenodo.666', 1)]
)
def test_dataset_import_renku_errors(runner, project, url, exit_code):
    """Test usage errors in Renku dataset import."""
    result = runner.invoke(cli, ['dataset', 'import', url], input='y')
    assert exit_code == result.exit_code

    result = runner.invoke(cli, ['dataset'])
    assert 0 == result.exit_code


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_dataset_reimport_renku_dataset(runner, project):
    """Test dataset import for existing dataset"""
    URL = 'https://dev.renku.ch/projects/virginiafriedrich/datasets-test/' \
        'datasets/48299db3-8870-4cbe-a480-f75985c42a62'

    result = runner.invoke(cli, ['dataset', 'import', URL], input='y')
    assert 'OK' in result.output
    assert 0 == result.exit_code

    result = runner.invoke(cli, ['dataset', 'import', URL], input='y')
    assert 1 == result.exit_code
    assert 'Dataset exists: "zhbikes"' in result.output


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
@pytest.mark.parametrize(
    'provider,params,output',
    [('zenodo', [], 'zenodo.org/deposit'),
     ('dataverse', ['--dataverse-name', 'sdsc-test-dataverse'], 'doi:')]
)
def test_dataset_export_upload_file(
    runner, project, tmpdir, client, zenodo_sandbox, dataverse_demo, provider,
    params, output
):
    """Test successful uploading of a file to Zenodo/Dataverse deposit."""
    result = runner.invoke(cli, ['dataset', 'create', 'my-dataset'])

    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'OK' in result.output

    # create data file
    new_file = tmpdir.join('datafile.csv')
    new_file.write('1,2,3')

    # add data to dataset
    result = runner.invoke(
        cli, ['dataset', 'add', 'my-dataset',
              str(new_file)]
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    with client.with_dataset('my-dataset') as dataset:
        dataset.description = 'awesome dataset'
        dataset.creator[0].affiliation = 'eth'

    data_repo = git.Repo(str(project))
    data_repo.git.add(update=True)
    data_repo.index.commit('metadata updated')

    result = runner.invoke(
        cli, ['dataset', 'export', 'my-dataset', provider] + params
    )

    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'Exported to:' in result.output
    assert output in result.output


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
@pytest.mark.parametrize(
    'provider,params,output',
    [('zenodo', [], 'zenodo.org/deposit'),
     ('dataverse', ['--dataverse-name', 'sdsc-test-dataverse'], 'doi:')]
)
def test_dataset_export_upload_tag(
    runner, project, tmpdir, client, zenodo_sandbox, dataverse_demo, provider,
    params, output
):
    """Test successful uploading of a file to Zenodo/Dataverse deposit."""
    result = runner.invoke(cli, ['dataset', 'create', 'my-dataset'])
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'OK' in result.output

    # create data file
    new_file = tmpdir.join('datafile.csv')
    new_file.write('1,2,3')

    # add data to dataset
    result = runner.invoke(
        cli, ['dataset', 'add', 'my-dataset',
              str(new_file)]
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    with client.with_dataset('my-dataset') as dataset:
        dataset.description = 'awesome dataset'
        dataset.creator[0].affiliation = 'eth'

    data_repo = git.Repo(str(project))
    data_repo.git.add(update=True)
    data_repo.index.commit('metadata updated')

    # tag dataset
    result = runner.invoke(cli, ['dataset', 'tag', 'my-dataset', '1.0'])
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    # create data file
    new_file = tmpdir.join('datafile2.csv')
    new_file.write('1,2,3,4')

    # add data to dataset
    result = runner.invoke(
        cli, ['dataset', 'add', 'my-dataset',
              str(new_file)]
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    # tag dataset
    result = runner.invoke(cli, ['dataset', 'tag', 'my-dataset', '2.0'])
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    result = runner.invoke(
        cli, ['dataset', 'export', 'my-dataset', provider] + params, input='3'
    )

    assert 0 == result.exit_code
    assert 'Exported to:' in result.output
    assert output in result.output
    assert '2/2' in result.output

    result = runner.invoke(
        cli, ['dataset', 'export', 'my-dataset', provider] + params, input='2'
    )

    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'Exported to:' in result.output
    assert output in result.output
    assert '1/1' in result.output

    result = runner.invoke(
        cli, ['dataset', 'export', 'my-dataset', provider] + params, input='1'
    )

    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'Exported to:' in result.output
    assert output in result.output
    assert '2/2' in result.output


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
@pytest.mark.parametrize(
    'provider,params,output',
    [('zenodo', [], 'zenodo.org/deposit'),
     ('dataverse', ['--dataverse-name', 'sdsc-test-dataverse'], 'doi:')]
)
def test_dataset_export_upload_multiple(
    runner, project, tmpdir, client, zenodo_sandbox, dataverse_demo, provider,
    params, output
):
    """Test successful uploading of a files to Zenodo deposit."""
    result = runner.invoke(cli, ['dataset', 'create', 'my-dataset'])

    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
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
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    with client.with_dataset('my-dataset') as dataset:
        dataset.description = 'awesome dataset'
        dataset.creator[0].affiliation = 'eth'

    data_repo = git.Repo(str(project))
    data_repo.git.add(update=True)
    data_repo.index.commit('metadata updated')

    result = runner.invoke(
        cli, ['dataset', 'export', 'my-dataset', provider] + params
    )

    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'Exported to:' in result.output
    assert output in result.output


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_dataset_export_upload_failure(runner, tmpdir, client, zenodo_sandbox):
    """Test failed uploading of a file to Zenodo deposit."""
    result = runner.invoke(cli, ['dataset', 'create', 'my-dataset'])

    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'OK' in result.output

    # create data file
    new_file = tmpdir.join('datafile.csv')
    new_file.write('1,2,3')

    # add data to dataset
    result = runner.invoke(
        cli, ['dataset', 'add', 'my-dataset',
              str(new_file)]
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    result = runner.invoke(cli, ['dataset', 'export', 'my-dataset', 'zenodo'])

    assert 1 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'metadata.creators.0.affiliation' in result.output
    assert 'metadata.description' in result.output


@pytest.mark.publish
@flaky(max_runs=10, min_passes=1)
@pytest.mark.parametrize(
    'provider,params,output',
    [('zenodo', [], 'zenodo.org/record'),
     (
         'dataverse', ['--dataverse-name', 'sdsc-published-test-dataverse'
                       ], 'doi:'
     )]
)
def test_dataset_export_published_url(
    runner, project, tmpdir, client, zenodo_sandbox, dataverse_demo, provider,
    params, output
):
    """Test publishing of dataset."""
    result = runner.invoke(cli, ['dataset', 'create', 'my-dataset'])

    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'OK' in result.output

    # create data file
    new_file = tmpdir.join('datafile.csv')
    new_file.write('1,2,3')

    # add data to dataset
    result = runner.invoke(
        cli, ['dataset', 'add', 'my-dataset',
              str(new_file)]
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    with client.with_dataset('my-dataset') as dataset:
        dataset.description = 'awesome dataset'
        dataset.creator[0].affiliation = 'eth'

    data_repo = git.Repo(str(project))
    data_repo.git.add(update=True)
    data_repo.index.commit('metadata updated')

    result = runner.invoke(
        cli,
        ['dataset', 'export', 'my-dataset', provider, '--publish'] + params
    )

    assert 0 == result.exit_code
    assert 'Exported to:' in result.output
    assert output in result.output


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_export_dataset_wrong_provider(runner, project, tmpdir, client):
    """Test non-existing provider."""
    result = runner.invoke(cli, ['dataset', 'create', 'my-dataset'])

    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'OK' in result.output

    # create data file
    new_file = tmpdir.join('datafile.csv')
    new_file.write('1,2,3')

    # add data to dataset
    result = runner.invoke(
        cli, ['dataset', 'add', 'my-dataset',
              str(new_file)]
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    result = runner.invoke(
        cli, ['dataset', 'export', 'my-dataset', 'unsupported-provider']
    )
    assert 2 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'Unknown provider.' in result.output


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_dataset_export(runner, client, project):
    """Check dataset not found exception raised."""
    result = runner.invoke(
        cli, ['dataset', 'export', 'doesnotexists', 'somewhere']
    )

    assert 2 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'Dataset "doesnotexists" is not found.' in result.output


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
@pytest.mark.parametrize(
    'provider,params',
    [('zenodo', []),
     ('dataverse', ['--dataverse-name', 'sdsc-test-dataverse'])]
)
def test_export_dataset_unauthorized(
    runner, project, client, tmpdir, zenodo_sandbox, dataverse_demo, provider,
    params
):
    """Test unauthorized exception raised."""
    client.set_value(provider, 'access_token', 'not-a-token')
    client.repo.git.add('.renku/renku.ini')
    client.repo.index.commit('update renku.ini')

    result = runner.invoke(cli, ['dataset', 'create', 'my-dataset'])
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'OK' in result.output

    # create data file
    new_file = tmpdir.join('datafile.csv')
    new_file.write('1,2,3')

    # add data to dataset
    result = runner.invoke(
        cli, ['dataset', 'add', 'my-dataset',
              str(new_file)]
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    result = runner.invoke(
        cli, ['dataset', 'export', 'my-dataset', provider] + params
    )

    assert 1 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'Access unauthorized - update access token.' in result.output

    secret = client.get_value('zenodo', 'secret')
    assert secret is None


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_export_dataverse_no_dataverse_name(
    runner, project, client, dataverse_demo
):
    """Test export without providing a dataverse name."""
    result = runner.invoke(cli, ['dataset', 'create', 'my-dataset'])
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    result = runner.invoke(
        cli, ['dataset', 'export', 'my-dataset', 'dataverse']
    )

    assert 2 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'Dataverse name is required.' in result.output


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_export_dataverse_no_dataverse_url(
    runner, client, dataverse_demo, global_config_dir
):
    """Test export without providing a dataverse server url."""
    client.remove_value('dataverse', 'server_url')
    client.repo.git.add('.renku/renku.ini')
    client.repo.index.commit('renku.ini')

    result = runner.invoke(cli, ['dataset', 'create', 'my-dataset'])
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    result = runner.invoke(
        cli, [
            'dataset', 'export', 'my-dataset', 'dataverse', '--dataverse-name',
            'sdsc-test-dataverse'
        ]
    )

    assert 2 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'Dataverse server URL is required.' in result.output


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_export_imported_dataset_to_dataverse(
    runner, client, dataverse_demo, zenodo_sandbox
):
    """Test exporting an imported Zenodo dataset to dataverse."""
    result = runner.invoke(
        cli, [
            'dataset', 'import', '10.5281/zenodo.2658634', '--short-name',
            'my-data'
        ],
        input='y'
    )
    assert 0 == result.exit_code

    result = runner.invoke(
        cli, [
            'dataset', 'export', 'my-data', 'dataverse', '--dataverse-name',
            'sdsc-test-dataverse'
        ],
        input='2'
    )

    assert 0 == result.exit_code
    assert 'OK' in result.output


@pytest.mark.integration
@pytest.mark.parametrize(
    'params,path',
    [
        # add data with no destination
        (['-s', 'docker'], 'data/remote/docker/r/Dockerfile'),
        (['-s', 'docker/r/Dockerfile'], 'data/remote/Dockerfile'),
        # add data to a non-existing destination
        (['-s', 'docker', '-d', 'new'], 'data/remote/new/docker/r/Dockerfile'),
        (['-s', 'docker/r', '-d', 'new'], 'data/remote/new/r/Dockerfile'),
        (['-s', 'docker/r/Dockerfile', '-d', 'new'
          ], 'data/remote/new/Dockerfile'),
        # add data to an existing destination
        (['-s', 'docker', '-d', 'existing'
          ], 'data/remote/existing/docker/r/Dockerfile'),
        (['-s', 'docker/r', '-d', 'existing'
          ], 'data/remote/existing/r/Dockerfile'),
        (['-s', 'docker/r/Dockerfile', '-d', 'existing'
          ], 'data/remote/existing/Dockerfile'),
    ]
)
@flaky(max_runs=10, min_passes=1)
def test_add_data_from_git(runner, client, params, path):
    """Test add data to datasets from a git repository."""
    remote = 'https://github.com/SwissDataScienceCenter/renku-jupyter.git'

    # create a dataset and add a file to it
    result = runner.invoke(
        cli,
        [
            'dataset', 'add', 'remote', '--create', '--ref', '0.3.0', '-s',
            'LICENSE', '-d', 'existing/LICENSE', remote
        ],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    result = runner.invoke(
        cli,
        ['dataset', 'add', 'remote', '--ref', '0.3.0', remote] + params,
        catch_exceptions=False,
    )

    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert Path(path).exists()


@pytest.mark.integration
@pytest.mark.parametrize(
    'params,files', [
        (['-s', 'docker*'], {'docker'}),
        (['-s', 'docker/*'
          ], {'py3.7', 'cuda10.0-tf1.14', 'cuda9.2', 'r3.6.1', 'bioc3_10'}),
        (['-s', 'docker/**'], {
            'powerline.config', 'Dockerfile', 'entrypoint.sh',
            'requirements.txt', 'fix-permissions.sh',
            'LICENSE-fix-permissions', 'powerline.bashrc', 'git-config.bashrc'
        }),
        (['-s', 'docker/*/*sh'], {'entrypoint.sh', 'fix-permissions.sh'}),
    ]
)
@flaky(max_runs=10, min_passes=1)
def test_add_data_from_git_with_wildcards(runner, client, params, files):
    """Test add data using wildcards to datasets from a git repository."""
    REMOTE = 'https://github.com/SwissDataScienceCenter/renku-jupyter.git'

    result = runner.invoke(
        cli,
        ['dataset', 'add', 'remote', '--create', '--ref', '0.5.2', REMOTE] +
        params,
        catch_exceptions=False,
    )
    assert 0 == result.exit_code
    assert files == set(os.listdir('data/remote'))

    result = runner.invoke(
        cli,
        ['dataset', 'add', 'remote', '--ref', '0.5.2', '-d', 'new', REMOTE] +
        params,
        catch_exceptions=False,
    )
    assert 0 == result.exit_code
    assert files == set(os.listdir('data/remote/new'))


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_add_from_git_copies_metadata(runner, client):
    """Test an import from a git repository keeps creators name."""
    # create a dataset and add a file to it
    result = runner.invoke(
        cli,
        [
            'dataset', 'add', 'remote', '--create', '--ref', 'v0.3.0', '-s',
            'README.rst',
            'https://github.com/SwissDataScienceCenter/renku-python.git'
        ],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    dataset = client.load_dataset('remote')
    assert dataset.files[0].name == 'README.rst'
    assert 'mailto:jiri.kuncar@gmail.com' in str(dataset.files[0].creator)
    assert 'mailto:rokroskar@gmail.co' in str(dataset.files[0].creator)


@pytest.mark.integration
@pytest.mark.parametrize(
    'params,n_urls,message', [
        ([], 0, 'No URL is specified'),
        (['-s', 'file', '-d', 'new-file'], 0, 'No URL is specified'),
        (['-s', 'file'], 2, 'Cannot use "--source" with multiple URLs.'),
        (['-s', 'non-existing'], 1, 'No such file or directory'),
        (['-s', 'docker/*Dockerfile'], 1, 'No such file or directory'),
        (['-s', 'docker', '-d', 'LICENSE'
          ], 1, 'Destination is not a directory'),
        (['-s', 'LICENSE', '-s', 'Makefile', '-d', 'LICENSE'
          ], 1, 'Destination is not a directory'),
        (['-d', 'LICENSE'], 1, 'Destination is not a directory'),
    ]
)
@flaky(max_runs=10, min_passes=1)
def test_usage_error_in_add_from_git(runner, client, params, n_urls, message):
    """Test user's errors when adding to a dataset from a git repository."""
    remote = 'https://github.com/SwissDataScienceCenter/renku-jupyter.git'

    # create a dataset and add a file to it
    result = runner.invoke(
        cli,
        [
            'dataset', 'add', 'remote', '--create', '--ref', '0.3.0', '-s',
            'LICENSE', remote
        ],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    urls = n_urls * [remote]

    result = runner.invoke(
        cli,
        ['dataset', 'add', 'remote', '--ref', '0.3.0'] + params + urls,
        catch_exceptions=False,
    )
    assert 2 == result.exit_code, result.output + str(result.stderr_bytes)
    assert message in result.output


def read_dataset_file_metadata(client, short_name, filename):
    """Return metadata from dataset's YAML file."""
    with client.with_dataset(short_name) as dataset:
        assert client.get_dataset_path(dataset.name).exists()

        for file_ in dataset.files:
            if file_.path.endswith(filename):
                return file_


@pytest.mark.integration
@pytest.mark.parametrize(
    'params', [[], ['-I', 'CHANGES.rst'], ['-I', 'C*'], ['remote']]
)
@flaky(max_runs=10, min_passes=1)
def test_dataset_update(client, runner, params):
    """Test local copy is updated when remote file is updates."""
    # Add dataset to project
    result = runner.invoke(
        cli, [
            'dataset', 'add', '--create', 'remote', '--ref', 'v0.3.0', '-s',
            'CHANGES.rst',
            'https://github.com/SwissDataScienceCenter/renku-python.git'
        ],
        catch_exceptions=False
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    before = read_dataset_file_metadata(client, 'remote', 'CHANGES.rst')

    result = runner.invoke(
        cli, ['dataset', 'update'] + params, catch_exceptions=False
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    after = read_dataset_file_metadata(client, 'remote', 'CHANGES.rst')
    assert after._id == before._id
    assert after._label != before._label
    assert after.added == before.added
    assert after.url == before.url
    assert after.based_on._id == before.based_on._id
    assert after.based_on._label != before.based_on._label
    assert after.based_on.path == before.based_on.path
    assert after.based_on.based_on is None


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_dataset_update_remove_file(client, runner):
    """Test local copy is removed when remote file is removed."""
    # Add dataset to project
    result = runner.invoke(
        cli, [
            'dataset', 'add', '--create', 'remote', '-s', 'docs/authors.rst',
            '--ref', 'v0.3.0',
            'https://github.com/SwissDataScienceCenter/renku-python.git'
        ],
        catch_exceptions=False
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    file_path = client.path / 'data' / 'remote' / 'authors.rst'
    assert file_path.exists()

    # docs/authors.rst does not exists in v0.5.0

    result = runner.invoke(
        cli, ['dataset', 'update', '--ref', 'v0.5.0'], catch_exceptions=False
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'Some files are deleted from remote.' in result.output
    assert file_path.exists()

    result = runner.invoke(
        cli, ['dataset', 'update', '--ref', 'v0.5.0', '--delete'],
        catch_exceptions=False
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    assert not file_path.exists()


@pytest.mark.integration
@pytest.mark.parametrize(
    'params', [['-I', 'non-existing'], ['non-existing-dataset']]
)
@flaky(max_runs=10, min_passes=1)
def test_dataset_invalid_update(client, runner, params):
    """Test updating a non-existing path."""
    # Add dataset to project
    result = runner.invoke(
        cli, [
            'dataset', 'add', '--create', 'remote', '-s', 'docs/authors.rst',
            '--ref', 'v0.3.0',
            'https://github.com/SwissDataScienceCenter/renku-python.git'
        ],
        catch_exceptions=False
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    result = runner.invoke(
        cli, ['dataset', 'update'] + params, catch_exceptions=False
    )
    assert 2 == result.exit_code, result.output + str(result.stderr_bytes)


@pytest.mark.integration
@pytest.mark.parametrize(
    'params',
    [[], ['-I', 'CHANGES.rst'], ['-I', 'CH*'], ['dataset-1', 'dataset-2']]
)
@flaky(max_runs=10, min_passes=1)
def test_dataset_update_multiple_datasets(
    client, runner, data_repository, directory_tree, params
):
    """Test update with multiple datasets."""
    path1 = client.path / 'data' / 'dataset-1' / 'CHANGES.rst'
    path2 = client.path / 'data' / 'dataset-2' / 'CHANGES.rst'
    # Add dataset to project
    result = runner.invoke(
        cli, [
            'dataset', 'add', '--create', 'dataset-1', '--ref', 'v0.3.0', '-s',
            'CHANGES.rst',
            'https://github.com/SwissDataScienceCenter/renku-python.git'
        ],
        catch_exceptions=False
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    result = runner.invoke(
        cli, [
            'dataset', 'add', '--create', 'dataset-2', '--ref', 'v0.3.0', '-s',
            'CHANGES.rst',
            'https://github.com/SwissDataScienceCenter/renku-python.git'
        ],
        catch_exceptions=False
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    assert 'v0.4.0' not in path1.read_text()
    assert 'v0.4.0' not in path2.read_text()

    result = runner.invoke(
        cli, ['dataset', 'update'] + params, catch_exceptions=False
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    assert 'v0.4.0' in path1.read_text()
    assert 'v0.4.0' in path2.read_text()


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_empty_update(client, runner, data_repository, directory_tree):
    """Test update when nothing changed does not create a commit."""
    # Add dataset to project
    result = runner.invoke(
        cli, [
            'dataset', 'add', '--create', 'remote', '--ref', 'v0.3.0', '-s',
            'CHANGES.rst',
            'https://github.com/SwissDataScienceCenter/renku-python.git'
        ],
        catch_exceptions=False
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    commit_sha_before = client.repo.head.object.hexsha
    result = runner.invoke(
        cli, ['dataset', 'update', '--ref', 'v0.3.0'], catch_exceptions=False
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    commit_sha_after = client.repo.head.object.hexsha
    assert commit_sha_after == commit_sha_before


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_import_from_renku_project(tmpdir, client, runner):
    """Test an imported dataset from other renku repos will have metadata."""
    from renku.core.management import LocalClient

    remote = 'https://dev.renku.ch/gitlab/virginiafriedrich/datasets-test.git'

    path = tmpdir.strpath
    os.environ['GIT_LFS_SKIP_SMUDGE'] = '1'
    git.Repo.clone_from(remote, path, recursive=True)

    remote_client = LocalClient(path)
    with chdir(remote_client.path):
        runner.invoke(cli, ['migrate'])
    file_ = read_dataset_file_metadata(
        remote_client, 'zhbikes',
        '2019_verkehrszaehlungen_werte_fussgaenger_velo.csv'
    )

    result = runner.invoke(
        cli,
        [
            'dataset', 'add', '--create', 'remote-dataset', '-s',
            'data/zhbikes/2019_verkehrszaehlungen_werte_fussgaenger_velo.csv',
            '-d', 'new-directory', '--ref', 'b973db5', remote
        ],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    path = 'new-directory/2019_verkehrszaehlungen_werte_fussgaenger_velo.csv'
    metadata = read_dataset_file_metadata(client, 'remote-dataset', path)
    assert metadata.creator[0].name == file_.creator[0].name
    assert metadata.based_on._id == file_._id
    assert metadata.based_on._label == file_._label
    assert metadata.based_on.path == file_.path
    assert metadata.based_on.based_on is None
    assert metadata.based_on.url == remote


@pytest.mark.integration
@pytest.mark.parametrize(
    'ref', ['v0.3.0', 'fe6ec65cc84bcf01e879ef38c0793208f7fab4bb']
)
@flaky(max_runs=10, min_passes=1)
def test_add_specific_refs(ref, runner, client):
    """Test adding a specific version of files."""
    FILENAME = 'CHANGES.rst'
    # create a dataset
    result = runner.invoke(cli, ['dataset', 'create', 'dataset'])
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    # add data from a git repo
    result = runner.invoke(
        cli, [
            'dataset', 'add', 'dataset', '-s', FILENAME, '--ref', ref,
            'https://github.com/SwissDataScienceCenter/renku-python.git'
        ]
    )
    assert 0 == result.exit_code
    content = (client.path / 'data' / 'dataset' / FILENAME).read_text()
    assert 'v0.3.0' in content
    assert 'v0.3.1' not in content


@pytest.mark.integration
@pytest.mark.parametrize(
    'ref', ['v0.3.1', '27e29abd409c83129a3fdb8b8b0b898b23bcb229']
)
@flaky(max_runs=10, min_passes=1)
def test_update_specific_refs(ref, runner, client):
    """Test updating to a specific version of files."""
    filename = 'CHANGES.rst'
    # create a dataset
    result = runner.invoke(cli, ['dataset', 'create', 'dataset'])
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    # add data from a git repo
    result = runner.invoke(
        cli, [
            'dataset', 'add', 'dataset', '-s', filename, '--ref', 'v0.3.0',
            'https://github.com/SwissDataScienceCenter/renku-python.git'
        ]
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    content = (client.path / 'data' / 'dataset' / filename).read_text()
    assert 'v0.3.1' not in content

    # update data to a later version
    result = runner.invoke(cli, ['dataset', 'update', '--ref', ref])
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    content = (client.path / 'data' / 'dataset' / filename).read_text()
    assert 'v0.3.1' in content
    assert 'v0.3.2' not in content


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_update_with_multiple_remotes_and_ref(runner, client):
    """Test updating fails when ref is ambiguous."""
    # create a dataset
    result = runner.invoke(cli, ['dataset', 'create', 'dataset'])
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    # add data from a git repo
    result = runner.invoke(
        cli, [
            'dataset', 'add', 'dataset', '-s', 'CHANGES.rst',
            'https://github.com/SwissDataScienceCenter/renku-python.git'
        ]
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    # add data from another git repo
    result = runner.invoke(
        cli, [
            'dataset', 'add', 'dataset', '-s', 'LICENSE',
            'https://github.com/SwissDataScienceCenter/renku-notebooks.git'
        ]
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    # update data to a later version
    result = runner.invoke(cli, ['dataset', 'update', '--ref', 'any-value'])
    assert 2 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'Cannot use "--ref" with more than one Git repo' in result.output


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_files_are_tracked_in_lfs(runner, client, no_lfs_size_limit):
    """Test files added from a Git repo are tacked in Git LFS."""
    filename = 'requirements-devel.txt'
    # create a dataset
    result = runner.invoke(cli, ['dataset', 'create', 'dataset'])
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    # add data from a git repo
    result = runner.invoke(
        cli, [
            'dataset', 'add', 'dataset', '-s', filename,
            'https://github.com/SwissDataScienceCenter/renku-python.git'
        ]
    )
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
    path = 'data/dataset/{}'.format(filename)
    assert path in subprocess.check_output(['git', 'lfs', 'ls-files']).decode()


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_renku_clone(runner, monkeypatch):
    """Test cloning of a Renku repo and existence of required settings."""
    from renku.core.management.storage import StorageApiMixin

    remote = 'https://dev.renku.ch/gitlab/virginiafriedrich/datasets-test.git'

    with runner.isolated_filesystem() as project_path:
        result = runner.invoke(cli, ['clone', remote, project_path])
        assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
        assert (Path(project_path) / 'Dockerfile').exists()

        # Check Git hooks are installed
        result = runner.invoke(cli, ['githooks', 'install'])
        assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
        assert 'Hook already exists.' in result.output

        result = runner.invoke(cli, ['migrate'])
        assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

        # Check Git LFS is enabled
        with monkeypatch.context() as monkey:
            # Pretend that git-lfs is not installed.
            monkey.setattr(StorageApiMixin, 'storage_installed', False)
            # Repo is using external storage but it's not installed.
            result = runner.invoke(cli, ['run', 'touch', 'output'])

            assert 'External storage is not configured' in result.output
            assert 1 == result.exit_code, result.output + str(
                result.stderr_bytes
            )


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_renku_clone_with_config(tmpdir):
    """Test cloning of a Renku repo and existence of required settings."""
    remote = 'https://dev.renku.ch/gitlab/virginiafriedrich/datasets-test.git'

    with chdir(str(tmpdir)):
        repo = project_clone(
            remote,
            config={
                'user.name': 'sam',
                'user.email': 's@m.i',
                'filter.lfs.custom': '0'
            }
        )

        assert 'master' == repo.active_branch.name
        reader = repo.config_reader()
        reader.values()

        lfs_config = dict(reader.items('filter.lfs'))
        assert '0' == lfs_config.get('custom')


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_renku_clone_checkout_rev(tmpdir):
    """Test cloning of a Renku repo checking out a rev with static config."""
    remote = 'https://dev.renku.ch/gitlab/virginiafriedrich/datasets-test.git'

    with chdir(str(tmpdir)):
        repo = project_clone(
            remote,
            config={
                'user.name': 'sam',
                'user.email': 's@m.i',
                'filter.lfs.custom': '0'
            },
            checkout_rev='3d387e64ea25079df8dd43b8875058cf9f4b0315',
        )

        assert '3d387e64ea25079df8dd43b8875058cf9f4b0315' == str(
            repo.active_branch
        )
        reader = repo.config_reader()
        reader.values()

        lfs_config = dict(reader.items('filter.lfs'))
        assert '0' == lfs_config.get('custom')


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
@pytest.mark.parametrize('rev', [
    'test-branch',
    'my-tag',
])
def test_renku_clone_checkout_revs(tmpdir, rev):
    """Test cloning of a Renku repo checking out a rev."""
    remote = 'https://dev.renku.ch/gitlab/contact/no-renku.git'

    with chdir(str(tmpdir)):
        repo = project_clone(
            remote,
            checkout_rev=rev,
        )

        assert rev == repo.active_branch.name


@pytest.mark.integration
@pytest.mark.parametrize(
    'path,expected_path', [('', 'datasets-test'), ('.', '.'),
                           ('new-name', 'new-name')]
)
@flaky(max_runs=10, min_passes=1)
def test_renku_clone_uses_project_name(
    runner, monkeypatch, path, expected_path
):
    """Test renku clone uses project name as target-path by default."""
    remote = 'https://dev.renku.ch/gitlab/virginiafriedrich/datasets-test.git'

    with runner.isolated_filesystem() as project_path:
        result = runner.invoke(cli, ['clone', remote, path])
        assert 0 == result.exit_code, result.output + str(result.stderr_bytes)
        assert (Path(project_path) / expected_path / 'Dockerfile').exists()


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_add_removes_credentials(runner, client):
    """Check removal of credentials during adding of remote data files."""
    url = 'https://username:password@example.com/index.html'
    result = runner.invoke(cli, ['dataset', 'add', '-c', 'my-dataset', url])
    assert 0 == result.exit_code, result.output + str(result.stderr_bytes)

    with client.with_dataset('my-dataset') as dataset:
        file_ = dataset.files[0]
        assert file_.url == 'https://example.com/index.html'


@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_check_disk_space(runner, client, monkeypatch):
    """Check adding to dataset prompts if disk space is not enough."""
    url = 'https://example.com/index.html'

    def disk_usage(_):
        """Mocked response."""
        Usage = namedtuple('Usage', 'free')
        return Usage(free=0)

    monkeypatch.setattr(shutil, 'disk_usage', disk_usage)

    result = runner.invoke(
        cli,
        ['dataset', 'add', '-c', 'my-data', url],
        catch_exceptions=False,
    )
    assert 1 == result.exit_code, result.output + str(result.stderr_bytes)
    assert 'Insufficient disk space' in result.output

    result = runner.invoke(cli, ['dataset', 'ls-files'])
    assert 'index.html' not in result.output + str(result.stderr_bytes)


@pytest.mark.migration
@pytest.mark.integration
@flaky(max_runs=10, min_passes=1)
def test_migration_submodule_datasets(
    isolated_runner, old_repository_with_submodules
):
    """Test migration of datasets that use submodules."""
    from renku.core.management import LocalClient

    repo = old_repository_with_submodules
    project_path = repo.working_dir
    os.chdir(project_path)

    assert {'local-repo', 'r10e-ds-py'} == {s.name for s in repo.submodules}

    result = isolated_runner.invoke(cli, ['migrate'])
    assert 0 == result.exit_code

    assert [] == repo.submodules

    client = LocalClient(path=project_path)

    with client.with_dataset('local') as dataset:
        for file_ in dataset.files:
            path = Path(file_.path)
            assert path.exists()
            assert not path.is_symlink()
            assert file_.based_on is None
            assert file_.url.startswith('file://')

    with client.with_dataset('remote') as dataset:
        for file_ in dataset.files:
            path = Path(file_.path)
            assert path.exists()
            assert not path.is_symlink()
            assert file_.based_on is not None
            assert file_.based_on.based_on is None
            assert file_.name == file_.based_on.name


@pytest.mark.integration
@pytest.mark.parametrize(
    'url,size', [('https://www.dropbox.com/s/qcpts6fc81x6j4f/addme?dl=0', 5)]
)
@flaky(max_runs=10, min_passes=1)
def test_dataset_add_dropbox(runner, client, project, url, size):
    """Test importing data from dropbox."""
    result = runner.invoke(
        cli,
        ['dataset', 'add', '-c', 'my-dropbox-data', url],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code

    filename = Path(parse.urlparse(url).path).name
    assert url

    datafile = Path(project) / 'data/my-dropbox-data' / filename
    assert size == len(datafile.read_text())
