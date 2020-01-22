# -*- coding: utf-8 -*-
#
# Copyright 2020 - Swiss Data Science Center (SDSC)
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
"""Renku service datasets view."""
import json
from pathlib import Path

from flask import Blueprint, jsonify, request
from flask_apispec import marshal_with, use_kwargs
from marshmallow import EXCLUDE

from renku.core.commands.dataset import add_file, create_dataset, \
    list_datasets, list_files
from renku.core.utils.contexts import chdir
from renku.service.config import INTERNAL_FAILURE_ERROR_CODE, \
    INVALID_PARAMS_ERROR_CODE, SERVICE_PREFIX
from renku.service.serializers.datasets import DatasetAddRequest, \
    DatasetAddResponse, DatasetAddResponseRPC, DatasetCreateRequest, \
    DatasetCreateResponse, DatasetCreateResponseRPC, DatasetDetails, \
    DatasetFileDetails, DatasetFilesListRequest, DatasetFilesListResponse, \
    DatasetFilesListResponseRPC, DatasetListRequest, DatasetListResponse, \
    DatasetListResponseRPC
from renku.service.utils import make_file_path, make_project_path, repo_sync
from renku.service.views.decorators import accepts_json, handle_base_except, \
    handle_git_except, handle_renku_except, handle_validation_except, \
    header_doc, requires_cache, requires_identity

DATASET_BLUEPRINT_TAG = 'datasets'
dataset_blueprint = Blueprint(
    DATASET_BLUEPRINT_TAG, __name__, url_prefix=SERVICE_PREFIX
)


@use_kwargs(DatasetListRequest, locations=['query'])
@marshal_with(DatasetListResponseRPC)
@header_doc('List all datasets in project.', tags=(DATASET_BLUEPRINT_TAG, ))
@dataset_blueprint.route(
    '/datasets.list',
    methods=['GET'],
    provide_automatic_options=False,
)
@handle_base_except
@handle_git_except
@handle_renku_except
@handle_validation_except
@requires_cache
@requires_identity
def list_datasets_view(user, cache):
    """List all datasets in project."""
    req = DatasetListRequest().load(request.args)
    project = cache.get_project(user, req['project_id'])
    project_path = make_project_path(user, project)

    if not project_path:
        return jsonify(
            error={
                'code': INVALID_PARAMS_ERROR_CODE,
                'reason': 'invalid project_id argument',
            }
        )

    with chdir(project_path):
        datasets = [
            DatasetDetails().load(ds, unknown=EXCLUDE)
            # TODO: fix core interface to address this issue (add ticket ref)
            for ds in json.loads(list_datasets(None, 'data', 'json-ld'))
        ]

    response = DatasetListResponse().load({'datasets': datasets})
    return jsonify(DatasetListResponseRPC().load({'result': response}))


@use_kwargs(DatasetFilesListRequest, locations=['query'])
@marshal_with(DatasetFilesListResponseRPC)
@header_doc('List files in a dataset.', tags=(DATASET_BLUEPRINT_TAG, ))
@dataset_blueprint.route(
    '/datasets.files_list',
    methods=['GET'],
    provide_automatic_options=False,
)
@handle_base_except
@handle_git_except
@handle_renku_except
@handle_validation_except
@requires_cache
@requires_identity
def list_dataset_files_view(user, cache):
    """List files in a dataset."""
    ctx = DatasetFilesListRequest().load(request.args)
    project = cache.get_project(user, ctx['project_id'])
    project_path = make_project_path(user, project)

    if not project_path:
        return jsonify(
            error={
                'code': INVALID_PARAMS_ERROR_CODE,
                'reason': 'invalid project_id argument',
            }
        )

    with chdir(project_path):
        dataset_files = json.loads(
            # TODO: fix core interface to address this issue (add ticket ref)
            list_files(ctx['dataset_name'], None, None, None, 'json-ld')
        )
        ctx['files'] = [
            DatasetFileDetails().load(ds, unknown=EXCLUDE)
            for ds in dataset_files
        ]

    response = DatasetFilesListResponse().load(ctx, unknown=EXCLUDE)
    return jsonify(DatasetFilesListResponseRPC().load({'result': response}))


@use_kwargs(DatasetAddRequest)
@marshal_with(DatasetAddResponseRPC)
@header_doc(
    'Add uploaded file to cloned repository.', tags=(DATASET_BLUEPRINT_TAG, )
)
@dataset_blueprint.route(
    '/datasets.add',
    methods=['POST'],
    provide_automatic_options=False,
)
@handle_base_except
@handle_git_except
@handle_renku_except
@handle_validation_except
@accepts_json
@requires_cache
@requires_identity
def add_file_to_dataset_view(user, cache):
    """Add the uploaded file to cloned repository."""
    ctx = DatasetAddRequest().load(request.json)
    project = cache.get_project(user, ctx['project_id'])
    project_path = make_project_path(user, project)

    if not project_path:
        return jsonify(
            error={
                'code': INVALID_PARAMS_ERROR_CODE,
                'message': 'invalid project_id: {0}'.format(ctx['project_id']),
            }
        )

    if not ctx['commit_message']:
        ctx['commit_message'] = 'service: dataset add {0}'.format(
            ctx['dataset_name']
        )

    local_paths = []
    for _file in ctx['files']:
        local_path = None

        if 'file_id' in _file:
            file = cache.get_file(user, _file['file_id'])
            local_path = make_file_path(user, file)

        elif 'file_path' in _file:
            local_path = project_path / Path(_file['file_path'])

        if not local_path or not local_path.exists():
            return jsonify(
                error={
                    'code': INVALID_PARAMS_ERROR_CODE,
                    'message':
                        'invalid file reference: {0}'.
                        format(local_path.relative_to(project_path))
                }
            )

        ctx['commit_message'] += ' {0}'.format(local_path.name)
        local_paths.append(str(local_path))

    with chdir(project_path):
        add_file(
            local_paths,
            ctx['dataset_name'],
            create=ctx['create_dataset'],
            commit_message=ctx['commit_message']
        )

        if not repo_sync(project_path):
            return jsonify(
                error={
                    'code': INTERNAL_FAILURE_ERROR_CODE,
                    'message': 'repo sync failed'
                }
            )

    return jsonify(
        DatasetAddResponseRPC().load({
            'result': DatasetAddResponse().load(ctx, unknown=EXCLUDE)
        })
    )


@use_kwargs(DatasetCreateRequest)
@marshal_with(DatasetCreateResponseRPC)
@header_doc(
    'Create a new dataset in a project.', tags=(DATASET_BLUEPRINT_TAG, )
)
@dataset_blueprint.route(
    '/datasets.create',
    methods=['POST'],
    provide_automatic_options=False,
)
@handle_base_except
@handle_git_except
@handle_renku_except
@handle_validation_except
@accepts_json
@requires_cache
@requires_identity
def create_dataset_view(user, cache):
    """Create a new dataset in a project."""
    ctx = DatasetCreateRequest().load(request.json)
    project = cache.get_project(user, ctx['project_id'])

    project_path = make_project_path(user, project)
    if not project_path:
        return jsonify(
            error={
                'code': INVALID_PARAMS_ERROR_CODE,
                'message': 'invalid project_id argument',
            }
        )

    with chdir(project_path):
        create_dataset(
            ctx['dataset_name'],
            commit_message=ctx['commit_message'],
            creators=ctx.get('creators'),
            description=ctx.get('description'),
        )

    if not repo_sync(project_path):
        return jsonify(
            error={
                'code': INTERNAL_FAILURE_ERROR_CODE,
                'reason': 'push to remote failed silently - try again'
            }
        )

    return jsonify(
        DatasetCreateResponseRPC().load({
            'result': DatasetCreateResponse().load(ctx, unknown=EXCLUDE)
        })
    )
