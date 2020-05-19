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
import os
import uuid
from pathlib import Path

from flask import Blueprint, request
from flask_apispec import marshal_with, use_kwargs
from git import GitCommandError

from renku.core.commands.dataset import add_file, create_dataset, \
    edit_dataset, list_datasets, list_files
from renku.core.models import json
from renku.core.utils.contexts import chdir
from renku.service.cache.serializers.job import USER_JOB_STATE_ENQUEUED
from renku.service.config import INTERNAL_FAILURE_ERROR_CODE, \
    INVALID_PARAMS_ERROR_CODE, SERVICE_PREFIX
from renku.service.jobs.contexts import enqueue_retry
from renku.service.jobs.datasets import dataset_add_remote_file, dataset_import
from renku.service.jobs.queues import DATASETS_JOB_QUEUE
from renku.service.serializers.datasets import DatasetAddRequest, \
    DatasetAddResponseRPC, DatasetCreateRequest, DatasetCreateResponseRPC, \
    DatasetEditRequest, DatasetEditResponseRPC, DatasetFilesListRequest, \
    DatasetFilesListResponseRPC, DatasetImportRequest, \
    DatasetImportResponseRPC, DatasetListRequest, DatasetListResponseRPC
from renku.service.utils import repo_sync
from renku.service.views import error_response, result_response
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
    ctx = DatasetListRequest().load(request.args)
    project = cache.get_project(cache.ensure_user(user), ctx['project_id'])

    if not project.abs_path.exists():
        return error_response(
            INVALID_PARAMS_ERROR_CODE, 'invalid project_id argument'
        )

    with chdir(project.abs_path):
        ctx['datasets'] = list_datasets()

    return result_response(DatasetListResponseRPC(), ctx)


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
    project = cache.get_project(cache.ensure_user(user), ctx['project_id'])

    if not project.abs_path.exists():
        return error_response(
            INVALID_PARAMS_ERROR_CODE, 'invalid project_id argument'
        )

    with chdir(project.abs_path):
        ctx['files'] = list_files(datasets=[ctx['short_name']])

    return result_response(DatasetFilesListResponseRPC(), ctx)


@use_kwargs(DatasetAddRequest)
@marshal_with(DatasetAddResponseRPC)
@header_doc(
    'Add an uploaded file to a cloned repository.',
    tags=(DATASET_BLUEPRINT_TAG, )
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
def add_file_to_dataset_view(user_data, cache):
    """Add the uploaded file to cloned repository."""
    ctx = DatasetAddRequest().load(request.json)
    user = cache.ensure_user(user_data)
    project = cache.get_project(user, ctx['project_id'])

    if not project.abs_path.exists():
        return error_response(
            INVALID_PARAMS_ERROR_CODE,
            'invalid project_id: {0}'.format(ctx['project_id'])
        )

    if not ctx['commit_message']:
        ctx['commit_message'] = 'service: dataset add {0}'.format(
            ctx['short_name']
        )

    local_paths = []
    for _file in ctx['files']:
        local_path = None

        if 'file_url' in _file:
            commit_message = '{0}{1}'.format(
                ctx['commit_message'], _file['file_url']
            )

            job = cache.make_job(user)
            _file['job_id'] = job.job_id

            with enqueue_retry(DATASETS_JOB_QUEUE) as queue:
                queue.enqueue(
                    dataset_add_remote_file, user_data, job.job_id,
                    project.project_id, ctx['create_dataset'], commit_message,
                    ctx['short_name'], _file['file_url']
                )
            continue

        if 'file_id' in _file:
            file = cache.get_file(user, _file['file_id'])
            local_path = file.abs_path

        elif 'file_path' in _file:
            local_path = project.abs_path / Path(_file['file_path'])

        if not local_path or not local_path.exists():
            return error_response(
                INVALID_PARAMS_ERROR_CODE,
                'invalid file reference: {0}'.format(json.dumps(_file))
            )

        ctx['commit_message'] += ' {0}'.format(local_path.name)
        local_paths.append(str(local_path))

    if local_paths:
        with chdir(project.abs_path):
            add_file(
                local_paths,
                ctx['short_name'],
                create=ctx['create_dataset'],
                force=ctx['force'],
                commit_message=ctx['commit_message']
            )

            try:
                ctx['remote_branch'] = repo_sync(project.abs_path)
            except GitCommandError:
                return error_response(
                    INTERNAL_FAILURE_ERROR_CODE, 'repo sync failed'
                )

    return result_response(DatasetAddResponseRPC(), ctx)


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
    project = cache.get_project(cache.ensure_user(user), ctx['project_id'])

    if not project.abs_path.exists():
        return error_response(
            INVALID_PARAMS_ERROR_CODE, 'invalid project_id argument'
        )

    with chdir(project.abs_path):
        create_dataset(
            ctx['short_name'],
            title=ctx.get('name'),
            creators=ctx.get('creator'),
            description=ctx.get('description'),
            keywords=ctx.get('keywords'),
            commit_message=ctx['commit_message']
        )

    try:
        ctx['remote_branch'] = repo_sync(project.abs_path)
    except GitCommandError:
        return error_response(
            INTERNAL_FAILURE_ERROR_CODE,
            'push to remote failed silently - try again'
        )

    return result_response(DatasetCreateResponseRPC(), ctx)


@use_kwargs(DatasetImportRequest)
@marshal_with(DatasetImportResponseRPC)
@header_doc('Import a dataset', tags=(DATASET_BLUEPRINT_TAG, ))
@dataset_blueprint.route(
    '/datasets.import',
    methods=['POST'],
    provide_automatic_options=False,
)
@handle_base_except
@handle_validation_except
@requires_cache
@requires_identity
def import_dataset_view(user_data, cache):
    """Import a dataset view."""
    user = cache.ensure_user(user_data)
    ctx = DatasetImportRequest().load(request.json)
    project = cache.get_project(user, ctx['project_id'])

    if project is None or project.abs_path is False:
        return error_response(
            INVALID_PARAMS_ERROR_CODE,
            'invalid project_id: {0}'.format(ctx['project_id'])
        )

    user_job = {
        'job_id': uuid.uuid4().hex,
        'state': USER_JOB_STATE_ENQUEUED,
    }
    job = cache.make_job(user, user_job, locked=ctx['project_id'])

    with enqueue_retry(DATASETS_JOB_QUEUE) as queue:
        queue.enqueue(
            dataset_import,
            user_data,
            user_job['job_id'],
            ctx['project_id'],
            ctx['dataset_uri'],
            short_name=ctx.get('short_name'),
            extract=ctx.get('extract', False),
            timeout=int(os.getenv('WORKER_DATASET_JOBS_TIMEOUT', 1800)),
            result_ttl=int(os.getenv('WORKER_DATASET_JOBS_RESULT_TTL', 500))
        )

    return result_response(DatasetImportResponseRPC(), job)


@use_kwargs(DatasetEditRequest)
@marshal_with(DatasetEditResponseRPC)
@header_doc('Edit dataset metadata', tags=(DATASET_BLUEPRINT_TAG, ))
@dataset_blueprint.route(
    '/datasets.edit',
    methods=['POST'],
    provide_automatic_options=False,
)
@handle_base_except
@handle_validation_except
@requires_cache
@requires_identity
def edit_dataset_view(user_data, cache):
    """Edit dataset metadata."""
    ctx = DatasetEditRequest().load(request.json)

    user = cache.ensure_user(user_data)
    project = cache.get_project(user, ctx['project_id'])

    if project is None or project.abs_path is False:
        return error_response(
            INVALID_PARAMS_ERROR_CODE,
            'invalid project_id: {0}'.format(ctx['project_id'])
        )

    if ctx.get('commit_message') is None:
        ctx['commit_message'] = 'service: dataset edit {0}'.format(
            ctx['short_name']
        )

    with chdir(project.abs_path):
        edited, warnings = edit_dataset(
            ctx['short_name'],
            ctx.get('title'),
            ctx.get('description'),
            ctx.get('creators'),
            keywords=ctx.get('keywords'),
            commit_message=ctx['commit_message']
        )

    return result_response(
        DatasetEditResponseRPC(), {
            'edited': {field: ctx.get(field)
                       for field in edited},
            'warnings': warnings
        }
    )
