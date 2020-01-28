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
"""Renku service cache views."""
import os
import shutil
from pathlib import Path

import patoolib
from flask import Blueprint, jsonify, request
from flask_apispec import marshal_with, use_kwargs
from marshmallow import EXCLUDE
from patoolib.util import PatoolError

from renku.core.commands.clone import renku_clone
from renku.service.config import CACHE_UPLOADS_PATH, \
    INVALID_PARAMS_ERROR_CODE, SERVICE_PREFIX, SUPPORTED_ARCHIVES
from renku.service.serializers.cache import FileListResponse, \
    FileListResponseRPC, FileUploadContext, FileUploadRequest, \
    FileUploadResponse, FileUploadResponseRPC, ProjectCloneContext, \
    ProjectCloneRequest, ProjectCloneResponse, ProjectCloneResponseRPC, \
    ProjectListResponse, ProjectListResponseRPC, extract_file
from renku.service.utils import make_project_path, valid_file
from renku.service.views.decorators import accepts_json, handle_base_except, \
    handle_git_except, handle_renku_except, handle_validation_except, \
    header_doc, requires_cache, requires_identity

CACHE_BLUEPRINT_TAG = 'cache'
cache_blueprint = Blueprint('cache', __name__, url_prefix=SERVICE_PREFIX)


@marshal_with(FileListResponseRPC)
@header_doc(description='List uploaded files.', tags=(CACHE_BLUEPRINT_TAG, ))
@cache_blueprint.route(
    '/cache.files_list',
    methods=['GET'],
    provide_automatic_options=False,
)
@handle_base_except
@handle_git_except
@handle_renku_except
@handle_validation_except
@requires_cache
@requires_identity
def list_uploaded_files_view(user, cache):
    """List uploaded files ready to be added to projects."""
    files = filter(None, [valid_file(user, f) for f in cache.get_files(user)])

    response_payload = {
        'files':
            sorted(
                files, key=lambda rec: (rec['is_dir'], rec['relative_path'])
            )
    }

    response = FileListResponseRPC().load({
        'result': FileListResponse().load(response_payload)
    })
    return jsonify(response)


@use_kwargs(FileUploadRequest)
@marshal_with(FileUploadResponseRPC)
@header_doc(
    description='Upload file or archive of files.',
    tags=(CACHE_BLUEPRINT_TAG, ),
)
@cache_blueprint.route(
    '/cache.files_upload',
    methods=['POST'],
    provide_automatic_options=False,
)
@handle_base_except
@handle_git_except
@handle_renku_except
@handle_validation_except
@requires_cache
@requires_identity
def upload_file_view(user, cache):
    """Upload file or archive of files."""
    file = extract_file(request)

    response_builder = {
        'file_name': file.filename,
        'content_type': file.content_type,
        'is_archive': file.content_type in SUPPORTED_ARCHIVES
    }
    response_builder.update(FileUploadRequest().load(request.args))

    user_cache_dir = CACHE_UPLOADS_PATH / user['user_id']
    user_cache_dir.mkdir(exist_ok=True)

    file_path = user_cache_dir / file.filename
    if file_path.exists():
        if response_builder.get('override_existing', False):
            file_path.unlink()
        else:
            return jsonify(
                error={
                    'code': INVALID_PARAMS_ERROR_CODE,
                    'reason': 'file exists',
                }
            )

    file.save(str(file_path))

    files = []
    if response_builder['unpack_archive'] and response_builder['is_archive']:
        unpack_dir = '{0}.unpacked'.format(file_path.name)
        temp_dir = file_path.parent / Path(unpack_dir)
        if temp_dir.exists():
            shutil.rmtree(str(temp_dir))
        temp_dir.mkdir(exist_ok=True)

        try:
            patoolib.extract_archive(str(file_path), outdir=str(temp_dir))
        except PatoolError:
            return jsonify(
                error={
                    'code': INVALID_PARAMS_ERROR_CODE,
                    'reason': 'unable to unpack archive'
                }
            )

        for file_ in temp_dir.glob('**/*'):
            relative_path = file_.relative_to(
                CACHE_UPLOADS_PATH / user['user_id']
            )

            file_obj = {
                'file_name': file_.name,
                'file_size': os.stat(str(file_path)).st_size,
                'relative_path': str(relative_path),
                'is_dir': relative_path.is_dir(),
            }

            files.append(FileUploadContext().load(file_obj, unknown=EXCLUDE))

    else:
        relative_path = file_path.relative_to(
            CACHE_UPLOADS_PATH / user['user_id']
        )
        response_builder['file_size'] = os.stat(str(file_path)).st_size
        response_builder['relative_path'] = str(relative_path)
        response_builder['is_dir'] = relative_path.is_dir()

        files.append(
            FileUploadContext().load(response_builder, unknown=EXCLUDE)
        )

    response = FileUploadResponseRPC().load({
        'result': FileUploadResponse().load({'files': files})
    })
    cache.set_files(user, files)

    return jsonify(response)


@use_kwargs(ProjectCloneRequest)
@marshal_with(ProjectCloneResponseRPC)
@header_doc(
    'Clone a remote project. If the project is cached already, '
    'new clone operation will override the old cache state.',
    tags=(CACHE_BLUEPRINT_TAG, )
)
@cache_blueprint.route(
    '/cache.project_clone',
    methods=['POST'],
    provide_automatic_options=False,
)
@handle_base_except
@handle_git_except
@handle_renku_except
@handle_validation_except
@requires_cache
@requires_identity
@accepts_json
def project_clone(user, cache):
    """Clone a remote repository."""
    ctx = ProjectCloneContext().load(
        (lambda a, b: a.update(b) or a)(request.json, user),
        unknown=EXCLUDE,
    )

    local_path = make_project_path(user, ctx)

    if local_path.exists():
        shutil.rmtree(str(local_path))

        for project in cache.get_projects(user):
            if project['git_url'] == ctx['git_url']:
                cache.invalidate_project(user, project['project_id'])

    local_path.mkdir(parents=True, exist_ok=True)
    renku_clone(
        ctx['url_with_auth'],
        local_path,
        depth=ctx['depth'],
        raise_git_except=True,
        config={
            'user.name': ctx['fullname'],
            'user.email': ctx['email'],
        }
    )
    cache.set_project(user, ctx['project_id'], ctx)

    response = ProjectCloneResponseRPC().load({
        'result': ProjectCloneResponse().load(ctx, unknown=EXCLUDE)
    })
    return jsonify(response)


@marshal_with(ProjectListResponseRPC)
@header_doc(
    'List cached projects.',
    tags=(CACHE_BLUEPRINT_TAG, ),
)
@cache_blueprint.route(
    '/cache.project_list',
    methods=['GET'],
    provide_automatic_options=False,
)
@handle_base_except
@handle_git_except
@handle_renku_except
@handle_validation_except
@requires_cache
@requires_identity
def list_projects_view(user, cache):
    """List cached projects."""
    projects = cache.get_projects(user)
    projects = [
        ProjectCloneResponse().load(p, unknown=EXCLUDE)
        for p in projects if make_project_path(user, p).exists()
    ]

    response = ProjectListResponseRPC().load({
        'result': ProjectListResponse().load({'projects': projects})
    })
    return jsonify(response)
