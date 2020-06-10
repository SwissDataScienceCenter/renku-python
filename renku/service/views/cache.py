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

from renku.core.commands.clone import project_clone
from renku.core.commands.migrate import migrate_project
from renku.core.utils.contexts import chdir
from renku.service.config import CACHE_UPLOADS_PATH, \
    INVALID_PARAMS_ERROR_CODE, SERVICE_PREFIX, SUPPORTED_ARCHIVES
from renku.service.serializers.cache import FileListResponseRPC, \
    FileUploadRequest, FileUploadResponseRPC, ProjectCloneContext, \
    ProjectCloneRequest, ProjectCloneResponseRPC, ProjectListResponseRPC, \
    ProjectMigrateRequest, ProjectMigrateResponseRPC, extract_file
from renku.service.utils import make_project_path
from renku.service.views import result_response
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
    user = cache.ensure_user(user)

    files = [f for f in cache.get_files(user) if f.exists()]

    response = {
        'files':
            sorted(files, key=lambda rec: (rec.is_dir, rec.relative_path))
    }

    return result_response(FileListResponseRPC(), response)


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
    user = cache.ensure_user(user)
    file = extract_file(request)

    response_builder = {
        'file_name': file.filename,
        'content_type': file.content_type,
        'is_archive': file.content_type in SUPPORTED_ARCHIVES
    }
    response_builder.update(FileUploadRequest().load(request.args))

    user_cache_dir = CACHE_UPLOADS_PATH / user.user_id
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
                CACHE_UPLOADS_PATH / user.user_id
            )

            file_obj = {
                'file_name': file_.name,
                'file_size': os.stat(str(file_path)).st_size,
                'relative_path': str(relative_path),
                'is_dir': relative_path.is_dir(),
            }

            files.append(file_obj)

    else:
        relative_path = file_path.relative_to(
            CACHE_UPLOADS_PATH / user.user_id
        )

        response_builder['file_size'] = os.stat(str(file_path)).st_size
        response_builder['relative_path'] = str(relative_path)
        response_builder['is_dir'] = relative_path.is_dir()

        files.append(response_builder)

    files = cache.set_files(user, files)
    return result_response(FileUploadResponseRPC(), {'files': files})


@requires_cache
def _project_clone(cache, user_data, project_data):
    """Clones the project for a given user."""
    local_path = make_project_path(user_data, project_data)
    user = cache.ensure_user(user_data)

    if local_path.exists():
        shutil.rmtree(str(local_path))

        for project in cache.get_projects(user):
            if project.git_url == project_data['git_url']:
                project.delete()

    local_path.mkdir(parents=True, exist_ok=True)
    project_clone(
        project_data['url_with_auth'],
        local_path,
        depth=project_data['depth'] if project_data['depth'] != 0 else None,
        raise_git_except=True,
        config={
            'user.name': project_data['fullname'],
            'user.email': project_data['email'],
        },
        checkout_rev=project_data['ref']
    )

    project = cache.make_project(user, project_data)
    return project


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
@requires_identity
@accepts_json
def project_clone_view(user_data):
    """Clone a remote repository."""
    project_data = ProjectCloneContext().load({
        **user_data,
        **request.json
    },
                                              unknown=EXCLUDE)
    project = _project_clone(user_data, project_data)

    return result_response(ProjectCloneResponseRPC(), project)


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
    projects = [
        project for project in cache.get_projects(cache.ensure_user(user))
        if project.abs_path.exists()
    ]

    return result_response(ProjectListResponseRPC(), {'projects': projects})


@use_kwargs(ProjectMigrateRequest)
@marshal_with(ProjectMigrateResponseRPC)
@header_doc(
    'Migrate project to the latest version.',
    tags=(CACHE_BLUEPRINT_TAG, ),
)
@cache_blueprint.route(
    '/cache.migrate',
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
def migrate_project_view(user_data, cache):
    """Migrate specified project."""
    user = cache.ensure_user(user_data)
    project = cache.get_project(user, request.json['project_id'])

    messages = []

    def collect_message(msg):
        """Collect migration message."""
        messages.append(msg)

    with chdir(project.abs_path):
        was_migrated = migrate_project(progress_callback=collect_message)

    return result_response(
        ProjectMigrateResponseRPC(), {
            'messages': messages,
            'was_migrated': was_migrated
        }
    )
