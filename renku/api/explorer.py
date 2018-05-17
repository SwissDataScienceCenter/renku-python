# -*- coding: utf-8 -*-
#
# Copyright 2017 - Swiss Data Science Center (SDSC)
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
"""Client for the explorer service."""


class ExplorerApiMixin(object):
    """Client for handling storage buckets."""

    def list_buckets(self):
        """Return a list of buckets."""
        resp = self.get(
            self._url('/api/explorer/storage/bucket'),
            expected_status_code=200
        )

        # parse each bucket JSON and flatten
        buckets = [_flatten_vertex(bucket) for bucket in resp.json()]
        return buckets

    def get_bucket(self, bucket_id):
        """Retrieve a bucket using the Explorer."""
        resp = self.get(
            self._url('/api/explorer/storage/bucket/{0}'.format(bucket_id)),
            expected_status_code=200
        )
        return _flatten_vertex(resp.json())

    def get_context_lineage(self, context_id):
        """Retrieve all nodes connected to a context."""
        resp = self.get(
            self._url('/api/explorer/lineage/context/{0}'.format(context_id)),
            expected_status_code=200
        )

        vertices = []
        edges = []
        for p in resp.json():
            vertices.append(_flatten_vertex(p['vertex']))
            edges.append(p['edge'])
        return vertices, edges

    def get_bucket_files(self, bucket_id):
        """Retrieve files stored in the bucket with bucket_id."""
        resp = self.get(
            self._url(
                '/api/explorer/storage/bucket/{0}/files'.format(bucket_id)
            ),
            expected_status_code=200
        )
        return [_flatten_vertex(vertex) for vertex in resp.json()]

    def get_file(self, file_id):
        """Retrieve a file metadata using the Explorer."""
        resp = self.get(
            self._url('/api/explorer/storage/file/{0}'.format(file_id)),
            expected_status_code=200
        )

        return _flatten_vertex(resp.json()['data'])

    def get_file_versions(self, file_id):
        """Retrieve file versions for the given file identifer."""
        resp = self.get(
            self._url(
                '/api/explorer/storage/file/{0}/versions'.format(file_id)
            ),
            expected_status_code=200
        )
        return [_flatten_vertex(vertex) for vertex in resp.json()]


def _flatten_vertex(vertex_json):
    """Flatten the nested json structure returned by the Explorer."""
    vertex = {'id': vertex_json['id'], 'properties': {}}
    for prop in vertex_json['properties']:
        if prop['cardinality'] == 'single':
            vals = prop['values'][0]['value']
        elif prop['cardinality'] == 'set':
            vals = {v['value'] for v in prop['values']}
        elif prop['cardinality'] == 'list':
            vals = [v['value'] for v in prop['values']]
        else:
            raise RuntimeError('Undefined property cardinality')
        vertex['properties'][prop['key']] = vals
    return vertex
