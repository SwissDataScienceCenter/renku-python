# -*- coding: utf-8 -*-
#
# Copyright 2017 Swiss Data Science Center
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
"""Client for the Renga graph mutation service."""

import time

import requests
from werkzeug.utils import cached_property

from renga.client._datastructures import Endpoint, EndpointMixin
from renga.client.authorization import AuthorizationClient


class GraphMutationClient(EndpointMixin):
    """Client for handling graph mutations."""

    TYPE_MAPPING = {'string': str}

    mutation_url = Endpoint('/api/mutation/mutation')
    mutation_status_url = Endpoint('/api/mutation/mutation/{uuid}')
    named_type_url = Endpoint('/api/types/management/named_type')

    def __init__(self, endpoint, authorization_client=None):
        """Create a new instance of graph mutation client."""
        EndpointMixin.__init__(self, endpoint)
        self.authorization = authorization_client or AuthorizationClient(
            endpoint)

    @cached_property
    def named_types(self):
        """Fetch named types from types service."""
        return requests.get(
            self.named_type_url,
            headers=self.authorization.service_headers).json()

    def vertex_operation(self, obj, temp_id, named_type):
        """Serialize an object to GraphMutation schema.

        We iterate through the type definitions presented by the graph
        typesystem to extract the pieces we need from the object.

        TODO: use marshmallow or similar to serialize
        """
        # named_types are in format `namespace:name`
        domain, name = named_type.split(':')

        properties = []
        for t in self.named_types:
            if t['name'] == name:
                for prop in t['properties']:
                    prop_names = prop['name'].split('_')
                    try:
                        if len(prop_names) == 2:
                            value = getattr(obj, prop_names[1])
                        elif len(prop_names) == 3:
                            value = getattr(obj, prop_names[1])[prop_names[2]]
                        else:
                            raise RuntimeError('Bad format for named type')
                    except (KeyError, AttributeError):
                        # the property was not found in obj, go to the next one
                        continue

                    # map to correct type
                    value = self.TYPE_MAPPING[prop['data_type']](value)

                    # append the property
                    properties.append({
                        'key':
                        '{named_type}_{key}'.format(
                            named_type=named_type,
                            key='_'.join(prop_names[1:])),
                        'data_type':
                        prop['data_type'],
                        'cardinality':
                        prop['cardinality'],
                        'values': [{
                            'key':
                            '{named_type}_{key}'.format(
                                named_type=named_type,
                                key='_'.join(prop_names[1:])),
                            'data_type':
                            prop['data_type'],
                            'value':
                            value
                        }]
                    })

        operation = {
            'type': 'create_vertex',
            'element': {
                'temp_id': temp_id,
                'types': ['{named_type}'.format(named_type=named_type)],
                'properties': properties
            }
        }

        return operation

    def mutation(self,
                 operations,
                 wait_for_response=False,
                 retries=10):
        """Submit a mutation to the graph.

        If ``wait_for_response == True`` the return value is the reponse JSON,
        otherwise the mutation UUID is returned.
        """
        response = requests.post(
            self.mutation_url,
            json={'operations': operations},
            headers=self.authorization.service_headers)

        uuid = response.json()['uuid']

        if wait_for_response:
            while retries:
                response = requests.get(
                    self.mutation_status_url.format(uuid=uuid)).json()
                completed = response['status'] == 'completed'
                if completed:
                    break
                # sleep for 200 miliseconds
                time.sleep(0.2)
                retries -= 1

            if response['response']['event']['status'] == 'success':
                vertex_id = response['response']['event']['results'][0]['id']
                return vertex_id
            else:
                raise RuntimeError('Adding vertex failed')

        return response.json()['uuid']
