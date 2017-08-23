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
"""Client for the renga-graph service."""


import requests

from renga.utils import join_url


class KnowledgeGraphClient(object):
    """Knowledge graph client."""

    type_mapping = {'string': str}

    def __init__(self, platform_url):
        """Initialize the KnowledgeGraphClient."""
        self.platform_url = platform_url
        self._named_types = None

    @property
    def named_types(self):
        """Fetch named types from types service."""
        if self._named_types is None:
            self._named_types = requests.get(
                join_url(self.platform_url,
                         'types/management/named_type')).json()
        return self._named_types

    def vertex_operation(self, obj, temp_id, named_type):
        """
        Serialize an object to KnowledgeGraph schema.

        We iterate through the type definitions presented by the graph typesystem
        to extract the pieces we need from the object.

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
                    value = self.type_mapping[prop['data_type']](value)

                    # append the property
                    properties.append({
                        'key':
                        '{named_type}_{key}'.format(
                            named_type=named_type, key='_'.join(prop_names[1:])),
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


    def mutation(operations, wait_for_response=False, token=None):
        """
        Submit a mutation to the graph.

        If ``wait_for_response == True`` the return value is the reponse JSON,
        otherwise the mutation UUID is returned.
        """
        knowledge_graph_url = current_app.config['KNOWLEDGE_GRAPH_URL']
        headers = {'Authorization': token}

        response = requests.post(
            join_url(knowledge_graph_url, '/mutation/mutation'),
            json={'operations': operations},
            headers=headers, )

        uuid = response.json()['uuid']

        if wait_for_response:
            completed = False
            while not completed:
                response = requests.get(
                    join_url(
                        knowledge_graph_url,
                        '/mutation/mutation/{uuid}'.format(uuid=uuid))).json()
                completed = response['status'] == 'completed'
                # sleep for 200 miliseconds
                time.sleep(0.2)

            return response

        return response.json()['uuid']
