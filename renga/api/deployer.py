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
"""Client for deployer service."""


class ContextsApiMixin(object):
    """Manage deployer contexts."""

    def create_context(self, spec):
        """Create a new deployer context."""
        resp = self.post(self._url('/api/deployer/contexts'), json=spec)
        return resp.json()

    def get_context(self, context_id):
        """List all known contexts."""
        resp = self.get(self._url('/api/deployer/contexts/{0}', context_id))
        return resp.json()

    def list_contexts(self):
        """List all known contexts."""
        resp = self.get(self._url('/api/deployer/contexts'))
        return resp.json()['contexts']

    def list_executions(self, context_id):
        """List all executions of a given context."""
        resp = self.get(
            self._url('/api/deployer/contexts/{0}/executions', context_id))
        return resp.json()['executions']

    def create_execution(self, context_id, **kwargs):
        """Create an execution of a context on a given engine."""
        resp = self.post(
            self._url('/api/deployer/contexts/{0}/executions', context_id),
            json=kwargs)
        return resp.json()

    # TODO fix everything from here down

    def delete_execution(self, context_id, execution_id):
        """Delete an execution."""
        r = requests.delete(
            self.execution_endpoint.format(
                context_id=context_id, execution_id=execution_id),
            headers=self.headers)

        return return_response(r, ok_code=200)

    def execution_logs(self, context_id, execution_id):
        """Retrieve logs of an execution."""
        r = requests.get(
            self.execution_logs_endpoint.format(
                context_id=context_id, execution_id=execution_id),
            headers=self.headers)

        return return_response(r, ok_code=200, return_json=True)

    def execution_ports(self, context_id, execution_id):
        """Retrieve port mappings for an execution."""
        r = requests.get(
            self.execution_ports_endpoint.format(
                context_id=context_id, execution_id=execution_id),
            headers=self.headers)

        return return_response(r, ok_code=200, return_json=True)['ports']
