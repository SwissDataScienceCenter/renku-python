..
    Copyright 2017-2020 - Swiss Data Science Center (SDSC)
    A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
    Eidgenössische Technische Hochschule Zürich (ETHZ).

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.


Plugin Support
==============

Plugins are supported using the `pluggy <https://pluggy.readthedocs.io/en/latest/>`_ library.

Plugins can be created as python packages that contain the respective entrypoint definition in their `setup.py` file, like so:

.. code-block:: python

    from setuptools import setup

    setup(
        ...
        entry_points={"renku": ["name_of_plugin = myproject.pluginmodule"]},
        ...
    )


where `myproject.pluginmodule` points to a Renku `hookimpl` e.g.:

.. code-block:: python

    from renku.core.plugins import hookimpl

    @hookimpl
    def plugin_hook_implementation(param1, param2):
        ...


``renku run`` hooks
-------------------

.. automodule:: renku.core.plugins.run
   :members:
