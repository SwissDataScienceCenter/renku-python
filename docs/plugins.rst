..
    Copyright 2017-2023 - Swiss Data Science Center (SDSC)
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

Renku CLI Plugins
=================

renku-mls
---------

`renku-mls <https://pypi.org/project/renku-mls/>`_ is a plugin for machine
learning models. Using Renku MLS one can expose the machine learning models
used in Renku projects (e.g. hyper-parameters and evaluation metrics).
It currently supports tracking metadata for ``scikit-learn``, ``keras`` and
``xgboost``. It adds custom metadata to the metadata tracked in renku workflows
about models used, hyper-parameters and metrics and adds a ``renku mls leaderboard``
command to the renku CLI that allows comparing different executions of your
data science pipeline.

`Documentation <https://github.com/ratschlab/renku-mls/blob/master/docs/gettingstarted.rst>`_
`Example Project <https://renkulab.io/projects/learn-renku/plugins/renku-mls-plugin>`_

renku-graph-vis
---------

`renku-graph-vis <https://github.com/oda-hub/renku-graph-vis/>`_ is a plugin that
provides a graphical representation of the renku repository's knowledge graph
from within the renku session. It provides two CLI commands:

* ``display`` to generate a representation of the graph over a png output image
* ``show-graph`` to start an interactive visualization of the graph over the browser

Furthermore, the plugin enables an interactive graph visualization feature 
for real-time monitoring during a renku session introducing the ability to have 
a live overview of the ongoing development.

renku-aqs-annotation
---------

`renku-aqs-annotation <https://github.com/oda-hub/renku-aqs-annotation/>`_ is a plugin that
intercepts several key astroquery methods and stores a number of dedicated annotations 
containing information about the calls to these methods (like the arguments used in the call) 
to the project's Knowledge Graph. 

Developing a plugin?
--------------------

For more information on developing a plugin, please refer to :ref:`develop-plugins-reference`.

If you are working on a Renku plugin and would like to have it listed here,
please create a pull request modifying this document in the
`renku-python repository <https://github.com/SwissDataScienceCenter/renku-python>`_.
