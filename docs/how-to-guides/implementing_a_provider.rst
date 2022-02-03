.. _implementing_a_provider:

Implementing a workflow provider
================================

In  :ref:`hpc`, we described how using different workflow providers can enable
the user running renku workflows on HPC. Here we discuss how to implement a new,
custom workflow provider as a plugin for Renku CLI.

Renku provides the option to add a new workflow executor backend with the
help of `pluggy <https://pluggy.readthedocs.io/en/latest/>`_ plugins.

In order to implement such a plugin, the developer of the new workflow
provider plugin should provide the new executor implementation by
implementing the ``IWorkflowProvider`` interface.

A simple example of a ``MyProvider`` workflow executor plugin:

 .. code-block:: python

        from pathlib import Path
        from typing import Any, Dict, List

        from renku.core.models.workflow.provider import IWorkflowProvider
        from renku.core.plugins import hookimpl

        class MyProvider(IWorkflowProvider):
            @hookimpl
            def workflow_provider(self):
                """Workflow provider name."""
                return (self, "my")

            @hookimpl
            def workflow_execute(self, dag: "nx.DiGraph", basedir: Path, config: Dict[str, Any]):
                """Executing the ``Plans``."""
                generated_outputs: List[str] = []

                # traversing the dag that contains the ``Plans``
                # and executing each plan in the graph
                ...

                return generated_outputs

The execution of the workflow(s) shall be defined in ``workflow_execute`` function, where

  - ``dag`` is a Directed Acyclic Graph of :py:class:`Plans<renku.core.models.workflow.plan>`
    to be executed represented with a
    `networkx.DiGraph <https://networkx.org/documentation/stable/reference/classes/digraph.html>`_,
  - ``basedir`` is the absolute path to the project,
  - ``config`` dictionary contains the provider related optional configuration parameters.

The ``workflow_provider`` function shall return a tuple of ``(object, str)``,  where object
should be the plugin object, i.e. ``self`` and the string is a unique identifier of the
provider plugin. This unique string will be the string that the user can provide to the
``--provider`` command line argument to select this plugin for executing the desired
workflows. A dummy provider implementation is available
`here <https://github.com/SwissDataScienceCenter/renku-dummy-provider>`_ in order to ease the
initial implementation of a provider plugin.

A provider HAS to set environment variables for a plans parameters, so they can be used by scripts.
These environment variables have to be prefixed with the value of the
``renku.core.plugins.provider.RENKU_ENV_PREFIX`` constant. So for a parameter with name
``my-param`` and the ``RENKU_ENV_PREFIX`` value of ``RENKU_ENV_``, the environment variable
should be called ``RENKU_ENV_my-param``.
