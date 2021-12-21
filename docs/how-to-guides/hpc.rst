.. _hpc:

Running Renku on HPC
====================

The Renku CLI supports various backends for executing workflows. Currently, two
different providers are implemented, namely ``cwltool`` and ``toil``. If you
have a specific provider you need for your infrastructure, have a look at
:doc:`implementing_a_provider` for a detailed description of how to implement
your own workflow provider. Alternatively, please `make a feature request
<https://github.com/SwissDataScienceCenter/renku-python/issues/new?assignees=&labels=&template=feature_request.md>`_.

By default all workflows are executed by the ``cwltool`` provider, that
exports the workflow to CWL and then uses `cwltool <https://github.com/common-workflow-language/cwltool>`_
to execute the given CWL.

The workflow backend can be changed by using the ``-p/--provider <PROVIDER>``
command line option. A backend's default configuration can be overridden by
providing the  ``-c/--config <config.yaml>`` command line parameter.
The following ``renku`` commands support the above mentioned workflow provider
related command line options:

 - :ref:`cli-rerun`
 - :ref:`cli-update`
 - :ref:`cli-workflow` ``execute`` and ``iterate``

For example, to execute a previously created ``my_plan`` workflow with ``toil``, one
would simply run the following command:

.. code-block:: console

   $ renku workflow execute -p toil my_plan

Using ``toil`` as a workflow provider has the advantage that it supports running
the workflows on various `high-performance computing <https://toil.readthedocs.io/en/latest/running/hpcEnvironments.html>`_
and `cloud <https://toil.readthedocs.io/en/latest/running/cloud/cloud.html#cloud-platforms>`_
platforms.

In order to use any other provider with ``renku`` you must first install the required
extras. In the case of ``toil`` this means running the install command like

.. code-block:: console

   $ pip install renku[toil]


Renku on Slurm
^^^^^^^^^^^^^^

`Slurm <https://www.schedmd.com/>`_ is a highly configurable open-source
workload manager, which is in widespread use at government laboratories,
universities and companies world wide. It is used in over half of the top 10
systems in the `TOP500 <https://www.top500.org/>`_ listing.

As ``toil`` supports Slurm, one can easily execute the previously created renku
workflows on a Slurm-managed HPC resource. One just needs to provide a simple
configuration file to the provider (``--config/-c``)::

  batchSystem: slurm
  disableCaching: true

The ``disableCaching`` option is necessary for Slurm; for more details see the
related ``toil`` issue `TOIL-1006
<https://ucsc-cgl.atlassian.net/browse/TOIL-1006>`_.

`Additional Slurm-specific parameters <https://slurm.schedmd.com/sbatch.html>`_ can be
provided with the ``TOIL_SLURM_ARGS`` environment variable.

Taking the example above, the following command line will execute ``my_plan`` on Slurm:

.. code-block:: console

   $ TOIL_SLURM_ARGS="-A my_account --export=ALL" renku workflow execute -p toil -c provider.yaml my_plan

where

 - ``provider.yaml`` file contains the above mentioned two parameters for ``toil``,
 - ``-A my_account`` specifies which account should be charged for the used resources,
 - ``--export=ALL`` specifies that all environment variables are propagated to the Slurm workers.
   It is often required in academic Slurm installations.
