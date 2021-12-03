.. _hpc:

Running Renku on HPC
====================

Renku CLI supports various backends for executing workflows. Currently, there
are two different providers are implemented, namely ``cwltool`` and ``toil``.
:ref:`provider` documents gives a more detailed description of how to implement
your own workflow provider.

The default all workflows are executed by the ``cwltool`` provider, that basically
exports the workflow to CWL and then uses `cwltool <https://github.com/common-workflow-language/cwltool>`_
to execute the given CWL.

The workflow backend can be changed by using the ``-p/--provider <PROVIDER>`` and
command line option. A backend's default configuration can be overridden by
providing the  ``-c/--config <config.yaml>`` a command line parameter.
The following ``renku`` commands support the above mentioned, workflow provider
related command line options:

 - :ref:`cli-rerun`,
 - :ref:`cli-update`,
 - :ref:`cli-workflow` execute and iterate.

For example, to execute a previously created ``my_plan`` workflow with ``toil``, one
simply would run the following command:

.. code-block:: console

   $ renku execute -p toil my_plan

Using ``toil`` as a workflow provider has the advantage that it supports running
the workflows on various `high-performance computing <https://toil.readthedocs.io/en/latest/running/hpcEnvironments.html>`_
and `cloud <https://toil.readthedocs.io/en/latest/running/cloud/cloud.html#cloud-platforms>`_
platforms.

Renku on Slurm
^^^^^^^^^^^^^^
`Slurm <https://www.schedmd.com/>`_ is a highly configurable open-source workload manager,
which is in widespread use at government laboratories, universities and companies world
wide and performs workload management for over half of the top 10 systems in the TOP500.

As ``toil`` supports Slurm, one can easily execute the previously created renku
workflows on Slurm, just need to provide a simple configuration file to the provider
(``--config``)::

  batchSystem: slurm
  disableCaching: true

The ``disableCaching`` is necessary to be enabled for Slurm, for more details see the
related ``toil`` issue `TOIL-1006 <https://ucsc-cgl.atlassian.net/browse/TOIL-1006>`_.

`Additional Slurm specific parameters <https://slurm.schedmd.com/sbatch.html>`_ can be
provided with the ``TOIL_SLURM_ARGS`` environment variable.

Taking the example above, the following command line will execute ``my_plan`` on Slurm:

.. code-block:: console

   $ TOIL_SLURM_ARGS="-A my_account --export=ALL" renku execute -p toil -c provider.yaml my_plan

where

 - ``provider.yaml`` file contains the above mentioned two parameters for ``toil``,
 - ``-A my_account`` specifies which account should be charged for the used resources,
 - ``--export=ALL`` specifies that all environment variables are propagated to the Slurm workers.
   It is often required in academic Slurm installations.
