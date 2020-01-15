Renku Repository Service Helm Chart
===================================

Provide a basic chart for deploying the Renku Repository service.application.

Configuration
-------------

- `cacheDirectory` location of the cache on disk
  (default: `/svc/cache`)
- `projectCloneDepth` git clone depth
  (default: `1`)

Usage
-----

In the `helm-chart` directory:

.. code-block:: console

    helm upgrade --install renku-core renku-core


To rebuild the images and update the chart you can run

.. code-block:: console

    pip install chartpress
    chartpress
