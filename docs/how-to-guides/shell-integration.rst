.. _shell-integration:

Shell integration of Renku CLI
==============================

Renku CLI is using for `click <https://click.palletsprojects.com>`_ library for defining
all the Renku commands.

Click provides tab completion support for Bash (version 4.4 and up), Zsh, and Fish. For
detailed information about how to setup the tab completion for these shells, please
read the `shell completion <https://click.palletsprojects.com/en/8.0.x/shell-completion/>`_
documentation of click.

For example to activate tab completion for Zsh run the following command after installing
Renku CLI:

.. code-block:: console

   $ eval "$(_RENKU_COMPLETE=zsh_source renku)"


After this not only sub-commands of `renku` will be auto-completed using tab, but for example
in case of `renku workflow execute` the available ``Plans`` are going to be listed.

.. code-block:: console

   $ renku workflow execute run
   run1   run10  run11  run12  run13  run14  run2   run3   run4   run7   run8

.. note::
   Tab completion of available ``Plans`` only works if the user is executing the command
   within a Renku project.
