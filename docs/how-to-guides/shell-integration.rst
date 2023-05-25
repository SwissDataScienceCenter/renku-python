.. _shell-integration:

Shell integration of Renku CLI
==============================

Renku CLI supports shell auto-completion for Renku commands and their arguments like datasets and workflows.

A convenience method is available for printing to the standard output the shell completion command for the
currently used shell:

.. code-block:: console

        $ renku env --shell-completion
        _RENKU_COMPLETE=zsh_source renku

To activate tab completion for your supported shell run the following command after installing Renku CLI:

.. tabs::

  .. tab:: bash

    .. code-block:: console

      $ eval "$(_RENKU_COMPLETE=bash_source renku)"

  .. tab:: fish

    .. code-block:: console

      $ eval (env _RENKU_COMPLETE=fish_source renku)

  .. tab:: zsh

    .. code-block:: console

      $ eval "$(_RENKU_COMPLETE=zsh_source renku)"

You can put the same command in your shell's startup script to enable completion by default.
After this not only sub-commands of ``renku`` will be auto-completed using tab, but for example
in case of ``renku workflow execute`` the available ``Plans`` are going to be listed.

.. code-block:: console

        $ renku workflow execute run<TAB>
        run1   run10  run11  run12  run13  run14  run2   run3   run4   run7   run8

.. note::

   Tab completion of available ``Plans`` (or ``Datasets``) only works if the user is executing the command
   within a Renku project.


For more information on how to set up shell auto-completion, see documentation for the Click library,
which used under the hood by Renku CLI:
`shell completion <https://click.palletsprojects.com/en/8.0.x/shell-completion/>`_
