Contributing
============

Contributions are welcome, and they are greatly appreciated! Every
little bit helps, and credit will always be given.

Types of Contributions
----------------------

Report issues / contacting developers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Report bugs on our issue tracker_.

If you want to submit a bug, improvement or feature suggestions feel free to open a
corresponding issue on GitHub.

If you are reporting a bug, please help us to speed up the diagnosing a problem
by providing us with as much as information as possible.
Ideally, that would include a step by step process on how to reproduce the bug.

If you have a general usage question please ask it on our discourse forum_ or our chat_.

.. _chat: https://gitter.im/SwissDataScienceCenter/renku
.. _forum: https://renku.discourse.group/
.. _tracker: https://github.com/SwissDataScienceCenter/renku-python/issues

Fix Bugs
~~~~~~~~

Look through the GitHub issues for bugs. Anything tagged with "bug"
is open to whoever wants to implement it.

Implement Features
~~~~~~~~~~~~~~~~~~

Look through the GitHub issues for proposal discussions or epics and feel free to
express your proposal on the topic. Once topic has been flushed out and we have
decided how feature should be implemented, we can start implementing them.


Improvement requests
~~~~~~~~~~~~~~~~~~~~

If you see room for improvement, please open an issue with a suggestion.
Please motivate your suggestion by illustrating a problem it solves.

Write Documentation
~~~~~~~~~~~~~~~~~~~

Renku could always use more documentation, whether as part of the
official Renku docs, in docstrings, or even on the web in blog posts,
articles, and such.

Submit Feedback
~~~~~~~~~~~~~~~

The best way to send feedback is to file an issue at
https://github.com/SwissDataScienceCenter/renku-python/issues.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

Get Started!
------------

Ready to contribute? Here's how to set up `renku` for local development.

1. Fork the `SwissDataScienceCenter/renku-python` repo on GitHub.
2. Clone your fork locally:

   .. code-block:: console

      $ git clone git@github.com:your_name_here/renku.git

3. Ensure you have your development environment set up. For this we
encourage usage of `pipenv` and `pyenv`:

   .. code-block:: console

      $ pyenv install 3.7.5rc1
      $ cd renku/
      $ pipenv install --python  ~/.pyenv/versions/3.7.5rc1/bin/python
      $ pipenv shell

4. Create a branch for local development:

   .. code-block:: console

      $ git checkout -b <issue_number>_<short_description>

   Now you can make your changes locally.

5. When you're done making changes, check that your changes pass tests:

   .. code-block:: console

      $ pipenv run tests

   The tests will provide you with test coverage and also check PEP8
   (code style), PEP257 (documentation), flake8 as well as build the Sphinx
   documentation and run doctests.

   Before you submit a pull request, please reformat the code using black_.

   .. code-block:: console

      $ black .

   You may want to set up black_ styling as a pre-commit hook to do this
   automatically. Install pre-commit_ and run the following command in the
   root of the `renku-python` repository:

   .. code-block:: console

      $ pre-commit install

   See https://github.com/psf/black#version-control-integration for more
   information. Make sure to remove other formatting hooks (e.g. yapf_) if you
   already have them set up.

   .. _pre-commit: https://pre-commit.com/
   .. _black: https://github.com/psf/black/
   .. _yapf: https://github.com/google/yapf/

6. Commit your changes and push your branch to GitHub:

   .. code-block:: console

      $ git add .
      $ git commit -s
          -m "type(scope): title without verbs"
          -m "* NEW Adds your new feature."
          -m "* FIX Fixes an existing issue."
          -m "* BETTER Improves and existing feature."
          -m "* Changes something that should not be visible in release notes."
      $ git push origin name-of-your-bugfix-or-feature

7. Submit a pull request through the GitHub website.


Code comment guidelines
-----------------------

To make comments more easily readable and allow automated styling/parsing in editors, all code comments
should start with one of these tags:

* :code:`TODO`: An informal task.
* :code:`FIXME`: Same as :code:`TODO`, but for bugs.
* :code:`NOTE`: Something to pay attention to regarding the code.
* :code:`INFO`: Explanation of what a section of code does (For more complicated pieces of code).
* :code:`WARNING`: Similar to :code:`NOTE` but higher severity (where disregarding can cause errors).
* :code:`HACK`: Ugly code that is needed for things to work.

So a comment should look like:

   .. code-block:: python

      # TODO: some message


Commit message guidelines
-------------------------

This project is using conventional_ commits style for generation of changelog upon
each release. Therefore, it's important that our commit messages convey what they
do correctly. Commit message should always follow this pattern:

.. _conventional: https://www.conventionalcommits.org/en/v1.0.0/

   $ %{type}(%{scope}): %{description}

**Type's used for describing commit's which will end up in changelog are** :code:`fix:` & :code:`feat:`.

:code:`description` should start with a lowercase letter.

Please note that the :code:`fix` type here is only for user-facing bug fixes and not fixes on tests or CI.
For those, please use: :code:`ci:` or :code:`test:`

Full list of types which are in use:
  * :code:`feat:` - Used for new user-facing features. This should be related to one of the predefined scopes. If a scope does not exist, a new scope may be proposed.
  * :code:`fix:` - Used for fixing user-facing bugs. This should be related to one of the predefined scopes.
  * :code:`chore:` - Used for changes which are not user-facing. The scope should be a module name in which chore occurred.
  * :code:`tests:` - Used for fixing existing or adding new tests. The scope should relate to a predefined scope or be omitted.
  * :code:`docs:` - Used for adding more documentation. If documentation is not related to predefined user scopes, it can be omitted.
  * :code:`refactor` - Used for changing the code structure. Scope being used here should be module name. If refactoring is across multiple modules, scope could be omitted or PR broken down into smaller chunks.

Full list of user-facing scopes which are in use:
  * :code:`graph` - Scope for describing knowledge graph which is being build with users usage of the system.
  * :code:`workflow` - Scope for describing reproducibility flow.
  * :code:`dataset` - Scope for describing datasets.
  * :code:`core` - General scope for describing all core elements of Renku project.
  * :code:`service` - General scope for describing interaction or operation of Renku as a service.
  * :code:`cli` - General scope for describing interaction through CLI.
  * :code:`api` - General scope for describing Renku API.


**PLEASE NOTE:** Types which are defined to be picked up for change log (:code:`feat:` and :code:`fix:`) should always contain
a scope due to grouping which occurs in changelog when we have them.


Pull Request Guidelines
-----------------------

Before you submit a pull request, check that it meets these guidelines:

* Make sure you agree with the license and follow the legal_ matter.
* The pull request should include tests and must not decrease test coverage.
* If the pull request adds functionality, the docs should be updated. Put your new functionality into a function with a docstring.
* The pull request should work for Python 3.6, 3.7 and 3.8. Check GitHub action builds and make sure that the tests pass for all supported Python versions.

.. _legal: (https://github.com/SwissDataScienceCenter/documentation/wiki/Legal-matter)
