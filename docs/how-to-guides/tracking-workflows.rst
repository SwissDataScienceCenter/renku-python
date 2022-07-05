.. _tracking-workflows:

Tracking Workflows with Renku CLI
=================================

One of the main uses of Renku is that it lets you track commands that you
execute on the command line, rerun them, compose them into bigger pipelines and
inspect how files were generated in your project.

For any command you would usually run on the command line, you can just pass
that command through Renku (by prepending ``renku run``) when running it to track the execution.

For instance, if we had a ``script.py`` that reads a file, appends text to the
content and writes it out to a different file, like

.. code-block:: python

   import sys

   input_path = sys.argv[1]
   output_path = sys.argv[2]
   append_text = sys.argv[3]

   with open(input_path, "r") as input_file, open(output_path, "w") as output_file:
       text = input_file.read() + append_text
       output_file.write(text)

that you normally call like

.. code-block:: console

   $ python script.py data.csv output.txt "my text"

You can just call it with Renku like

.. code-block:: console

   $ renku run -- python script.py data.csv output.txt "my text"

This would

- Track this execution of the command, detecting any files it used as input and
  generated as output, as well as the text parameter ``"my text"``
- Add the recorded execution in the overall directed acyclic graph (DAG) that
  links together workflow executions
- Create a Plan entity, which serves as a recipe for the command you just
  executed, allowing you to execute it with different input, output or parameter values
- Allow you to detect out of date outputs should the input change in the future

You can see that all your workflow outputs are up to date using

.. code-block:: console

   $ renku status
   Everything is up-to-date.

Right now, everything is fine since we didn't make any changes. But if we modify
``data.csv``, we would get

.. code-block:: console

   $ renku status Outdated outputs(1):
     (use `renku workflow visualize [<file>...]` to see the full lineage) (use
     `renku update --all` to generate the file from its latest inputs)

           output.txt: data.csv

   Modified inputs(1):

           data.csv

This tells us that ``data.csv`` was changed and as a result ``output.txt`` is
out of date and should be updated.

We can do so using

.. code-block:: console

   $ renku update output.txt
   Resolved '../../../../../tmp/tmp9wtjmp5_' to
   'file:///tmp/tmp9wtjmp5_' [job 1f2c73c4-01d9-40cc-b351-b13e48c51577]
   /tmp/xkjzau4m$ python \
       /tmp/xkjzau4m/script.py \ /tmp/xkjzau4m/data.csv \ output.txt "my text"
   [job 1f2c73c4-01d9-40cc-b351-b13e48c51577] completed success Moving outputs
   [                                    ]  1/1

This runs the command we recorded earlier, with the new input data, to create
``output.txt`` again. Renku is smart enough to only run those parts of the DAG
that changed and need to be updated.

Manual specification of inputs and outputs
------------------------------------------

Sometimes there are cases where the automated detection of
inputs/outputs/parameters doesn't work or is not sufficient.

Lets say our ``script.py`` looked instead like:

.. code-block:: python

   with open("data.csv", "r") as input_file, open("output.txt", "w") as output_file:
       text = input_file.read() + "my text"
       output_file.write()

Renku doesn't know that you script reads ``data.csv`` as an input, because it
does not show up on the command line. Though it would still detect
``output.txt`` as an output since it monitors files on disk for changes.

You could let renku know manually that this is the case by running

.. code-block:: console

   $ renku run --input data.csv --output output.txt -- python script.py

This would let Renku know that this script has one input ``data.csv``  along
with one output ``output.txt``.

Renku will automatically generate names for inputs, outputs and parameters on
the created Plan, so they can be used in other Renku commands such as ``renku
workflow execute``. You can also specify the names directly to have more human
readable names, by prepending the name like:

.. code-block:: console

   $ renku run --input data_file=data.csv --output result=output.txt -- python script.py

This would set the name for the input file to ``data_file`` and the name for the
output file to ``result``.

Similarily, if you had a command ``python script.py example`` and there is a
file named ``example`` on disk, renku would detect it as an input. But if this
was just a coincidence and ``example`` was actually a string input unrelated to
the file, you could run ``renku run --parameter my_param="example" -- python
script.py example`` to let renku know that ``example`` is a parameter, not an
input file.

Alternatively, you can also specify this information in a YAML file, which is
nicer in cases where there are many inputs or you want to specify inputs
programmatically.

In this case, the file would look like

.. code-block:: yaml

   data_file: data.csv

and should be stored as ``.renku/tmp/inputs.yml``. along with

.. code-block:: yaml

   result: output.txt

stored as ``.renku/tmp/outputs.yml``.

Then running the command normally will pick this up and add it to the workflow
metadata, so it just becomes:

.. code-block:: console

   $ renku run  -- python script.py

Note that while this allows renku to track ``data.csv`` as an input, it does not
allow you to specify a different path for the input later on, as the path is
hard-coded in your code.

The same can be done with ``.renku/tmp/parameters.yml`` for parameters.

A third option if you are working with Python is to make use of the Renku
Python API. This lets you specify inputs/outputs/parameters directly in code. Our script
would the look something like this:

.. code-block:: python

   from renku.api import Input, Output, Parameter

   with open(Input("data_file", "data.csv"), "r") as input_file, open(Output("result", "output.txt"), "w") as output_file:
       text = input_file.read() + Parameter("append_text", "my text").value
       output_file.write()

and run it like

.. code-block:: console

   $ renku run  -- python script.py

This achieves the same as in the examples above, specifying that ``data.csv`` is
an input, ``output.txt`` is an output and ``example`` is a parameter. It names
the references on the created Plan ``data_file``, ``result`` and ``append_text``
respectively. The big benefit of this approach is that it does allow changing
the values used when executing the created workflow again, e.g. using ``renku
workflow execute``. Then for instance the ``Input(...)`` part could return the
modified value instead of the hard-coded ``data.csv``.


If you do not want renku to try and automatically detect inputs or outputs, you
can use the ``--no-input-detection`` or ``--no-output-detection`` flags to
``renku run``, respectively. You can also let Renku know that a workflow does
not produce an output file with the ``--no-output`` flag.
