.. _tracking-workflows:

Tracking Workflows with Renku CLI
=================================

One of the main uses of Renku is that it lets you track commands that you
execute on the command line, rerun them, compose them into bigger pipelines and
inspect how files were generated in your project.

For any command you would usually run on the command line, you can just pass
that command through Renku when running it to track the execution.

For instance, if we had a ``script.py`` like

.. code-block:: python

   import sys

   input_path = sys.argv[1] output_path = sys.argv[2]

   with open(input_path, "r") as input_file, open(output_path, "w") as
   output_file:
       output_file.write(input_file.read())

that you normally call like

.. code-block:: console

   $ python script.py data.csv output.txt

You can just call it with Renku like

.. code-block:: console

   $ renku run -- python script.py data.csv output.txt

This would

- Track this execution of the command, detecting any files it used as input and
  generated as output
- Add the recorded execution in the overall directed acyclic graph (DAG) that
  links together executions
- Create a Plan entity, which serves as a recipe for the command you just
  executed, allowing you to execute it with different input or output values
- Allow you to detect out of date outputs should the input change in the future

You can see that all your workflow outputs are up to date using

.. code-block:: console

   $ renku status Everything is up-to-date.

Right now, everything is fine since we didn't make any changes. But if we modify
for instance data.csv, we would get

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

   $ renku update output.txt Resolved '../../../../../tmp/tmp9wtjmp5_' to
   'file:///tmp/tmp9wtjmp5_' [job 1f2c73c4-01d9-40cc-b351-b13e48c51577]
   /tmp/xkjzau4m$ python \
       /tmp/xkjzau4m/script.py \ /tmp/xkjzau4m/data.csv \ output.txt
   [job 1f2c73c4-01d9-40cc-b351-b13e48c51577] completed success Moving outputs
   [                                    ]  1/1

This runs the command we recorded earlier again, with the new input data, to
create ``output.txt`` again. Renku is smart enough to only run those parts of
the DAG that changed and need to be updated.
