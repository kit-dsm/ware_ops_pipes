Getting started
===============

Install the locked environment with ``uv`` from the repository root:

.. code-block:: bash

   uv sync --frozen --extra eval

Check which algorithm configurations apply to the bundled Foodmart Data Card:

.. code-block:: bash

   uv run --frozen python examples/list_applicable_algorithms.py

After obtaining the Foodmart benchmark files, synthesize and execute three
pipelines for one instance:

.. code-block:: bash

   uv run --frozen python examples/run_one_foodmart_instance.py data/instances/FoodmartData/instances_d5_ord5_MAL.txt --max-pipelines 3

The exact routing and integrated batching-routing configurations require a
Gurobi license. The applicability example and the extension example do not
solve a Gurobi model.
