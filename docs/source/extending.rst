Extending the pipeline repository
=================================

An algorithm implementation belongs in ``ware_ops_algos`` under the common
algorithm interface. Its Algorithm Card records the subproblem, objective,
domain requirements, and configuration parameters.

To make the implementation available to pipeline synthesis, add a CLS-Luigi
component that subclasses the corresponding abstract component from
``pipelines/templates/template_1.py``. The component loads its required domain
objects and returns an initialized algorithm from ``get_inited_*()``. Importing
the concrete component registers it with CLS-Luigi.

The following runnable example defines a batching algorithm, wraps it as a
``MultiOrderBatching`` component, and verifies its registration:

.. code-block:: bash

   uv run --frozen python examples/extend_with_custom_batching.py

.. literalinclude:: ../../examples/extend_with_custom_batching.py
   :language: python

For a permanent component, place the wrapper under the matching
``pipelines/subproblems/`` package and connect its Algorithm Card name to the
component module in ``PipelineRunner``. Generated wrappers for configured
batching algorithms are under ``pipelines/subproblems/batching/generated/``.
