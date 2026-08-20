Working with pipelines
======================

``PipelineRunner`` performs the executable workflow:

1. Load the Data Card and Algorithm Cards.
2. Use the Problem Taxonomy to identify applicable algorithms.
3. Import the corresponding CLS-Luigi components.
4. Synthesize valid pipelines from the component repository.
5. Execute the pipelines with Luigi.
6. Rank their results for the objective declared by the Data Card.

The smallest complete entry point is
``examples/run_one_foodmart_instance.py``:

.. literalinclude:: ../../examples/run_one_foodmart_instance.py
   :language: python

For another instance format, subclass ``PipelineRunner``, implement
``discover_instances()``, select the loader class and Data Card, and call
``run_all()`` or ``run_instance()``. The runners under ``experiments/`` show
the configurations used for the seven evaluated instance sets.
