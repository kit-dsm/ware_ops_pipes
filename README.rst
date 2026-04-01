ware_ops_pipes
==============

What is it?
-----------
This repository contains the source code to reproduce the results presented in the paper
"Warehouse-Aware Design of Algorithmic Pipelines for Decision-Making in Warehouse Operations".

Together with `ware_ops_algos`_, ``ware_ops_pipes`` forms the meta-model framework **Context-Aware Synthesis
for Optimization Problems (CASOP)**.

.. _ware_ops_algos: https://github.com/kit-dsm/ware_ops_algos

CASOP enables:

1. **Semantic representation of warehouse systems**  (layout, orders, resources, storage)
2. **Context-specific algorithm selection** by matching algorithm requirements against warehouse features
3. **Automated pipeline synthesis** across decision stages (item assignment, batching, routing, scheduling)


Framework Overview
------------------

CASOP consists of four main components:

🏭 **Data Layer and Domain Objects** (``ware_ops_algos``)
   To deal with heterogenous data sources, warehouse information is organized into domain objects: Layout, Articles, Orders, Resources, and Storage.

🛠️ **Algorithm Repository** (``ware_ops_algos``)
   Modular implementations of algorithms for item assignment, batching, routing, and scheduling. Each algorithm is annotated with its requirements via algorithm cards.

⚙️ **Domain-Algorithm Mapping** (``ware_ops_algos``)
   Filtering mechanism that identifies applicable algorithms based on instance characteristics and algorithm requirements.

🔄 **Context-aware Pipelines** (``ware_ops_pipes``)
   Uses `CLS-Luigi`_ to automatically generate all feasible algorithm combinations as directed acyclic graphs.
.. _CLS-Luigi: https://github.com/cls-python/cls-luigi

.. figure:: docs/source/_static/3d4l_framework_v5.png
   :alt: CASOP Architecture
   :width: 100%
   :align: center

   Architecture of the CASOP framework.

Supported Problem Classes
-------------------------
- **SPRP** - Single Picker Routing Problem
- **OBRP** - Order Batching and Routing Problem
- **OBSRP** - Order Batching, Scheduling, and Routing Problem


Getting Started
---------------

Clone repositories
~~~~~~~~~~~~~~~~~~

First, clone both required repositories:

.. code-block:: bash

   cd Documents/projects
   git clone https://github.com/kit-dsm/ware_ops_algos.git
   git clone https://github.com/kit-dsm/ware_ops_pipes.git

Both repositories should be in the same parent directory for easy installation.

Create virtual environment
~~~~~~~~~~~~~~~~~~~~~~~~~~

Create and activate a virtual environment for ware_pipe:

.. code-block:: bash

   cd ware_ops_pipes
   python -m venv .venv
   source .venv/bin/activate  # On Linux/Mac
   # or
   .venv\Scripts\activate  # On Windows

Install dependencies
~~~~~~~~~~~~~~~~~~~~

With the ware_pipe virtual environment activated, install ware_ops_algos first:

.. code-block:: bash

   pip install -e ../ware_ops_algos

Then install ware_ops_pipes:

.. code-block:: bash

   pip install -e .

The ``-e`` flag installs both packages in editable mode, allowing you to make changes without reinstalling.


Running Experiments
-------------------

Experiment scripts to reproduce the results in
"Warehouse-Aware Design of Algorithmic Pipelines for Decision-Making in Warehouse Operations" are located in the ``experiments/`` folder:

.. code-block:: text

   experiments/
   ├── evaluation/
   │   ├── plots/
   │   ├── df_results.pkl
   │   ├── result_evaluation.ipynb
   │   └── runtimes_evaluation.py
   └── output/
       ├── run_foodmart.py
       ├── run_foodmart.sh
       ├── run_hessler_irnich.py
       ├── run_hessler_irnich.sh
       ├── run_iopvrp.py
       └── run_iopvrp.sh

**Running an experiment:**

.. code-block:: bash

   # Run directly with Python
   python experiments/output/run_foodmart.py

   # Or use the shell script
   bash experiments/output/run_foodmart.sh

**Evaluating results:**

Experiment results are saved to ``experiments/output/``.
Use the Jupyter notebook in ``experiments/evaluation/result_evaluation.ipynb`` to analyze.
We provide a pickled dataframe of the results from the paper which are stored in ``experiments/evaluation/df_results.pkl``.

Citation
========

If you use ware_ops_pipes in your research, please cite:

.. code-block:: bibtex

   @misc{bischoff2026ware_ops_pipes,
    author = {Bischoff, Janik and Suba, Oezge Nur and Barlang, Maximilian and Kutabi, Hadi and Mohring, Uta and Dunke, Fabian and Meyer, Anne and Nickel, Stefan and Furmans, Kai},
    title = {ware_ops_pipes},
    year = {2026},
    publisher = {GitHub},
    journal = {GitHub Repository},
    howpublished = {\url{https://github.com/kit-dsm/ware_ops_pipes.git}},
}

Authors
-------

- Janik Bischoff (IMI, Karlsruhe Institute of Technology)
- Özge Nur Subas (IOR, Karlsruhe Institute of Technology)
- Maximilian Barlang (IFL, Karlsruhe Institute of Technology)
- Hadi Kutabi (IMI, Karlsruhe Institute of Technology)
- Uta Mohring (University of Zurich)
- Fabian Dunke (IMI, Karlsruhe Institute of Technology)
- Anne Meyer (IMI, Karlsruhe Institute of Technology)
- Stefan Nickel (IOR, Karlsruhe Institute of Technology)
- Kai Furmans (IFL, Karlsruhe Institute of Technology)