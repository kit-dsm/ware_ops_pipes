ware_ops_pipes
==============

``ware_ops_pipes`` is the pipeline-synthesis component of Context-Aware
Synthesis for Optimization Problems (CASOP). It combines compatible warehouse
algorithms from `ware_ops_algos`_ into executable pipelines using `CLS-Luigi`_.

.. _ware_ops_algos: https://github.com/kit-dsm/ware_ops_algos
.. _CLS-Luigi: https://github.com/cls-python/cls-luigi

The repository accompanies the paper *Context-Aware Synthesis of Optimization
Pipelines for Warehouse Operations*. The experiments cover seven established
benchmark families: SPRP, SPRP-SS, Bahceci--Öncan, Henn--Wäscher,
Muter--Öncan, Foodmart, and Kris. Together they represent routing, joint
batching and routing, and joint batching, scheduling, and routing problems.

Installation
------------

Python 3.11 or newer is required. The exact ``ware_ops_algos`` revision used by
this release is pinned in ``pyproject.toml`` and ``uv.lock``.

.. code-block:: bash

   git clone https://github.com/kit-dsm/ware_ops_pipes.git
   cd ware_ops_pipes
   python -m venv .venv
   source .venv/bin/activate
   pip install -e ".[eval]"

On Windows, activate the environment with
``.venv\Scripts\activate``.

Experiments
-----------

The active experiment entry points are:

* ``experiments/run_hessler_irnich.py`` for SPRP, SPRP-SS,
  Bahceci--Öncan, Henn--Wäscher, and Muter--Öncan;
* ``experiments/run_foodmart.py`` for Foodmart;
* ``experiments/run_ibrsp.py`` for Kris.

Benchmark data are not stored in the Git repository. Acquisition and directory
names are documented in ``data/instances/README.md``.

Evaluation
----------

The numbered scripts in ``experiments/evaluation`` form the paper evaluation
workflow. Script ``01`` combines raw experiment outputs into the canonical
Parquet summary. Scripts ``02`` and ``03`` prepare and compare the literature
reference values. Scripts ``04`` to ``06`` generate the paper tables, runtime
table, and appendix figure. See ``experiments/evaluation/README.md`` for the
commands and release data.

License and citation
--------------------

The software is released under the BSD 3-Clause License. Third-party benchmark
data remain subject to their original terms. Citation metadata are provided in
``CITATION.cff``.
