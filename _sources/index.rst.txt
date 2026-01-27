.. 4D4L documentation master file

=====================================
ware_ops_pipes
=====================================

.. image:: https://img.shields.io/badge/python-3.11+-blue.svg
   :target: https://www.python.org/downloads/
   :alt: Python Version

.. image:: https://img.shields.io/badge/license-MIT-green.svg
   :alt: License

**ware_ops_pipes** is a library for automated and context aware pipeline synthesis of warehouse operations problems.
Together with `ware_ops_algos`_, ``ware_ops_pipes`` forms the meta-model framework **Data Driven Decisions for Logistics (3D4L)**.

.. _ware_ops_algos: https://github.com/kit-dsm/ware_ops_algos


Framework Overview
============


The 3D4L framework consists of four main components:

🏭 **Data Layer and Domain Objects** (``ware_ops_algos``)
   To deal with heterogenous data sources, warehouse information is organized into domain objects: Layout, Articles, Orders, Resources, and Storage.

🛠️ **Algorithm Repository** (``ware_ops_algos``)
   Modular implementations of algorithms for item assignment, batching, routing, and scheduling. Each algorithm is annotated with its requirements via algorithm cards.

⚙️ **Domain-Algorithm Mapping** (``ware_ops_algos``)
   Filtering mechanism that identifies applicable algorithms based on instance characteristics and algorithm requirements.

🔄 **Context-aware Pipelines** (``ware_ops_pipes``)
   Uses `CLS-Luigi`_ to automatically generate all feasible algorithm combinations as directed acyclic graphs.

.. _CLS-Luigi: https://github.com/cls-python/cls-luigi

.. figure:: _static/3d4l_framework_v5.png
   :alt: 3D4L Architecture
   :width: 100%
   :align: center

   Architecture of the 3D4L framework.


Quick Start
=======================

.. grid:: 1

   .. grid-item-card:: 📘 Getting Started
      :link: ./examples/getting_started
      :link-type: doc

      Start here with a basic introduction to 3D4L.


.. grid:: 2

   .. grid-item-card:: 📚 API Documentation
      :link: autoapi/index
      :link-type: doc

      Detailed API reference for all modules and classes.

   .. grid-item-card:: 💡 Examples
      :link: ./examples
      :link-type: doc

      Examples and benchmark evaluations.


Citation
========

If you use ware_ops_algos in your research, please cite:

.. code-block:: bibtex

   @misc{bischoff2026ware_ops_pipes,
    author = {Bischoff, Janik and Suba, Oezge Nur and Barlang, Maximilian and Kutabi, Hadi and Mohring, Uta and Dunke, Fabian and Meyer, Anne and Nickel, Stefan and Furmans, Kai},
    title = {ware_ops_pipes},
    year = {2026},
    publisher = {GitHub},
    journal = {GitHub Repository},
    howpublished = {\url{https://github.com/kit-dsm/ware_ops_pipes.git}},
}


Indices and Tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


Support
=======

* 📧 Email: janik.bischoff@kit.edu