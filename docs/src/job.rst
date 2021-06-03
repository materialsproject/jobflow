Job API
=======

The main interface for constructing jobs is the :obj:`job` function.

:obj:`job` wraps functions for use in Flow objects. It can be used as a decorator, or
around function calls directly (i.e. ``job(foo)(a, b, c)``). Outputs from functions
wrapped in ``job`` are proxy objects of type :obj:`Job` that contain the inputs
and function to be executed.

.. currentmodule:: jobflow.core.job

.. autofunction:: job
   :noindex:
