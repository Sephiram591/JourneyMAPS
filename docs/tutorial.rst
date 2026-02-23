Tutorial: From Parameters to Journeys
=====================================

This tutorial walks through the core pieces of a small but realistic workflow,
mirroring the example notebooks in ``src/jmaps/dev``. It assumes you have
completed the :doc:`installation` steps and have ``jmaps`` importable.

All examples use the recommended import:

.. code-block:: python

   import jmaps as jm

Defining an Environment with ``JDict``
--------------------------------------

The first step is to define an **environment**: a tree of parameters describing
how a path (or set of paths) should run. Environments are represented by
 :class:`jmaps.journey.param.JDict`.

.. code-block:: python

   from datetime import datetime, timezone
   import numpy as np
   from jmaps import ResetCondition

   env = jm.JDict(data={"a": jm.JValue(1)})
   env["o"] = jm.JValue(2, float)              # typed scalar
   env["l"] = {"apples": 3}                    # nested dict, auto-wrapped
   env["r"] = jm.Refer(["l", "apples"])        # reference into the tree
   env["i"] = jm.InvisibleParam({"oranges": 4})
   env["x"] = jm.XBuffer(
       datetime.now,
       timezone.utc,
       reset_condition=ResetCondition.ON_RUN_IF_PARENT_PATH,
   )
   env["y"] = jm.YBuffer(np.linspace, 1, 5, jm.InvisibleParam(11))

The ``param_test.ipynb`` notebook in ``src/jmaps/dev`` contains additional
experiments showing how usage tracking and SQL export work via
``env.get_sql_data(show_unused=..., show_invisible=...)``.

Creating a Simple Path
----------------------

Next, we define a **path** by subclassing :class:`jmaps.journey.path.JPath`.
Each path implements a ``_run`` method that consumes an environment and returns
 a :class:`jmaps.journey.path.PathResult`.

Below, ``PowerPath`` simply squares the parameter ``o`` and stores it in the SQL
result dictionary:

.. code-block:: python

   class PowerPath(jm.JPath):
       name: str = "power"
       changelog: str | None = 'Takes the square of "o"'

       def _run(self, env: jm.JDict, subpath_results, verbose: bool = False):
           return jm.PathResult(sql={"result": env["o"] ** 2}, file=None)

This corresponds to the ``PowerPath`` example in ``journey_test.ipynb``.

Constructing and Inspecting a Journey
-------------------------------------

To execute paths and cache their results, we build a :class:`jmaps.journey.journey.Journey`
object. In practice you will point ``db_engine_arg`` at your own Postgres
database; here we use a placeholder connection string:

.. code-block:: python

   engine = "postgresql+psycopg://USERNAME:PASSWORD@HOST:5432/DBNAME"
   journey = jm.Journey("Test", db_engine_arg=engine, cache_db_meta=True)
   journey.update_path(PowerPath())

   print(journey)
   # Journey(Test)
   # Paths:
   #    power

Running Paths and Using ``PathOptions``
---------------------------------------

The :class:`jmaps.journey.journey.PathOptions` object controls how paths are
executed and whether cached results are reused:

.. code-block:: python

   force_0 = jm.PathOptions(force_run_to_depth=0, verbose=True)
   force_1 = jm.PathOptions(force_run_to_depth=1, verbose=True)

   # First run: compute and cache
   result, subpath_results = journey.run(env, "power", force_1)
   print(result.sql)          # {"result": 4}

   # Second run: load from cache (no recomputation)
   result, subpath_results = journey.run(env, "power", force_0)
   print(result.sql)          # {"result": 4}

Behind the scenes, ``Journey`` uses SQLAlchemy models (:mod:`jmaps.journey.jmalc`)
to store path definitions, versions, and results. The lower part of
``journey_test.ipynb`` shows how to inspect these tables directly with a
SQLAlchemy session.

Where to Go Next
----------------

- Explore ``param_test.ipynb`` to see more advanced parameter-tree behaviors
  (usage tracking, invisible parameters, and SQL export options).
- Explore ``alchemy.ipynb`` for an illustrative SQLAlchemy schema that
  motivated the higher-level abstractions in :mod:`jmaps.journey.jmalc`.
- Use the patterns above to build your own environments and paths, then register
  them on a ``Journey`` and scale up to more complex, batched workflows.

