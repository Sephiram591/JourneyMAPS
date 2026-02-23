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

The first step is to define an **environment**: a tree of parameters that are used by paths. Environments are represented by
 :class:`jmaps.journey.param.JDict`. This class acts mostly as a python dictionary 
(indexed by strings, you can even use \*\*jdict to unpack a dictionary into the JDict).
However, when you access a parameter in the JDict, it will be marked as used! By this,
JourneyMAPS can track which parameters are used by which paths, and which are not.
Path results will be saved according to the parameters that were used in the path.

 The building blocks of an environment are:
- **JDict**: Dictionary of named child parameters, typically used as the top-level environment.
- **XBuffer / YBuffer**: Functions/classes that are evaluated when accessed, or retreived from a cache of their previous evaluation. The cache can reset based on a :class:`ResetCondition`. XBuffers save only only the output of the function to the database, while YBuffers save only input.
- **InvisibleParam**: Wrapper that hides parameters from SQL exports by default.
- **Refer**: Reference to another parameter elsewhere in the environment tree.
- **JValue**: Leaf parameter that holds a concrete Python value (with an optional explicit dtype). 

Here is an example environment:

.. code-block:: python

   from datetime import datetime, timezone
   import numpy as np
   from jmaps import ResetCondition

   env = jm.JDict(data={"a":1})                 # 1 is wrapped into a JValue with dtype=None
   env["o"] = jm.JValue(2, float)               # typed scalar, will always be saved as a float to the database
   env["l"] = {"apples": 3}                     # nested dict, wrapped into a JDict
   env["r"] = jm.Refer(["l", "apples"])         # reference into the tree
   env["i"] = jm.InvisibleParam({"oranges": 4}) # Will not be saved to the database, even if it is used in a path.
   env["x"] = jm.XBuffer(                       # This function will be evaluated every time a path is run (as long as it is not a subpath)
       datetime.now,
       timezone.utc,
       reset_condition=ResetCondition.ON_RUN_IF_PARENT_PATH,
   )
   env["y"] = jm.YBuffer(np.linspace, 1, 5, jm.InvisibleParam(11)) # This function will be evaluated only once.    

   print(env["a"]) # This will print the integer 1
   print(env["r"]) # This will print the integer 3, obtained from env["l"]["apples"]
   print(env["x"]) # This will print the current time in UTC
   print(env["y"]) # This will print the linspace array [1, 2, 3, 4, 5]
   print(env["i"]["oranges"]) # This will print the integer 4
   print(env.get_sql_data(show_unused=False, show_invisible=False)) # This will return a json compatible dictionary with 'a', 'r', 'x', and 'y' keys, but not 'o', 'l', or 'i' (i is invisible)
   
   
Toying around with the show_unused and show_invisible flags can give you a better understanding of how the environment works.

Creating a Simple Path
----------------------

Next, we define a **path** by subclassing :class:`jmaps.journey.path.JPath`.
Each path implements a ``_run`` method that uses an environment and returns
 a :class:`jmaps.journey.path.PathResult`. A PathResult is a container for the results of a path execution. 
It can contain both SQL (result.sql: dict|None) and file (result.file: dict|None) results.

Below, ``PowerPath`` simply squares the parameter ``o`` and stores it in the SQL
result dictionary:

.. code-block:: python

   class PowerPath(jm.JPath):
      name: str = "power"
      changelog: str | None = 'Takes the square of "o"'
      save_datetime: bool = False # Whether to save the completion time of the path in the database. This means no runs will be loaded from the database, because they each have a unique completion time.
      def _run(self, env: jm.JDict, subpath_results, verbose: bool = False):
         return jm.PathResult(sql={"result": env["o"] ** 2}, file=None)
   
A path may require the results of other paths. This is done by listing the subpaths 
in the subpaths attribute of the parent path. If multiple runs of the same subpath 
are necessary, or a custom environment distinct from the parent path is necessary, 
the batched_subpaths attribute can be used to specify which subpaths are batched, 
and the get_batch method must be overridden to define the jm.JBatch of environments 
for the subpath. A subpath without batches will be run with the same environment as the parent path.

Below, ``PowerSweepPath`` sweeps the parameter ``o`` from 1 to 10 and stores the results of the ``PowerPath`` as a file. 
:mod:`jmaps.io.jpickle` is used to pickle the results of the ``PowerPath`` to a file (see "Using the IO Registry" in the tutorial).

.. code-block:: python

   from jmaps.io import jpickle
   class PowerSweepPath(jm.JPath):
      name:str='power_sweep'
      changelog:str|None = 'Sweeps o from 1 to 10'
      subpaths:list[str] = ['power'] # This path requires the result of the power path.
      batched_subpaths:set[str] = set(['power']) # This path will execute the power path 11 times, with o = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11].
      def _run(self, env: jm.JDict, subpath_results, verbose: bool = False):
         # subpath_results is a dictionary of the results of the subpaths.
         # Formatted as {subpath_name: PathResult} for non-batched subpaths, 
         # and          {subpath_name: {batch_id: PathResult}} for batched subpaths.
         xs = np.array([float(x) for x in subpath_results['power'].keys()])
         ys = np.array([float(y.sql['result']) for y in subpath_results['power'].values()])
         return jm.PathResult(
               file={"xs":xs, "ys":ys},
               sql=None
         )
      def get_batch(self, subpath_name, env: jm.JDict, previous_subpath_results: dict[str, Any]): # This method is used to define the batch of environments for power path.
         batch = None # If no batch is needed, return None.
         match subpath_name:
            case 'power':
               batch = jm.JBatch()
               for p in np.linspace(1, 10, 11):
                  batch.add_run(str(p), {'o':p})
         return batch


Constructing and Inspecting a Journey
-------------------------------------

To execute paths and cache their results, we build a :class:`jmaps.journey.journey.Journey`
object. In practice you will point ``db_engine_arg`` at your own Postgres
database; here we use a placeholder connection string:

.. code-block:: python

   engine = "postgresql+psycopg://USERNAME:PASSWORD@HOST:5432/DBNAME"
   journey = jm.Journey(db_engine_arg=engine, paths=[PowerPath()], result_directory=None)
   journey.update_path(PowerSweepPath())

   print(journey)
   # Journey(Test)
   # Paths:
   #    power
   #    power_sweep

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

   # First run: compute and cache. Note that the power path is run 11 times, once for each value of o. 
   # Each individual run of the power path is retrieved from the cache because the force_run_to_depth is 1. If force_run_to_depth was 2, the power path would be re-computed 11 times for each value of o.
   result, subpath_results = journey.run(env, "power_sweep", force_1)
   print(result.file)         # {"xs": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], "ys": [1, 4, 9, 16, 25, 36, 49, 64, 81, 100, 121]}

   # Second run: load from cache (no recomputation)
   result, subpath_results = journey.run(env, "power_sweep", force_0)
   print(result.file)         # {"xs": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], "ys": [1, 4, 9, 16, 25, 36, 49, 64, 81, 100, 121]}

Why is ``force_run_to_depth=1`` necessary? If you change the structure of ``_run`` or ``get_batch`` for a path,
the results of the path will be different than the cached results. This is because the schema of the path 
has changed, and the cached results will not be compatible with the new schema. Schemas are recomputed every
time a path is run without loading from the database. Thus, if you change the structure of ``_run`` or ``get_batch`` for a path,
you must set ``force_run_to_depth=1`` to ensure that the results of the path are re-computed rather than loaded and the schema is updated.

Note that if you run a path with a subpath that has changed, you will want to set force_run_to_depth=2 to ensure that the subpath is re-computed and the schema is updated.
And if that subpath has a subpath that has changed, you will want to set force_run_to_depth=3 to ensure that the subpath is re-computed and the schema is updated. 
You get the idea.

When a new schema is detected, a PathVersion will be created in the database with the new schema. The changelog
at the moment of creation will be saved as the changelog for the path version, allowing you to track changes to the path.
Note that the changelog is saved for the first PathVersion as well.


Behind the scenes, ``Journey`` uses SQLAlchemy models (:mod:`jmaps.journey.jmalc`)
to store path definitions, versions, and results. The lower part of
``journey_test.ipynb`` shows how to inspect these tables directly with a
SQLAlchemy session.

Using the IO Registry
---------------------

When a path returns a :class:`~jmaps.journey.path.PathResult` with a non-empty
``file`` dict, the Journey saves each value to disk and records *how* it was
saved so it can be loaded later. The **IO registry** (:mod:`jmaps.io`)
maps Python types to writer and reader callables. When an object needs saved,
the registry will look up the object's type and use the registered writer to save it. 
If the type does not have a registered writer, it will loop through all parent types 
and use the registered writer for the first parent that has a registered writer. 
If no parent has a registered writer, a TypeError will be raised.

:mod:`jmaps.io` contains some default handlers for common types:  

- **Generic pickle** in :mod:`jmaps.io.jpickle`: registers ``object`` so any
  pickleable value can go in ``result.file``. Useful when you don't need
  cross-language or human-readable format.  
- **Tidy3D objects** in :mod:`jmaps.io.jtidy3d`: registers
  ``tidy3d.components.base.Tidy3dBaseModel`` and uses ``.to_file`` / ``.from_file``
  with HDF5.  

Importing the module that defines the handlers will register the handlers with the registry.

You can register your own types with the decorators :func:`~jmaps.io.writable` and :func:`~jmaps.io.readable`.
The writer function must accept ``(obj, file_path)``; the reader must accept ``(root_cls, file_path)`` and return the
deserialized object. root_cls is the fully qualified name of the class that was saved, in case the true class is a subclass of the writer class.

Example: NumPy arrays as `.npy` files (using the decorators, which the registry uses for lookup):

.. code-block:: python

   import numpy as np
   from pathlib import Path
   from jmaps.journey.io import writable, readable

   @writable(np.ndarray)
   def _write_ndarray(obj: np.ndarray, file_path: Path):
       np.save(file_path.with_suffix(".npy"), obj)

   @readable(np.ndarray)
   def _read_ndarray(root_cls: str, file_path: Path):
       return np.load(file_path.with_suffix(".npy"))

After writing this, import the module/run the cell that defines the handlers. Now you can use
``result.file["key"] = np.array([1, 2, 3])`` in a path and it will be
saved/loaded automatically!

