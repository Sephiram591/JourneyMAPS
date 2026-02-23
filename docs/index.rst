JourneyMAPS Documentation
=========================

Journey - A Multistep Automated Parameter Search Library
--------------------------------------------------------
A single journey can be life changing. However, tens or hundreds of them get overwhelming pretty quickly.  
This package is meant to generalize the pipeline for the completion of any parameter search (or Journey),  
to make embarking on and completing many Journeys feasible.

JourneyMAPS (Journey - A Multistep Automated Parameter Search Package) is a Python package designed for automated parameter optimization 
and simulation workflows. It is used most effectively as an ideologically consistent framework for you to build jupyter-notebooks for 
any experiment, simulation, or optimization problem. Thus, regardless of whether you are running photonics simulations, quantum experiments,
machine learning, or any other type of problem, your notebooks will have an elegant format for legibility and reproducibility. 
Jmaps will be particularly useful where any of the following are true:

- Many steps (Paths) are required to complete the problem
- Many parameters determine the result of running a Path. Those parameters might include:

   - Timestamps (such as when an optical alignment is performed)
   - Values that depend on results from other Paths (such as a locking frequency for a laser based on a measurement of a resonance)

- It is important to automatically save and retrieve previous Path results based on their parameter values
- It is important to view all parameter sets that have been used to run a Path (in case you forget or want to see what you've done), and retrieve data for an individual run (not yet implemented)
- You want to optimize over an arbitrary subset of the parameters (not yet implemented)

Below, we describe the core concepts of Jmaps.

Journeys (class Journey)
========================


Paths (class JPath)
===================

In any given Journey (parameter search), you often have multiple steps, processes, or simulations that  
lead to refining the parameters. In JMaps, these are called **Paths**. For a Path, one must often:

- **run**: Do the step, process, or simulation
- **ponder**: Look at graphs, visuals, and tables
- **evaluate**: Determine the Figure of Merit (FoM) of your parameters for this Path.  
  This could be efficiency, loss, accuracy, or more.

Environments (using ``JDict``)
==============================

Paths will often share some parameters, and have their own parameters.  
The group of all parameters that share the same set of Paths is called an **Environment**.  
In code, environments are represented by :class:`jmaps.journey.param.JDict`, a tree of parameters.
All parameters in a Journey are sorted into Environments.

Parameters (``JParam`` tree)
============================

Parameters are represented as a tree of :class:`jmaps.journey.param.JParam` objects.
The most common building blocks are:

- **JValue**: Leaf parameter that holds a concrete Python value (with an optional explicit dtype).
- **JDict**: Dictionary of named child parameters, typically used as the top-level environment.
- **Buffer / XBuffer / YBuffer**: Cached values that can reset based on a :class:`ResetCondition`.
- **InvisibleParam**: Wrapper that hides parameters from SQL exports by default.
- **Refer**: Reference to another parameter elsewhere in the environment tree.

Together these let you define rich, nested environments that can be locked during path execution,
tracked for usage, and exported to SQL-friendly structures for caching.

The package provides:

* **Journey Framework**: Core functionality for automated parameter search and optimization
* **Paths Package**: Implementations of specific paths (e.g., Tidy3D)

Contents
--------

.. toctree::
   Home <self>
   installation
   tutorial
   api
