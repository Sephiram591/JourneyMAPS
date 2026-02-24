JourneyMAPS Documentation
=========================

Journey - A Multistep Automated Parameter Search Library
--------------------------------------------------------
A single journey can be life changing. However, tens or hundreds of them get overwhelming pretty quickly.  
This package is meant to generalize the pipeline for the completion of any parameter search (or Journey),  
to make embarking on and completing many Journeys feasible.

JourneyMAPS (Journey - A Multistep Automated Parameter Search Package) is a Python package designed for automated parameter optimization 
and simulation workflows. It avoids repetitious computation by saving all results by their input parameter values to a database, 
loading relevant results automatically from the database or from automatically saved files. It is used most effectively as an 
ideologically consistent framework for you to build jupyter-notebooks for any experiment, simulation, or optimization problem. 
Thus, regardless of whether you are running photonics simulations, quantum experiments, machine learning, or any other type of 
problem, your notebooks will have an elegant format for legibility and reproducibility. 

Below, we describe the core concepts of Jmaps.

Paths (class JPath)
===================

In any given Journey (parameter search), you often have multiple steps, processes, or simulations that  
lead to refining the parameters. In JMaps, these are called **Paths**. For a Path, one must often:

- **run**: Do the step, process, or simulation
- **plot**: Look at graphs, visuals, and tables
- **update environment**: Update the environment based on relevant results from the Path

When the path is deterministic (not dependent on time or randomness), results will be automatically

Journeys (class Journey)
========================

A Journey is a container for a set of Paths. It is used to define the overall problem, and to run the Paths in a consistent 
manner. Often a large experiment will be broken down into multiple steps 
(such as: initial simulation, alignment, validation experiment, data analysis).

Environments (using ``JDict``)
==============================

Paths will often share some parameters, and have their own parameters. Environments automatically track parameter usage,
such that only the parameters that are used by a Path are saved to the database for a given path result
In code, environments are represented by :class:`jmaps.journey.param.JDict`, a tree of parameters.
All parameters in a Journey are sorted into Environments.

Parameters (``JParam`` tree)
============================

Parameters are represented as a tree of :class:`jmaps.journey.param.JParam` objects.
The most common building blocks are:

- **JDict**: Dictionary of named child parameters, typically used as the top-level environment.
- **XBuffer / YBuffer**: Functions/classes that are evaluated when accessed, or retreived from a cache of their previous evaluation. The cache can reset based on a :class:`ResetCondition`. XBuffers save only only the output of the function to the database, while YBuffers save only input.
- **InvisibleParam**: Wrapper that hides parameters from SQL exports by default.
- **Refer**: Reference to another parameter elsewhere in the environment tree.
- **JValue**: Leaf parameter that holds a concrete Python value (with an optional explicit dtype). 


Together these let you define rich, nested environments that are tracked for usage and exported to SQL-friendly 
structures for saving and loading results.

The package provides:

* **Journey Framework**: Core functionality for automated parameter search and optimization
* **Paths Package**: Implementations of specific paths (e.g., Tidy3D)
* **IO Package**: Functions for saving and loading results to and from files. Useful for large results, or results that are not easily serializable to JSONB.

Contents
--------

.. toctree::
   Home <self>
   installation
   tutorial
   api
