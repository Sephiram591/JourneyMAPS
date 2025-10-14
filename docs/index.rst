JourneyMAPS Documentation
=========================

Journey - A Multistep Automated Parameter Search Library
--------------------------------------------------------

JourneyMAPS (Journey - A Multistep Automated Parameter Search Package) is a Python package designed for automated parameter optimization 
and simulation workflows. It is used most effectively as an ideologically consistent framework for you to build jupyter-notebooks for 
any experiment, simulation, or optimization problem. Thus, regardless of whether you are running photonics simulations, quantum experiments,
machine learning, or any other type of problem, your notebooks will have an elegant format for legibility and reproducibility. 
Jmaps will be particularly useful where any of the following are true:

- Many steps are required to complete the problem..
- Many parameters are involved (which might include timestamps), some of which may be dependent on other parameters or on results from other steps
- You want to optimize over an arbitrary subset of the parameters (not yet implemented)
- It is important to save and retrieve previous results, and even better if this is done automatically in the background
- It is important to view the parameters spaces that have been explored, and quickly retrieve previous results from a location in that space (not yet implemented)

Journeys (class Journey)
========================

A single journey can be life changing. However, tens or hundreds of them get overwhelming pretty quickly.  
This package is meant to generalize the pipeline for the completion of any parameter search (or Journey),  
to make embarking on and completing many Journeys feasible.

Paths (class JPath)
===================

In any given Journey (parameter search), you often have multiple steps, processes, or simulations that  
lead to refining the parameters. In JMaps, these are called **Paths**. For a Path, one must often:

- **run**: Do the step, process, or simulation
- **ponder**: Look at graphs, visuals, and tables
- **evaluate**: Determine the Figure of Merit (FoM) of your parameters for this Path.  
  This could be efficiency, loss, accuracy, or more.

Environments (class JEnv)
=========================

Paths will often share some parameters, and have their own parameters.  
The group of all parameters that share the same set of Paths is called an **Environment**.  
All parameters in a Journey are sorted into Environments.

Parameters (class JParams, with children JVar, JSet, JOpt)
==========================================================

Parameters have a value and a type. The three types are:

- **Variable (JVar)**: Will be changed by optimizers operating on a Path.
- **Settings (JSet)**: Unchanged by optimizers, but affect the result of the Journey.
- **Options (JOpt)**: Unchanged by optimizers, and don't affect the result of any Path on the Journey.  
  These are parameters such as ``plot=True`` and ``verbose=False``. Any JParams of this type will  
  be ignored when looking up or saving the Path results.

The package provides:

* **Journey Framework**: Core functionality for automated parameter search and optimization
* **Paths Package**: Implementations of specific paths (e.g., Tidy3D)

Contents
--------

.. toctree::
   Home <self>
   installation
   api
