"""Tidy3D-backed path utilities.

This module provides a `JPath` implementation that integrates with `gplugins`
and `tidy3d` to generate photonic components, run simulations (either via the
default GDS modeler or custom FDTD parameters), and visualize results.

"""
from abc import abstractmethod
from jmaps.journey.path import JPath, PathResult
from jmaps.journey.param import JDict
from pydantic import Field
import gplugins as gp
import gplugins.tidy3d as gt
import matplotlib.pyplot as plt
from pmag.simulation.tidytools import validate_sim_for_daily_allowance, get_fdtd_sim
import tidy3d as td
from typing import Any

def evaluate_keys(env: JDict, keys: str|list[str]):
    if isinstance(keys, str):
        return env[keys]
    result = env
    for key in keys:
        result = result[key]
    return result

class GDS_Tidy3DPath(JPath):
    """A `JPath` that creates a component, then simulates it with Tidy3D.

    Subclasses may optionally override `get_component`
    and `batch_modeler` to define how components are generated and how batches
    of simulations are formed.
    """
    
    gds_component: str|list[str]|None = Field(['gds_component'], description='List of keys leading to the gds component for the path.')
    custom_fdtd: str|list[str]|None = Field(None, description='List of keys leading to the fdtd parameters for the path.')
    td_modeler_args: str|list[str]|None = Field(None, description='List of keys leading to the modeler parameters for the path')
    td_component_args: str|list[str]|None = Field(None, description='List of keys leading to the component parameters for the path')
    delete_server_data: bool = True

    @property
    def name(self) -> str:
        """Unique name of the path"""
        return "gds_tidy3d"

    def get_component(self, env: JDict, subpath_results: dict[str, Any], batch_i: int=0):
        '''Override this method to return the component of the path.'''
        return env['gds_component']['c'](**env['gds_args'])
    
    def _run(self, env: JDict, subpath_results: dict[str, Any], verbose: bool=False):
        ''' Run the simulation of the component.
        Args:
            envs: Dictionary of environment variables.
            subpath_results: Dictionary of results from the subpaths.
            verbose: Whether to print verbose output.
        Returns:
            sp: S-parameters of the component if using default gds modeler.
            result: SimulationData object of the tidy3d simulation if using custom fdtd parameters.
        '''
        result = PathResult()
        if not self.custom_fdtd:
            sp = gt.write_sparameters(
                component=evaluate_keys(env, self.gds_component),
                **evaluate_keys(env, self.td_component_args),
                **evaluate_keys(env, self.td_modeler_args),
            )
            result.file['s_params'] = sp
        else:
            batch = None
            try:
                sim, td_c, modeler = self.get_simulation(env, subpath_results)
                job = td.web.Job(sim, verbose=verbose)
                result.file['sim_data'] = batch.run()
            finally:
                if self.delete_server_data and batch is not None:
                    job.delete()
        return result

    def plot(self, result: Any, subpath_results: dict[str, Any]):
        """Plots the S-parameters of the component if using default gds modeler.
        
        Args:
            result: The results of the path run given the environments.
            subpath_results: Dictionary of results from the subpaths.
        """
        if not self.custom_fdtd:
            plt.axhline(0, color='k', linestyle='--')
            plt.axhline(1, color='k', linestyle='--')
            gp.plot.plot_sparameters(result.file['s_params'], logscale=False)

    def get_simulation(self, env: JDict, subpath_results: dict[str, Any]):
        """Get the simulation of the component.
        
        Args:
            envs: Dictionary of environment variables.
        
        Returns:
            sim: tidy3d Simulation object.
            td_c: gds Tidy3DComponent of the component.
            modeler: gds ComponentModeler of the component.
        """
        c = evaluate_keys(env, self.gds_component)
        td_c = gt.Tidy3DComponent(component=c,**evaluate_keys(env, self.td_component_args))
        modeler = td_c.get_component_modeler(**evaluate_keys(env, self.td_modeler_args))
        sim = modeler.simulation
        if self.custom_fdtd:
            sim = modeler.simulation.copy(update=dict(**evaluate_keys(env, self.custom_fdtd)))
        return sim, td_c, modeler

    def plot_geom(self, env: JDict, subpath_results: dict[str, Any], layer_name: str, validate=True):
        """Plot the geometry of the component.
        
        Args:
            envs: Dictionary of environment variables.
            layer_name: Name of the gds layer to plot.
            validate: Whether to validate the simulation for the daily allowance.
        """
        sim, td_c, modeler = self.get_simulation(env, subpath_results)
        # we can plot the tidy3d simulation setup
        if self.custom_fdtd:
            fig, ax = plt.subplots(3, 1)
            sim.plot(z=td_c.get_layer_center(layer_name)[2], ax=ax[0])
            sim.plot(x=td_c.ports[0].dcenter[0], ax=ax[1])
            sim.plot(y=td_c.ports[0].dcenter[1], ax=ax[2])
            fig.tight_layout()
            plt.show()
        else:
            fig, ax = plt.subplots(3, 1)
            modeler.plot_sim(z=td_c.get_layer_center(layer_name)[2], ax=ax[0])
            modeler.plot_sim(x=td_c.ports[0].dcenter[0], ax=ax[1])
            modeler.plot_sim(y=td_c.ports[0].dcenter[1], ax=ax[2])
            fig.tight_layout()
            plt.show()
        if validate:
            validate_sim_for_daily_allowance(sim)

    def plot_mode(self, env:JDict, subpath_results: dict[str, Any], mode_index=0, port_index=0):
        """Plot the mode of the component.
        
        Args:
            envs: Dictionary of environment variables.
            mode_index: Index of the mode to plot.
            port_index: Index of the port to plot.
        """
        c = evaluate_keys(env, self.td_component_args)
        sp = gt.write_sparameters(
            component=c,
            **evaluate_keys(env, self.td_component_args),
            **evaluate_keys(env, self.td_modeler_args),
            plot_mode_index=mode_index,
            plot_mode_port_name=c.ports[port_index].name,
        )
