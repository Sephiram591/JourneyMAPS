"""Tidy3D-backed path utilities.

This module provides a `JPath` implementation that integrates with `gplugins`
and `tidy3d` to generate photonic components, run simulations (either via the
default GDS modeler or custom FDTD parameters), and visualize results.

"""
from abc import abstractmethod
from jmaps.journey.path import JPath
from jmaps.journey.environment import JEnv
import gplugins as gp
import gplugins.tidy3d as gt
import matplotlib.pyplot as plt
from pmag.simulation.tidytools import validate_sim_for_daily_allowance, get_fdtd_sim
import tidy3d as td
from typing import Any

class GDS_Tidy3DPath(JPath):
    """A `JPath` that creates a component, then simulates it with Tidy3D.

    Subclasses may optionally override `get_component`
    and `batch_modeler` to define how components are generated and how batches
    of simulations are formed.
    """
    def __init__(self, custom_fdtd=None, custom_modeler=None, delete_server_data=True):
        """Initialize the GDS_Tidy3DPath.
        
        Args:
            custom_fdtd: Name of the custom fdtd environment. If not none, the simulation will be run using tidy3d directly with the custom fdtd parameters.
            custom_modeler: Name of the custom modeler environment, passed to the gds ComponentModeler.
        """
        self.custom_fdtd = custom_fdtd
        self.custom_modeler = custom_modeler
        self.delete_server_data = delete_server_data
    
    @property
    def name(self) -> str:
        """Unique name of the path"""
        return "gds_tidy3d"

    @property
    def env_names(self) -> list[str]:
        """List of required environment names for all subclasses."""
        env_reqs = ['component', 'modeler', 'gds_component', 'gds_args', 'pdk', 'materials']
        env_reqs.extend([self.custom_modeler] if self.custom_modeler else [])
        env_reqs.extend([self.custom_fdtd] if self.custom_fdtd else [])
        # env_reqs.extend(self._env_names)
        return env_reqs

    def get_component(self, envs: dict[str, JEnv], subpath_results: dict[str, Any], batch_i: int=0):
        '''Override this method to return the component of the path.'''
        return envs['gds_component']['c'](**envs['gds_args'])
    
    def batch_modeler(self, envs: dict[str, JEnv], subpath_results: dict[str, Any]) -> list[dict[str, Any]]:
        '''Override this method to return a list of modeler parameters that form a batch of simulations that are run by this path. 
        By default, a single simulation is run with no extra modeler parameters.'''
        return [{}]
    
    def _run(self, envs: dict[str, JEnv], subpath_results: dict[str, Any], verbose: bool=False):
        ''' Run the simulation of the component.
        Args:
            envs: Dictionary of environment variables.
            subpath_results: Dictionary of results from the subpaths.
            verbose: Whether to print verbose output.
        Returns:
            sp: S-parameters of the component if using default gds modeler.
            result: SimulationData object of the tidy3d simulation if using custom fdtd parameters.
        '''
        if not self.custom_fdtd:
            batch_results = {}
            for batch_i, batch_modeler_params in enumerate(self.batch_modeler(envs, subpath_results)):
                c = self.get_component(envs, subpath_results, batch_i)
                sp = gt.write_sparameters(
                    component=c,
                    **envs['component'],
                    **envs['modeler'],
                    **(envs[self.custom_modeler] if self.custom_modeler else {}),
                    **batch_modeler_params
                )
                batch_results[batch_i] = sp
        else:
            batch = None
            try:
                sims = {}
                for batch_i, batch_modeler_params in enumerate(self.batch_modeler(envs, subpath_results)):
                    sim, td_c, modeler = self.get_simulation(envs, subpath_results, batch_i)
                    sims[self.name + f"_b{batch_i}"] = sim
                batch = td.web.Batch(simulations=sims, verbose=verbose)
                batch_data = batch.run('/home/floresh2/orcd/scratch/tidy3d')
                batch_results = {}
                for batch_i, (task_name, sim_data) in enumerate(batch_data.items()):
                    # print(type(sim_data))
                    # print(f"sim_data: {sim_data}")
                    # print(type(sim_data.log))
                    # print(sim_data.log)
                    batch_results[batch_i] = sim_data
            finally:
                if self.delete_server_data and batch is not None:
                    batch.delete()
        return batch_results

    def ponder(self, result: Any, subpath_results: dict[str, Any]):
        """Plots the S-parameters of the component if using default gds modeler.
        
        Args:
            result: The results of the path run given the environments.
            subpath_results: Dictionary of results from the subpaths.
        """
        if not self.custom_fdtd:
            for batch_result in result:
                plt.axhline(0, color='k', linestyle='--')
                plt.axhline(1, color='k', linestyle='--')
                gp.plot.plot_sparameters(batch_result, logscale=False)

    def get_simulation(self, envs: dict[str, JEnv], subpath_results: dict[str, Any], batch_i: int=0):
        """Get the simulation of the component.
        
        Args:
            envs: Dictionary of environment variables.
        
        Returns:
            sim: tidy3d Simulation object.
            td_c: gds Tidy3DComponent of the component.
            modeler: gds ComponentModeler of the component.
        """
        c = self.get_component(envs, subpath_results, batch_i)
        td_c = gt.Tidy3DComponent(component=c,**envs['component'])
        custom_modeler_params = envs[self.custom_modeler] if self.custom_modeler else {}
        batch_modeler_params = self.batch_modeler(envs, subpath_results)[batch_i]
        modeler = td_c.get_component_modeler(**envs['modeler'], **custom_modeler_params, **batch_modeler_params)
        if self.custom_fdtd and self.custom_fdtd in self.env_names:
            sim = get_fdtd_sim(td_c, modeler, **envs[self.custom_fdtd])
        else:
            sim = modeler.simulation
        return sim, td_c, modeler

    def plot_geom(self, envs: dict[str, JEnv], subpath_results: dict[str, Any], layer_name: str, validate=True, batch_i: int=0):
        """Plot the geometry of the component.
        
        Args:
            envs: Dictionary of environment variables.
            layer_name: Name of the gds layer to plot.
            validate: Whether to validate the simulation for the daily allowance.
        """
        sim, td_c, modeler = self.get_simulation(envs, subpath_results, batch_i)
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

    def plot_mode(self, envs: dict[str, JEnv], subpath_results: dict[str, Any], mode_index=0, port_index=0, batch_i: int=0):
        """Plot the mode of the component.
        
        Args:
            envs: Dictionary of environment variables.
            mode_index: Index of the mode to plot.
            port_index: Index of the port to plot.
        """
        c = self.get_component(envs, subpath_results, batch_i)
        custom_modeler_params = envs[self.custom_modeler] if self.custom_modeler else {}
        batch_modeler_params = self.batch_modeler(envs, subpath_results)[batch_i]
        sp = gt.write_sparameters(
            component=c,
            **envs['component'],
            **envs['modeler'],
            **custom_modeler_params,
            **batch_modeler_params,
            plot_mode_index=mode_index,
            plot_mode_port_name=c.ports[port_index].name,
        )
