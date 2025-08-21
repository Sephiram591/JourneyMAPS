from abc import ABC, abstractmethod
from jmaps.journey.path import JPath
from jmaps.journey.environment import JEnv
import gplugins as gp
import gplugins.tidy3d as gt
import matplotlib.pyplot as plt
import pmag as pm
from pmag.simulation.tidytools import validate_sim_for_daily_allowance
import tidy3d as td

class GDS_Tidy3DPath(JPath):
    def __init__(self, custom_fdtd=None, custom_modeler=None):
        self.custom_fdtd = custom_fdtd
        self.custom_modeler = custom_modeler
    @property
    @abstractmethod
    def env_names(self) -> list[str]:
        """List of required environment names"""
        return ['component', 'modeler'] + ([self.custom_modeler] if self.custom_modeler else []) + ([self.custom_fdtd] if self.custom_fdtd else [])

    def _run(self, envs: dict[str, JEnv], verbose: bool=False):
        if not self.custom_fdtd:
            c = self.get_component(envs)
            sp = gt.write_sparameters(
                component=c,
                **envs['component'].get_stripped_params(),
                **envs['modeler'].get_stripped_params(),
                **(envs[self.custom_modeler].get_stripped_params() if self.custom_modeler else {})
            )
            return sp
        else:
            sim, td_c, modeler = self.get_simulation(envs)
            job = td.web.Job(simulation=sim, task_name=self.name, verbose=verbose)
            data = job.run()
            return data

    def ponder(self, envs: dict[str, JEnv], data):
        if not self.custom_fdtd:
            plt.axhline(0, color='k', linestyle='--')
            plt.axhline(1, color='k', linestyle='--')
            gp.plot.plot_sparameters(data, logscale=False)

    def get_simulation(self, envs: dict[str, JEnv]):
        c = self.get_component(envs)
        td_c = gt.Tidy3DComponent(component=c,**envs['component'].get_stripped_params())
        custom_modeler_params = envs[self.custom_modeler].get_stripped_params() if self.custom_modeler else {}
        modeler = td_c.get_component_modeler(**envs['modeler'].get_stripped_params(), **custom_modeler_params)
        if self.custom_fdtd and self.custom_fdtd in self.env_names:
            sim = pm.tidytools.get_fdtd_sim(td_c, modeler, **envs[self.custom_fdtd].get_stripped_params())
        else:
            sim = modeler.simulation
        return sim, td_c, modeler

    def plot_geom(self, envs: dict[str, JEnv], layer_name: str, validate=True):
        sim, td_c, modeler = self.get_simulation(envs)
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

    def plot_mode(self, envs: dict[str, JEnv], mode_index=0, port_index=0):
        c = self.get_component(envs)
        custom_modeler_params = envs[self.custom_modeler].get_stripped_params() if self.custom_modeler else {}
        sp = gt.write_sparameters(
            component=c,
            **envs['component'].get_stripped_params(),
            **envs['modeler'].get_stripped_params(),
            **custom_modeler_params,
            plot_mode_index=mode_index,
            plot_mode_port_name=c.ports[port_index].name,
        )