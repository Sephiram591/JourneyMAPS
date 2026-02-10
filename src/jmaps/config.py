import pathlib

__version__ = "0.0"
__next_major_version__ = "0.1"

PathType = str | pathlib.Path

home = pathlib.Path.home()
cwd = pathlib.Path.cwd()
module_path = pathlib.Path(__file__).parent.absolute()
repo_path = module_path.parent


class Paths:
    """Paths is a class that contains the paths to:
    module, repo, cwd, and cache.
    
    Args:
        module (pathlib.Path): The path to the module.
        repo (pathlib.Path): The path to the repo.
        cwd (pathlib.Path): The path to the current working directory.
        journeys (pathlib.Path): The path to the journey storage directory.
    """
    module = module_path
    repo = repo_path
    cwd = cwd
    journeys = module / "journeys"


PATH = Paths()