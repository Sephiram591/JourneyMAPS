import os
import sys
from datetime import datetime

# Add project root to sys.path so Sphinx can find the package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

project = "jmaps"
author = "Helaman Flores"
# copyright = f"{datetime.now().year}, {author}"

extensions = [
    "sphinx.ext.autodoc",
    # "sphinx.ext.napoleon",
    # "sphinx.ext.viewcode",
    "sphinx.ext.autosummary",
    "myst_parser",
]

autosummary_generate = True
autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
}

templates_path = ["_templates"]
exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
]

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

napoleon_google_docstring = True
napoleon_numpy_docstring = True

master_doc = "index"

autodoc_mock_imports = [
    "pmag",
]