import os
import sys
from datetime import datetime

# Add project root to sys.path so Sphinx can find the package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

project = "jmaps"
author = "Helaman Flores"
copyright = f"{datetime.now().year}, {author}"

extensions = [
    "sphinx.ext.duration",
    "sphinx.ext.doctest",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    'sphinx.ext.napoleon',
]

napoleon_google_docstring = True

templates_path = ["_templates"]
exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
]

intersphinx_mapping = {
    "rtd": ("https://docs.readthedocs.io/en/stable/", None),
    "python": ("https://docs.python.org/3/", None),
    "sphinx": ("https://www.sphinx-doc.org/en/master/", None),
}
intersphinx_disabled_domains = ["std"]

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

# Global navigation configuration
html_sidebars = {
    '**': [
        'globaltoc.html',
        'relations.html',
        'sourcelink.html',
        'searchbox.html',
    ]
}

master_doc = "index"

autodoc_mock_imports = [
    "pmag",
    "tidy3d",
    "gplugins",
    "gdstk",
    "sqlalchemy",
    "dill",
    "numpy",
    "matplotlib",
]

# Autodoc configuration
autodoc_default_options = {
    'members': True,
    'member-order': 'bysource',
    'special-members': '__init__',
    'undoc-members': True,
    'exclude-members': '__weakref__'
}

# Autosummary configuration
autosummary_generate = True
autosummary_imported_members = True