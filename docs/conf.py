import os
import sys
from datetime import datetime

# Add project root to sys.path so Sphinx can find the package
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

project = "jmaps"
author = "Helaman Flores"
# copyright = f"{datetime.now().year}, {author}"

extensions = [
    "sphinx.ext.duration",
    "sphinx.ext.doctest",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
]

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


# master_doc = "index"

autodoc_mock_imports = [
    "pmag",
]