# -*- coding: utf-8 -*-
#
# Configuration file for the Sphinx documentation builder.
#
# This file does only contain a selection of the most common options. For a
# full list see the documentation:
# http://www.sphinx-doc.org/en/master/config

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

from activities import __version__

sys.path.insert(0, os.path.abspath("../../"))


# -- Project information -----------------------------------------------------

project = "activities"
copyright = "2021, hackingmaterials"
author = "Alex Ganose"

# The short X.Y version
version = __version__
# The full version, including alpha/beta/rc tags
release = __version__


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx.ext.autosummary",
    # "m2r2",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["Thumbs.db", ".DS_Store", "test*.py"]

# use type hints
autodoc_typehints = "description"
autoclass_content = "both"

napoleon_use_param = True
napoleon_use_rtype = True
napoleon_use_ivar = True
autodoc_member_order = "bysource"

# The suffix(es) of source filenames.
source_suffix = [".rst", ".md"]

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'furo'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
html_css_files = ["custom.css"]
html_favicon = "_static/favicon.ico"
html_theme_options = {
    "light_css_variables": {
        "admonition-font-size": "92%",
        "admonition-title-font-size": "92%",
    },
    "dark_css_variables": {
        "admonition-font-size": "92%",
        "admonition-title-font-size": "92%"
    }
}
html_title = f"Activities v{__version__}"

html_context = {
    "display_github": True,
    "github_user": "hackingmaterials",
    "github_repo": "activities",
    "github_version": "master",
    "conf_py_path": "/docs_rst/",
}

# -- Options for intersphinx extension ---------------------------------------

# Example configuration for intersphinx: refer to the Python standard library.
intersphinx_mapping = {
    "python": ("https://docs.python.org/3.8", None),
    "numpy": ("http://docs.scipy.org/doc/numpy/", None),
    "pymatgen": ("http://pymatgen.org/", None),
    "h5py": ("http://docs.h5py.org/en/latest/", None),
    "matplotlib": ("http://matplotlib.org", None),
}
