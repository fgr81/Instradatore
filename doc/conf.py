# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Instradatore'
copyright = '2024, fabio grasso'
author = 'fabio grasso'
release = '0.1'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['sphinx.ext.napoleon', 'sphinx.ext.autodoc', 'sphinx.ext.viewcode']

templates_path = ['_templates']
exclude_patterns = ['/cantiere/opendap_data/instradatore_doc/', 'Thumbs.db', '.DS_Store']



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# html_theme = 'alabaster'
html_theme = 'furo'
html_static_path = ['_static']


#fmg

import os
import sys

# Aggiungi i percorsi ai sorgenti
sys.path.insert(0, os.path.abspath('../instradatore'))
sys.path.insert(1, os.path.abspath('../handlers'))

