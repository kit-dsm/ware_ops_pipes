# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import os
import sys
import shutil

sys.path.insert(0, os.path.abspath('../../src'))

project = 'ware_ops_algos'
author = 'Janik Bischoff'
release = '0.1'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    'sphinx.ext.intersphinx',
    'sphinx.ext.autosectionlabel',
    'sphinx.ext.autosummary',
    'sphinx.ext.napoleon',  # For Google/NumPy docstrings
    # 'sphinxcontrib.tikz',
    'autoapi.extension',
    'sphinx_design',  # For grid cards
    'sphinx_copybutton',  # Copy buttons on code blocks
    'myst_nb'
]

autoapi_type = 'python'
# Updated to point to new src structure
autoapi_dirs = [
    '../../src/ware_ops_algos/algorithms',
    '../../src/ware_ops_algos/domain_models',
    '../../src/ware_ops_algos/utils',
]

# AutoAPI options
autoapi_options = [
    'members',
    'undoc-members',
    'show-inheritance',
    'show-module-summary',
    'imported-members',
]

# Copy examples to docs
# DOCS_EXAMPLES = '../../docs/source/examples'
# if os.path.exists(DOCS_EXAMPLES):
#     shutil.rmtree(DOCS_EXAMPLES)
# if not os.path.exists(DOCS_EXAMPLES):
#     os.makedirs(DOCS_EXAMPLES)

# Check if examples directory exists before copying
# EXAMPLES_DIR = '../../examples'
# if os.path.exists(EXAMPLES_DIR):
#     shutil.copytree(EXAMPLES_DIR, DOCS_EXAMPLES, dirs_exist_ok=True)
#
# if os.path.exists('examples.rst'):
#     os.remove('examples.rst')
#
# with open("examples.rst", 'w+') as f:
#     f.write('Examples\n')
#     f.write('========\n\n')
#
#     f.write('.. toctree::\n')
#     f.write('   :maxdepth: 2\n\n')
#
#     if os.path.exists("examples"):
#         for root, dirs, files in os.walk("examples"):
#             for file in files:
#                 if file.endswith('.rst'):
#                     readme_path = os.path.join(root, file)
#                     dir_name = os.path.basename(os.path.dirname(readme_path))
#                     if dir_name == "getting_started":
#                         continue
#                     rel_path = os.path.relpath(os.path.join(root, file), "examples")
#                     rel_path = rel_path.replace(os.sep, '/')
#                     f.write(f'   ./examples/{os.path.splitext(rel_path)[0]}\n')

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']

html_theme = 'furo'

html_theme_options = {
    "light_css_variables": {
        "color-brand-primary": "#0080c0",
        "color-brand-content": "#0080c0",
    },
    "dark_css_variables": {
        "color-brand-primary": "#00b4d8",
        "color-brand-content": "#00b4d8",
    },
    "sidebar_hide_name": True,
}

html_title = "ware_ops_algos: Algorithms for warehouse operations"
html_short_title = "4D4L"

html_meta = {
    'description': 'Open source implementation of algorithms for warehouse operations',
    'keywords': 'warehouse optimization, order picking, algorithm selection, logistics, operations research',
}

html_logo = "_static/favicon.png"
html_favicon = "_static/favicon.svg"
html_context = {
  'display_github': True,
  'github_user': 'kit-dsm',  # Update this
  'github_repo': 'ware_ops_algos',
  'github_version': 'main/docs/',
}

# Autodoc settings
autodoc_default_options = {
    'members': True,
    'undoc-members': True,
    'private-members': False,
    'special-members': '__init__',
    'inherited-members': True,
    'show-inheritance': True,
}