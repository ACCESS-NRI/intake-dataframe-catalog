# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

html_title = "intake-dataframe-catalog"
project = "intake-dataframe-catalog"
copyright = "2023, ACCESS-NRI"
author = "ACCESS-NRI"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "numpydoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "myst_nb",
    "sphinx_copybutton",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

autosummary_generate = True
autodoc_typehints = "none"
autodoc_member_order = "groupwise"

# Config numpydoc
numpydoc_show_class_members = True
numpydoc_show_inherited_class_members = False
numpydoc_class_members_toctree = False

master_doc = "index"

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "sphinx"

# Config myst-nb
nb_execution_excludepatterns = ["quickstart.ipynb"]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_book_theme"
html_static_path = ["_static"]
html_context = {
    "github_user": "dougiesquire",
    "github_repo": "intake-dataframe-catalog",
    "github_version": "main",
    "doc_path": "./docs",
}
html_theme_options = {
    "use_edit_page_button": True,
    "github_url": "https://github.com/ACCESS-NRI/intake-dataframe-catalog",
}
