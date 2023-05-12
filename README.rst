========================
intake-dataframe-catalog
========================

**A simple intake plugin for a searchable table of intake sources and associated metadata.**

------------

+---------------+----------------------+
| Documentation | |docs|               |
+---------------+----------------------+
| Package       | |pypi| |conda|       |
+---------------+----------------------+
| CI/CD         | |ci| |cd|            |
+---------------+----------------------+
| Development   | |codecov| |black|    |
+---------------+----------------------+
| License       | |license|            |
+---------------+----------------------+

Overview
--------

intake-dataframe-catalog is a simple intake plugin for a searchable table of intake sources. 
The table is represented in memory as a pandas DataFrame and can be serialized and shared as 
a CSV file. Each row in the dataframe catalog corresponds to an intake source and the columns 
contain metadata associated with each source that a user may want to peruse and/or search. 
The original use-case for intake-dataframe-catalog was to provide a user-friendly catalog of 
a large number `intake-esm <https://intake-esm.readthedocs.io/en/stable/>`_ datastores. 
intake-dataframe-catalog enables users to peruse and search on core metadata from each 
intake-esm datastore to find the datastores that are most relevant to their work (e.g. 
"which datastores contain model X and variable Y?"). Once a users has found the datastores(s) 
that interest them, they can load those datastores and access the data they reference.

Why?
----

Intake already provides the ability to 
`nest sources in a catalog <https://intake.readthedocs.io/en/latest/catalog.html#catalog-nesting>`_ 
and search across them. However, data discoverability is limited in the case of very large numbers
of nested sources, and the search functionality does readily provide the ability to execute 
complex searches on nested source metadata. intake-dataframe-catalog aims to provide a very
simple catalog of intake sources that emphasises source search and discoverability.
    

.. |docs| image:: https://readthedocs.org/projects/intake-dataframe-catalog/badge/?version=latest
        :target: https://intake-dataframe-catalog.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status
        
.. |pypi| image:: https://img.shields.io/pypi/v/intake-dataframe-catalog
        :target: https://pypi.org/project/intake-dataframe-catalog/
        :alt: PyPI package
        
.. |conda| image:: https://img.shields.io/conda/v/accessnri/intake-dataframe-catalog
        :target: https://anaconda.org/accessnri/intake-dataframe-catalog
        :alt: Conda package

.. |ci| image:: https://github.com/ACCESS-NRI/intake-dataframe-catalog/actions/workflows/ci.yml/badge.svg
        :target: https://github.com/ACCESS-NRI/intake-dataframe-catalog/actions/workflows/ci.yml
        :alt: Package CI test status
        
.. |cd| image:: https://github.com/ACCESS-NRI/intake-dataframe-catalog/actions/workflows/cd.yml/badge.svg
        :target: https://github.com/ACCESS-NRI/intake-dataframe-catalog/actions/workflows/cd.yml
        :alt: Package CD status
        
.. |codecov| image:: https://codecov.io/gh/ACCESS-NRI/intake-dataframe-catalog/branch/main/graph/badge.svg?token=4EZNH1HYAN
        :target: https://codecov.io/gh/ACCESS-NRI/intake-dataframe-catalog
        :alt: Code test coverage
        
.. |black| image:: https://img.shields.io/badge/code%20style-black-000000.svg
        :target: https://github.com/python/black
        :alt: Black code formatter
        
.. |license| image:: https://img.shields.io/github/license/ACCESS-NRI/intake-dataframe-catalog
        :target: https://github.com/ACCESS-NRI/intake-dataframe-catalog/blob/main/LICENSE
        :alt: Apache-2.0 License
