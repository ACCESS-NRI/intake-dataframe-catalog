intake-dataframe-catalog 
========================

**An intake plugin for a searchable table of intake catalogs and associated metadata**

.. toctree::
   :maxdepth: 1
   :hidden:

   getting_started/index
   reference/index

Overview
--------

intake-dataframe-catalog is a simple intake plugin for a searchable table of intake catalogs. 
The table is represented in memory as a pandas DataFrame and can be serialized and shared as 
a CSV file. Each row in the dataframe catalog corresponds to another intake catalog (refered 
to in this documentation as a "subcatalog") and the columns contain metadata associated with 
each subcatalog that a user may want to peruse and/or search. The original use-case for 
intake-dataframe-catalog was to provide a user-friendly catalog of a large number 
`intake-esm <https://intake-esm.readthedocs.io/en/stable/>`_ catalogs. intake-dataframe-catalog 
enables users to peruse and search on core metadata from each intake-esm subcatalog to find 
the subcatalogs that are most relevant to their work (e.g. "which subcatalogs contain model 
X and variable Y?"). Once a users has found the subcatalog(s) that interest them, they can 
load those subcatalogs and access the data they reference.

Why?
----

Intake already provides the ability to 
`nest catalogs <https://intake.readthedocs.io/en/latest/catalog.html#catalog-nesting>`_ and 
search across them. However, data discoverability is limited in the case of very large numbers
of nested catalogs, and the search functionality does readily provide the ability to execute 
complex searches on nested catalog metadata. intake-dataframe-catalog aims to provide a very
simple catalog of subcatalogs that emphasises subcatalog search and discoverability.

Get in touch
------------

If you encounter any issues with intake-dataframe-catalog or you'd like to request any new features, please open an issue `here <https://github.com/ACCESS-NRI/intake-dataframe-catalog/issues>`_.
