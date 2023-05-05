# intake-dataframe-catalog

A simple intake plugin for a searchable table of intake catalogs and associated metadata.
__________

| Documentation | [![docs][docs-badge]][docs-link] |
| :---: |  :---: |
| **Package** | [![PyPI][PyPI-badge]][PyPI-link] |
| **CI** | [![tests][ci-tests-badge]][ci-tests-link] [![pre-commit][ci-precommit-badge]][ci-precommit-link] |
| **Development** | [![codecov][codecov-badge]][codecov-link] [![black][black-badge]][black-link] |
| **License** | [![License][license-badge]][license-link] |

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

## Installation

To install using the [pip](https://pypi.org/project/pip/) package manager:

``` bash
$ python -m pip install intake-dataframe-catalog
```

[docs-badge]: https://readthedocs.org/projects/intake-dataframe-catalog/badge/?version=latest
[docs-link]: https://intake-dataframe-catalog.readthedocs.io/en/latest/?badge=latest
[PyPI-badge]: https://img.shields.io/pypi/v/intake-dataframe-catalog
[PyPI-link]: https://pypi.org/project/intake-dataframe-catalog/
[ci-tests-badge]: https://github.com/ACCESS-NRI/intake-dataframe-catalog/actions/workflows/tests.yml/badge.svg
[ci-tests-link]: https://github.com/ACCESS-NRI/intake-dataframe-catalog/actions/workflows/tests.yml
[ci-precommit-badge]: https://github.com/ACCESS-NRI/intake-dataframe-catalog/actions/workflows/pre-commit.yml/badge.svg
[ci-precommit-link]: https://github.com/ACCESS-NRI/intake-dataframe-catalog/actions/workflows/pre-commit.yml
[codecov-badge]: https://codecov.io/gh/ACCESS-NRI/intake-dataframe-catalog/branch/main/graph/badge.svg?token=4EZNH1HYAN
[codecov-link]: https://codecov.io/gh/ACCESS-NRI/intake-dataframe-catalog
[black-badge]: https://img.shields.io/badge/code%20style-black-000000.svg
[black-link]: https://github.com/python/black
[license-badge]: https://img.shields.io/github/license/ACCESS-NRI/intake-dataframe-catalog
[license-link]: https://github.com/ACCESS-NRI/intake-dataframe-catalog/blob/main/LICENSE
