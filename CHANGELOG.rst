.. _changelog:

Changelog
=========

0.2.4
-----

Released 28/02/2024

- Pin intake<2.0.0 while it is in alpha phase  (:issue:`62`, :pull:`63`). By
  `Dougie Squire <https://github.com/dougiesquire>`_.

0.2.3
-----

Released 02/11/2023

- Move PyPI package upload to trusted publishing (:issue:`44`, :pull:`55`). By
  `Dougie Squire <https://github.com/dougiesquire>`_.
- Fix failing readthedocs build (:issue:`51`, :pull:`54`). By
  `Dougie Squire <https://github.com/dougiesquire>`_.
- Updates for upcoming changes in pandas (:issue:`52`, :pull:`53`). By
  `Dougie Squire <https://github.com/dougiesquire>`_.
- Add fix for fsspec>=2023.10.0 (:issue:`49`, :pull:`50`). By
  `Dougie Squire <https://github.com/dougiesquire>`_.

0.2.2
-----

Released 20/05/2023

- :code:`.to_source_dict(pass_query=True)` now passes queries to the source(s) *sequentially* and
  warns, rather than errors, if a query is not valid on the source (:issue:`40`, :pull:`43`).
  By `Dougie Squire <https://github.com/dougiesquire>`_.
- Fix broken scrolling of :code:`_ipython_display_` in some JupyterLabs (:issue:`41`, :pull:`42`). 
  By `Dougie Squire <https://github.com/dougiesquire>`_.

0.2.1
-----

Released 19/05/2023

- Add :code:`pass_query` flag to :code:`.to_source` and :code:`to_source_dict` methods to enable
  users to blindly pass the most recent query provided to :code:`self.search` on to the 
  :code:`.search` method of the source(s) (:issue:`37`, :pull:`38`). By
  `Dougie Squire <https://github.com/dougiesquire>`_.
- Add the ability to run docs/getting_started/quickstart.ipynb using Binder (:issue:`36`, 
  :pull:`38`). By `Dougie Squire <https://github.com/dougiesquire>`_.
- Add CSV source example to quickstart in docs (:issue:`33`, :pull:`35`). By 
  `Dougie Squire <https://github.com/dougiesquire>`_.
- Make dataframe displayed with :code:`_ipython_display_` scrollable (:issue:`33`, :pull:`35`).
  By `Dougie Squire <https://github.com/dougiesquire>`_.

0.2.0
-----

Released 17/05/2023

- Remove :code:`.to_subcatalog` and :code:`.to_subcatalog_dict` methods (:issue:`31`, :pull:`32`). 
  By `Dougie Squire <https://github.com/dougiesquire>`_.
- Re-implement the way that matched iterables are concatenated in searches to avoid having 
  to determine the iterable type from the first element (:issue:`29`, :pull:`30`). By 
  `Dougie Squire <https://github.com/dougiesquire>`_.

0.1.1.post1
-----------

Released 15/05/2023

- Post-release due to PyPI failure partway through 0.1.1 release

0.1.1
-----

Released 12/05/2023

- Rename methods :code:`.to_subcatalog` and :code:`.to_subcatalog_dict` to :code:`.to_source` and 
  :code:`.to_source_dict` respectively and add depreciation warnings (:issue:`27`, :pull:`28`).
  By `Dougie Squire <https://github.com/dougiesquire>`_.
- Update terminology to better align with intake (:issue:`27`, :pull:`28`).
  By `Dougie Squire <https://github.com/dougiesquire>`_.
- Use :code:`load_setup_py_data` from :code:`conda-build` to template version in meta.yaml.
  By `Dougie Squire <https://github.com/dougiesquire>`_.


0.1.0
-----

Released 10/05/2023

- Initial release
