.. _changelog:

Changelog
=========

0.2.0
-----

Release 17/05/2023

- Remove :code:`to_subcatalog` and :code:`to_subcatalog_dict` methods (:issue:`31`, :pull:`32`). 
  By `Dougie Squire <https://github.com/dougiesquire>`_.
- Re-implement the way that matched iterables are concatenated in searches to avoid having 
  to determine the iterable type from the first element (:issue:`29`, :pull:`30`). By 
  `Dougie Squire <https://github.com/dougiesquire>`_.

0.1.1.post1
-----------

Release 15/05/2023

- Post-release due to PyPI failure partway through 0.1.1 release

0.1.1
-----

Released 12/05/2023

- Rename methods :code:`to_subcatalog` and :code:`to_subcatalog_dict` to :code:`to_source` and 
  :code:`to_source_dict` respectively and add depreciation warnings (:issue:`27`, :pull:`28`).
  By `Dougie Squire <https://github.com/dougiesquire>`_.
- Update terminology to better align with intake (:issue:`27`, :pull:`28`).
  By `Dougie Squire <https://github.com/dougiesquire>`_.
- Use :code:`load_setup_py_data` from :code:`conda-build` to template version in meta.yaml.
  By `Dougie Squire <https://github.com/dougiesquire>`_.


0.1.0
-----

Released 10/05/2023

- Initial release
