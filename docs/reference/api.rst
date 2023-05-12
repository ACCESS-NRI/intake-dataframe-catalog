.. _api:

API reference
=============

Excluding some supporting functions, intake_dataframe_catalog comprises a just single class, :code:`intake_dataframe_catalog.core.DfFileCatalog`, for loading/creating/managing a table of intake sources. This class is registered as an `intake driver <https://intake.readthedocs.io/en/latest/making-plugins.html#making-drivers>`_ called :code:`df_catalog`, meaning that intake will automatically create an :code:`open_df_catalog` convenience function under the intake module namespace. Calling :code:`intake.open_df_catalog()` is equivalent to instantiating the :code:`DfFileCatalog` class.

The following API summary is auto-generated.

.. autoclass:: intake_dataframe_catalog.core.DfFileCatalog
    :members:
    :noindex:
    :special-members: __init__
