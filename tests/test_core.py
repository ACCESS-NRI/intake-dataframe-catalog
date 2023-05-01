import pytest

import pandas as pd

from intake_dataframe_catalog.core import DfFileCatalog


@pytest.mark.parametrize("mode", ["r", "a", "r+"])
def test_load(data_path, mode):
    """
    Test loading table from a file
    """
    path = data_path / "dfcat.csv"

    cat = DfFileCatalog(
        path=path,
        mode=mode,
    )

    if mode == "r":
        with pytest.raises(ValueError):
            cat.save()

    assert isinstance(cat, DfFileCatalog)
    assert isinstance(cat.df, pd.DataFrame)
    assert not cat.df.empty
    assert isinstance(cat.df_summary, pd.DataFrame)
    assert isinstance(cat.columns, list)
    assert isinstance(cat.columns_with_iterables, list)
