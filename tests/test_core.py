import pytest

import pandas as pd

from intake_dataframe_catalog.core import DfFileCatalog


@pytest.mark.parametrize("mode", ["r", "a", "r+"])
def test_load(datadir, mode):
    path = datadir.join("dfcat_multi_source.csv")
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
