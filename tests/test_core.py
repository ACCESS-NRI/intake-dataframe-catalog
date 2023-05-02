from io import UnsupportedOperation

import pytest

import intake
import pandas as pd

from intake_dataframe_catalog.core import DfFileCatalog


@pytest.mark.parametrize("mode", ["r", "a", "r+"])
@pytest.mark.parametrize("kwargs", [{}, {"columns_with_iterables": ["variable"]}])
def test_load(catalog_path, mode, kwargs):
    """
    Test loading catalog from a file
    """
    path = catalog_path / "dfcat.csv"

    cat = DfFileCatalog(path=path, mode=mode, **kwargs)

    _assert_DfFileCatalog(cat)

    if mode == "r":
        with pytest.raises(UnsupportedOperation):
            cat.save()


def test_create_w(catalog_path, source_path):
    """
    Test creating a catalog with mode="w"
    """

    # Create new
    cat = DfFileCatalog(path=catalog_path / "cat0.csv", mode="w")

    _assert_DfFileCatalog(cat, empty=True)

    # Overwrite existing
    path = catalog_path / "dfcat.csv"

    cat = DfFileCatalog(path=path, mode="w")

    _assert_DfFileCatalog(cat, empty=True)

    _add_gistemp(cat, source_path)
    _add_cesm(cat, source_path)
    _add_cmip5(cat, source_path)
    cat.save()

    cat = DfFileCatalog(path=path, mode="r")

    _assert_DfFileCatalog(cat)


def test_create_x(catalog_path, source_path):
    """
    Test creating a catalog with mode="x"
    """
    # Create new
    cat = DfFileCatalog(path=catalog_path / "cat1.csv", mode="x")

    _assert_DfFileCatalog(cat, empty=True)

    # Overwrite existing
    with pytest.raises(FileExistsError):
        cat = DfFileCatalog(path=catalog_path / "dfcat.csv", mode="x")


def _assert_DfFileCatalog(cat, empty=False):
    """
    Assert that the input is a DfFileCatalog
    """
    assert isinstance(cat, DfFileCatalog)
    assert isinstance(cat.df, pd.DataFrame)
    assert isinstance(cat.df_summary, pd.DataFrame)
    assert isinstance(cat.columns, list)
    if empty:
        assert cat.df.empty
    else:
        assert not cat.df.empty
    assert isinstance(cat.columns_with_iterables, list)


def _add_gistemp(cat, source_path):
    """
    Add GISTEMP csv file to catalog
    """
    gistemp = intake.open_csv(source_path / "gistemp.csv")
    gistemp.name = "gistemp"
    cat.add(gistemp, metadata={"realm": "atmos", "variable": ["tas"]})
    return cat


def _add_cesm(cat, source_path):
    """
    Add CESM intake-esm catalog to catalog
    """
    cesm = intake.open_esm_datastore(
        source_path / "cesm.json", columns_with_iterables=["variable"]
    )
    cesm.name = "cesm"
    cat.add(
        cesm, metadata={"realm": "ocean", "variable": list(set(cesm.df.variable.sum()))}
    )
    return cat


def _add_cmip5(cat, source_path):
    """
    Add CMIP5 intake-esm catalog to catalog
    """
    cmip5 = intake.open_esm_datastore(
        source_path / "cmip5.json",
    )
    cmip5.name = "cmip5"
    for realm in cmip5.df.modeling_realm.unique():
        variable = list(
            cmip5.df.loc[cmip5.df.modeling_realm == realm].variable.unique()
        )
        cat.add(cmip5, metadata={"realm": realm, "variable": variable})
    return cat
