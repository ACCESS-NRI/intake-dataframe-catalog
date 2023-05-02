import ast
from io import UnsupportedOperation

import pytest

import xarray as xr
import intake
from intake.source.csv import CSVSource
from intake_esm.core import esm_datastore
import pandas as pd

from intake_dataframe_catalog.core import DfFileCatalog, DfFileCatalogError


@pytest.mark.parametrize("mode", ["r", "a", "r+"])
@pytest.mark.parametrize("kwargs", [{}, {"columns_with_iterables": ["variable"]}])
def test_load(catalog_path, mode, kwargs):
    """
    Test loading catalog from a file
    """
    path = catalog_path / "dfcat.csv"

    cat = intake.open_df_catalog(path=str(path), mode=mode, **kwargs)

    _assert_DfFileCatalog(cat)

    if mode == "r":
        with pytest.raises(UnsupportedOperation):
            cat.save()


def test_create_w(catalog_path, source_path):
    """
    Test creating a catalog with mode="w"
    """

    # Create new
    cat = intake.open_df_catalog(path=str(catalog_path / "tmp.csv"), mode="w")

    _assert_DfFileCatalog(cat, empty=True)

    # Overwrite existing
    path = catalog_path / "dfcat.csv"

    cat = intake.open_df_catalog(path=str(path), mode="w")

    _assert_DfFileCatalog(cat, empty=True)

    _add_gistemp(cat, source_path)
    _add_cesm(cat, source_path)
    _add_cmip5(cat, source_path)
    cat.save()

    cat = intake.open_df_catalog(path=str(path), mode="r")

    _assert_DfFileCatalog(cat)


def test_create_x(catalog_path, source_path):
    """
    Test creating a catalog with mode="x"
    """
    # Create new
    cat = intake.open_df_catalog(path=str(catalog_path / "tmp2.csv"), mode="x")

    _assert_DfFileCatalog(cat, empty=True)

    # Overwrite existing
    with pytest.raises(FileExistsError):
        cat = intake.open_df_catalog(path=str(catalog_path / "dfcat.csv"), mode="x")


def test_columns_with_iterables(catalog_path):
    """
    Test that columns with iterables are successfully evaluated.
    """
    cat = intake.open_df_catalog(
        str(catalog_path / "dfcat.csv"), mode="r", columns_with_iterables=["variable"]
    )

    assert all(
        isinstance(
            v,
            (
                list,
                tuple,
                set,
            ),
        )
        for v in cat.df.variable
    )
    assert "variable" in cat.columns_with_iterables


def test_read_csv_conflict(catalog_path):
    """
    Test that error is raised when `columns_with_iterables` conflicts with `read_csv_kwargs`.
    """
    # Work when inputs are consistent
    intake.open_df_catalog(
        str(catalog_path / "dfcat.csv"),
        read_kwargs={"converters": {"converters": {"variable": ast.literal_eval}}},
        columns_with_iterables=["variable"],
    )

    # Fails on conflict
    with pytest.raises(ValueError):
        intake.open_df_catalog(
            str(catalog_path / "dfcat.csv"),
            read_kwargs={"converters": {"variable": lambda x: x}},
            columns_with_iterables=["variable"],
        )


def test_catalog_unique(catalog_path):
    """
    Test unique and nunique methods
    """
    cat = intake.open_df_catalog(
        str(catalog_path / "dfcat.csv"),
        columns_with_iterables=["variable"],
    )
    uniques = cat.unique()
    nuniques = cat.nunique()
    assert isinstance(uniques, pd.Series)
    assert isinstance(nuniques, pd.Series)
    assert len(uniques.keys()) == len(cat.columns)


def test_catalog_contains(catalog_path):
    """
    Test subcat in cat operations
    """
    cat = intake.open_df_catalog(
        str(catalog_path / "dfcat.csv"),
        columns_with_iterables=["variable"],
    )
    assert "gistemp" in cat
    assert "cesm" in cat
    assert "cmip5" in cat
    assert "foo" not in cat


def test_keys(catalog_path):
    """
    Test keys method
    """
    cat = intake.open_df_catalog(
        str(catalog_path / "dfcat.csv"),
        columns_with_iterables=["variable"],
    )
    assert set(cat.keys()) == set(["gistemp", "cesm", "cmip5"])

    cat = intake.open_df_catalog(str(catalog_path / "tmp.csv"), mode="w")
    assert cat.keys() == []


@pytest.mark.parametrize(
    "query, require_all, expected_len",
    [
        (dict(realm="ocean"), False, 1),
        (dict(realm=["ocean", "ocnBgchem"]), False, 2),
        (dict(realm="atmos"), False, 2),
        (dict(realm="atmos", variable="tas"), False, 1),
        (dict(realm="atmos", variable=["tas"]), False, 1),
        (dict(variable=["NO2", "tas", "fgco2"]), False, 3),
        (dict(variable=["NO2", "tas", "fgco2"]), True, 0),
        (dict(realm="atmos", name="cesm"), False, 0),
        ({}, False, 0),
    ],
)
def test_catalog_search(catalog_path, query, require_all, expected_len):
    """
    Test search functionality
    """
    cat = intake.open_df_catalog(
        str(catalog_path / "dfcat.csv"),
        columns_with_iterables=["variable"],
    )
    new_cat = cat.search(require_all, **query)

    assert len(new_cat) == expected_len
    assert cat.columns == new_cat.columns

    if ("variable" in query) & (not require_all):
        assert all(var in query["variable"] for var in new_cat.df.variable.sum())


def test_add(catalog_path, source_path):
    """
    Test adding subcatalogs to the catalog
    """
    cat = intake.open_df_catalog(str(catalog_path / "tmp.csv"), mode="w")

    gistemp = intake.open_csv(str(source_path / "gistemp.csv"))
    gistemp.name = None

    with pytest.raises(DfFileCatalogError):
        cat.add(gistemp, metadata={"realm": "atmos", "variable": ["tas"]})

    gistemp.name = "gistemp"
    cat.add(gistemp, metadata={"realm": "atmos", "variable": ["tas"]})
    assert len(cat) == 1
    assert len(cat.df) == 1

    cat.add(gistemp, metadata={"realm": "foo", "variable": ["bar"]})
    assert len(cat) == 1
    assert len(cat.df) == 2

    cat.add(gistemp, metadata={"realm": "atmos", "variable": ["tas"]}, overwrite=True)
    assert len(cat) == 1
    assert len(cat.df) == 1

    with pytest.raises(DfFileCatalogError):
        cat.add(gistemp, metadata={"realm": "atmos", "variable": "tas"})
        cat.add(gistemp, metadata={"realm": "atmos", "foo": ["tas"]})


def test_remove(catalog_path):
    """
    Test removing subcatalogs from the catalog
    """
    cat = intake.open_df_catalog(
        str(catalog_path / "dfcat.csv"),
        columns_with_iterables=["variable"],
    )
    cat.remove("gistemp")
    assert "gistemp" not in cat

    with pytest.raises(ValueError):
        cat.remove("foo")


def test_add_remove(catalog_path, source_path):
    """
    Test adding and removing subcatalogs to the catalog
    """
    cat = intake.open_df_catalog(str(catalog_path / "tmp.csv"), mode="w")

    _assert_DfFileCatalog(cat, empty=True)

    _add_gistemp(cat, source_path)
    assert len(cat) == 1
    assert len(cat.df) == 1

    _add_cmip5(cat, source_path)
    assert len(cat) == 2
    assert len(cat.df) == 3

    _add_cesm(cat, source_path)
    assert len(cat) == 3
    assert len(cat.df) == 4

    _add_cesm(cat, source_path)
    assert len(cat) == 3
    assert len(cat.df) == 5

    _add_cesm(cat, source_path, overwrite=True)
    assert len(cat) == 3
    assert len(cat.df) == 4

    cat.remove("cesm")
    cat.remove("cmip5")
    assert len(cat) == 1
    assert len(cat.df) == 1


@pytest.mark.parametrize(
    "key, expected",
    [
        ("gistemp", CSVSource),
        ("cesm", esm_datastore),
        ("cmip5", esm_datastore),
        ("foo", None),
    ],
)
def test_catalog_getitem(catalog_path, key, expected):
    """
    Test getting subcatalogs from catalog
    """
    cat = intake.open_df_catalog(
        str(catalog_path / "dfcat.csv"),
        columns_with_iterables=["variable"],
    )

    # As key
    if expected:
        entry = cat[key]
        assert isinstance(entry, expected)
    else:
        with pytest.raises(KeyError):
            cat[key]

    # As attribute
    if expected:
        entry = getattr(cat, key)
        assert isinstance(entry, expected)
    else:
        with pytest.raises(AttributeError):
            getattr(cat, key)


@pytest.mark.parametrize(
    "method",
    ["save", "serialize"],
)
@pytest.mark.parametrize(
    "kwargs",
    [
        dict(compression={"method": "gzip"}),
        dict(compression={"method": "bz2"}),
        {},
    ],
)
def test_catalog_save(catalog_path, method, kwargs):
    """
    Test saving catalogs
    """
    path = str(catalog_path / "dfcat.csv")
    cat = intake.open_df_catalog(
        path=path,
        columns_with_iterables=["variable"],
        mode="a",
    )

    # Resave or overwrite
    if kwargs:
        path = str(catalog_path / "tmp.csv")
        getattr(cat, method)(path=path, **kwargs)
    else:
        getattr(cat, method)()
    cat_reread = intake.open_df_catalog(
        path=path,
        columns_with_iterables=["variable"],
        read_kwargs=kwargs,
    )
    pd.testing.assert_frame_equal(
        cat.df.reset_index(drop=True), cat_reread.df.reset_index(drop=True)
    )

    # Save new
    cat_subset = cat.search(variable="tas")
    getattr(cat_subset, method)(path=str(catalog_path / "tmp.csv"), **kwargs)
    cat_subset_reread = intake.open_df_catalog(
        str(catalog_path / "tmp.csv"),
        columns_with_iterables=["variable"],
        read_kwargs=kwargs,
    )
    pd.testing.assert_frame_equal(
        cat_subset.df.reset_index(drop=True),
        cat_subset_reread.df.reset_index(drop=True),
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(storage_options={}),
        {},
    ],
)
def test_to_subcatalog(catalog_path, kwargs):
    """
    Test to_subcatalog and to_subcatalog_dict methods
    """
    cat = intake.open_df_catalog(
        path=str(catalog_path / "dfcat.csv"),
        columns_with_iterables=["variable"],
        mode="a",
    )

    with pytest.raises(ValueError):
        cat.to_subcatalog(**kwargs)

    cat.to_subcatalog_dict(**kwargs)

    cat_new = cat.search(name="cesm")
    cat_new.to_subcatalog(**kwargs)


def test_empty(catalog_path):
    """
    Test warning when trying to load subcatalog from empty catalog
    """
    cat = intake.open_df_catalog(path=str(catalog_path / "tmp.csv"), mode="w")

    with pytest.warns(
        UserWarning,
        match=r"There are no subcatalogs to open. Returning an empty dictionary.",
    ):
        subcats = cat.to_subcatalog_dict()
        assert not subcats


def test_read_subcat(catalog_path):
    """
    Test reading data from subcatalogs
    """
    from distributed import Client

    cat = intake.open_df_catalog(
        path=str(catalog_path / "dfcat.csv"),
        columns_with_iterables=["variable"],
        mode="r",
    )

    x = cat.gistemp.read()
    assert isinstance(x, pd.DataFrame)

    # Force single-threaded
    with Client(threads_per_worker=1):
        x = cat.cesm.to_dask(xarray_open_kwargs={"chunks": {}}, progressbar=False)
    assert isinstance(x, xr.Dataset)

    with Client(threads_per_worker=1):
        x = cat.cmip5.to_dataset_dict(
            xarray_open_kwargs={"chunks": {}}, progressbar=False
        )
    assert len(x) == len(cat.cmip5)


def test_subclassing_catalog(catalog_path):
    """
    Test subclassing DfFileCatalog
    """

    class ChildCatalog(DfFileCatalog):
        pass

    cat = ChildCatalog(
        path=str(catalog_path / "dfcat.csv"),
        columns_with_iterables=["variable"],
        mode="r",
    )
    scat = cat.search(variable="tas")
    assert type(scat) is ChildCatalog


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
        assert len(cat) == 0
    else:
        assert not cat.df.empty
        assert len(cat) > 0
    assert isinstance(cat.columns_with_iterables, list)

    assert "catalog with" in repr(cat)


def _add_gistemp(cat, source_path):
    """
    Add GISTEMP csv file to catalog
    """
    gistemp = intake.open_csv(str(source_path / "gistemp.csv"))
    gistemp.name = "gistemp"
    cat.add(gistemp, metadata={"realm": "atmos", "variable": ["tas"]})
    return cat


def _add_cesm(cat, source_path, overwrite=False):
    """
    Add CESM intake-esm catalog to catalog
    """
    cesm = intake.open_esm_datastore(
        str(source_path / "cesm.json"), columns_with_iterables=["variable"]
    )
    cesm.name = "cesm"
    cat.add(
        cesm,
        metadata={"realm": "ocean", "variable": list(set(cesm.df.variable.sum()))},
        overwrite=overwrite,
    )
    return cat


def _add_cmip5(cat, source_path):
    """
    Add CMIP5 intake-esm catalog to catalog
    """
    cmip5 = intake.open_esm_datastore(
        str(source_path / "cmip5.json"),
    )
    cmip5.name = "cmip5"
    for realm in cmip5.df.modeling_realm.unique():
        variable = list(
            cmip5.df.loc[cmip5.df.modeling_realm == realm].variable.unique()
        )
        cat.add(cmip5, metadata={"realm": realm, "variable": variable})
    return cat
