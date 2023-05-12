# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

import ast
from io import UnsupportedOperation

import intake
import pandas as pd
import pytest
import xarray as xr
from intake.source.csv import CSVSource
from intake_esm.core import esm_datastore

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
        with pytest.raises(UnsupportedOperation) as excinfo:
            cat.save()
        assert "Cannot save catalog initialised with mode='r'" in str(excinfo.value)


def test_create_w(catalog_path, source_path):
    """
    Test creating a catalog with mode="w"
    """

    # Create new
    cat = intake.open_df_catalog(
        path=str(catalog_path / "tmp.csv"),
        yaml_column="foo",
        name_column="bar",
        mode="w",
    )

    _assert_DfFileCatalog(cat, empty=True)

    # Overwrite existing
    path = catalog_path / "dfcat.csv"

    cat = intake.open_df_catalog(path=str(path), mode="w")

    _assert_DfFileCatalog(cat, empty=True)

    _add_gistemp(cat, source_path)
    _add_cesm(cat, source_path)
    _add_cmip5(cat, source_path)
    _add_cmip6(cat, source_path)
    cat.save()

    cat = intake.open_df_catalog(path=str(path), mode="r")

    _assert_DfFileCatalog(cat)


def test_create_x(catalog_path, source_path):
    """
    Test creating a catalog with mode="x"
    """
    # Create new
    cat = intake.open_df_catalog(
        path=str(catalog_path / "tmp2.csv"),
        yaml_column="foo",
        name_column="bar",
        mode="x",
    )

    _assert_DfFileCatalog(cat, empty=True)

    # Overwrite existing
    with pytest.raises(FileExistsError):
        cat = intake.open_df_catalog(path=str(catalog_path / "dfcat.csv"), mode="x")


def test_column_name_error(catalog_path):
    """
    Test that error message is thrown with yaml_column/name_column are not in catalog
    """
    with pytest.raises(DfFileCatalogError) as excinfo:
        intake.open_df_catalog(
            str(catalog_path / "dfcat.csv"),
            yaml_column="foo",
            mode="r",
        )
    assert (
        "Please provide the name of the column containing the intake source YAML descriptions"
        in str(excinfo.value)
    )

    with pytest.raises(DfFileCatalogError) as excinfo:
        intake.open_df_catalog(
            str(catalog_path / "dfcat.csv"),
            name_column="bar",
            mode="r",
        )
    assert (
        "Please provide the name of the column containing the intake source names"
        in str(excinfo.value)
    )


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
    with pytest.raises(ValueError) as excinfo:
        intake.open_df_catalog(
            str(catalog_path / "dfcat.csv"),
            read_kwargs={"converters": {"variable": lambda x: x}},
            columns_with_iterables=["variable"],
        )
    assert "Cannot provide converter" in str(excinfo.value)


@pytest.mark.parametrize(
    "query, expected_unique, expected_nunique",
    [
        (
            None,
            {
                "realm": ["atmos", "ocean", "ocnBgchem", "land"],
                "variable": [
                    "tas",
                    "O2",
                    "SiO3",
                    "DXU",
                    "PO4",
                    "SHF",
                    "ANGLE",
                    "NO2",
                    "TEMP",
                    "REGION_MASK",
                    "KMT",
                    "fgco2",
                    "hfls",
                    "tasmax",
                    "prsn",
                    "gpp",
                    "residualFrac",
                ],
                "name": ["gistemp", "cesm", "cmip5", "cmip6"],
            },
            {"realm": 4, "variable": 17, "name": 4},
        ),
        (
            {"realm": "atmos"},
            {
                "realm": ["atmos"],
                "variable": ["tas", "hfls", "tasmax", "prsn"],
                "name": ["gistemp", "cmip5", "cmip6"],
            },
            {"realm": 1, "variable": 4, "name": 3},
        ),
        (
            {"variable": "tas"},
            {"realm": ["atmos"], "variable": ["tas"], "name": ["gistemp"]},
            {"realm": 1, "variable": 1, "name": 1},
        ),
        (
            {"variable": "foo"},
            {"realm": [], "variable": [], "name": []},
            {"realm": 0, "variable": 0, "name": 0},
        ),
    ],
)
def test_catalog_unique(catalog_path, query, expected_unique, expected_nunique):
    """
    Test unique and nunique methods
    """
    cat = intake.open_df_catalog(
        str(catalog_path / "dfcat.csv"),
        columns_with_iterables=["variable"],
    )
    if query:
        cat = cat.search(**query)

    unique = cat.unique().to_dict()
    assert unique.keys() == expected_unique.keys()
    for key in unique.keys():
        assert set(unique[key]) == set(expected_unique[key])

    nunique = cat.nunique().to_dict()
    assert nunique == expected_nunique


def test_catalog_contains(catalog_path):
    """
    Test source in cat operations
    """
    cat = intake.open_df_catalog(
        str(catalog_path / "dfcat.csv"),
        columns_with_iterables=["variable"],
    )
    assert "gistemp" in cat
    assert "cesm" in cat
    assert "cmip5" in cat
    assert "foo" not in cat


def test_catalog_keys(catalog_path):
    """
    Test keys method
    """
    cat = intake.open_df_catalog(
        str(catalog_path / "dfcat.csv"),
        columns_with_iterables=["variable"],
    )
    assert set(cat.keys()) == set(["gistemp", "cesm", "cmip5", "cmip6"])

    cat = intake.open_df_catalog(str(catalog_path / "tmp.csv"), mode="w")
    assert cat.keys() == []


@pytest.mark.parametrize(
    "query, require_all, expected_len",
    [
        ({"realm": "ocean"}, False, 1),
        ({"realm": ["atmos", "ocnBgchem"]}, False, 3),
        ({"realm": ["atmos", "ocnBgchem"]}, True, 1),
        ({"realm": "atmos"}, False, 3),
        ({"realm": "atmos", "variable": "tas"}, False, 1),
        ({"realm": "atmos", "variable": ["tas"]}, False, 1),
        ({"variable": ["NO2", "tas", "fgco2"]}, False, 3),
        ({"variable": ["NO2", "tas", "fgco2"]}, True, 0),
        ({"name": ["cesm", "cmip5"]}, False, 2),
        ({"name": ["cesm", "cmip5"]}, True, 0),
        ({"realm": "atmos", "name": "cesm"}, False, 0),
        ({}, False, 4),
        ({}, True, 4),
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
    if expected_len:
        assert cat.columns == new_cat.columns

    if ("variable" in query) & (not require_all):
        assert all(var in query["variable"] for var in new_cat.df.variable.sum())


def test_bad_search(catalog_path):
    """
    Test search on non-existent column
    """
    cat = intake.open_df_catalog(
        str(catalog_path / "dfcat.csv"),
        columns_with_iterables=["variable"],
    )

    with pytest.raises(ValueError) as excinfo:
        cat.search(foo="bar")
    assert "Column 'foo' not in columns" in str(excinfo.value)


def test_catalog_add(catalog_path, source_path):
    """
    Test adding sources to the catalog
    """
    cat = intake.open_df_catalog(str(catalog_path / "tmp.csv"), mode="w")

    gistemp = intake.open_csv(str(source_path / "gistemp.csv"))
    gistemp.name = None

    with pytest.raises(DfFileCatalogError) as excinfo:
        cat.add(gistemp, metadata={"realm": "atmos", "variable": ["tas"]})
    assert "Cannot add an unnamed source to the dataframe catalog" in str(excinfo.value)

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

    with pytest.raises(DfFileCatalogError) as excinfo:
        cat.add(gistemp, metadata={"realm": "atmos", "variable": "tas"})
    assert "Cannot add entry with iterable metadata columns" in str(excinfo.value)

    with pytest.raises(DfFileCatalogError) as excinfo:
        cat.add(gistemp, metadata={"foo": "bar", "variable": ["tas"]})
    assert "metadata must include the following keys" in str(excinfo.value)


def test_catalog_remove(catalog_path):
    """
    Test removing sources from the catalog
    """
    cat = intake.open_df_catalog(
        str(catalog_path / "dfcat.csv"),
        columns_with_iterables=["variable"],
    )
    cat.remove("gistemp")
    assert "gistemp" not in cat

    with pytest.raises(ValueError) as excinfo:
        cat.remove("foo")
    assert "'foo' is not an entry" in str(excinfo.value)


def test_catalog_add_remove(catalog_path, source_path):
    """
    Test adding and removing sources to the catalog
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


def test_use_metadata_name(catalog_path, source_path):
    """
    Test that if name is specified in the metadata it is used preferentially over
    the catalog name
    """
    cat = intake.open_df_catalog(
        str(catalog_path / "tmp.csv"),
        mode="w",
    )
    source = intake.open_csv(str(source_path / "gistemp.csv"))
    source.name = "gistemp"

    cat.add(source, metadata={"name": "new_name"})
    assert "new_name" in cat
    assert "gistemp" not in cat


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
    Test getting sources from catalog
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
        with pytest.raises(KeyError) as excinfo:
            cat[key]
        assert f"key='{key}' not found in catalog" in str(excinfo.value)

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
        {"compression": {"method": "gzip"}},
        {"compression": {"method": "bz2"}},
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
        {"storage_options": {}},
        {},
    ],
)
def test_to_source(catalog_path, kwargs):
    """
    Test to_source and to_source_dict methods
    """
    cat = intake.open_df_catalog(
        path=str(catalog_path / "dfcat.csv"),
        columns_with_iterables=["variable"],
        mode="a",
    )

    with pytest.raises(ValueError) as excinfo:
        cat.to_source(**kwargs)
    assert "Expected exactly one source" in str(excinfo.value)

    cat.to_source_dict(**kwargs)

    cat_new = cat.search(name="cesm")
    cat_new.to_source(**kwargs)


def test_empty_catalog(catalog_path):
    """
    Test warning when trying to load source from empty catalog
    """
    cat = intake.open_df_catalog(path=str(catalog_path / "tmp.csv"), mode="w")

    with pytest.warns(
        UserWarning,
        match=r"There are no sources to open. Returning an empty dictionary.",
    ):
        sources = cat.to_source_dict()
        assert not sources


def test_read_source(catalog_path):
    """
    Test reading data from sources
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


@pytest.mark.parametrize(
    "metadata, expected_dict",
    [
        (
            [
                {"meta0": "a", "meta1": "b"},
                {"meta0": "b", "meta1": "c"},
            ],
            {"columns": ["meta0", "meta1"], "data": [[{"a", "b"}, {"b", "c"}]]},
        ),
        (
            [
                {
                    "meta0": "a",
                    "meta1": [
                        "b",
                    ],
                },
                {
                    "meta0": "a",
                    "meta1": [
                        "c",
                    ],
                },
            ],
            {"columns": ["meta0", "meta1"], "data": [[{"a"}, {"b", "c"}]]},
        ),
        (
            [
                {"meta0": 0, "meta1": ("b",)},
                {"meta0": 1, "meta1": ("b",)},
            ],
            {"columns": ["meta0", "meta1"], "data": [[{0, 1}, {"b"}]]},
        ),
        (
            [
                {
                    "meta0": {
                        1.0,
                    },
                    "meta1": {
                        "b",
                    },
                },
                {
                    "meta0": {
                        1.0,
                    },
                    "meta1": {
                        "c",
                    },
                },
            ],
            {"columns": ["meta0", "meta1"], "data": [[{1.0}, {"b", "c"}]]},
        ),
    ],
)
def test_df_summary(catalog_path, source_path, metadata, expected_dict):
    """
    Test expected output of df_summary
    """
    cat = intake.open_df_catalog(
        str(catalog_path / "tmp.csv"),
        mode="w",
    )
    source = intake.open_csv(str(source_path / "gistemp.csv"))

    for meta in metadata:
        cat.add(source, metadata=meta)

    assert cat.df_summary.to_dict(orient="split", index=False) == expected_dict


def test_df_summary_update(catalog_path, source_path):
    """
    Test that df_summary updates correctly
    """
    cat = intake.open_df_catalog(
        str(catalog_path / "tmp.csv"),
        mode="w",
    )
    source = intake.open_csv(str(source_path / "gistemp.csv"))
    source.name = "gistemp"

    expected_dict = {"columns": [], "data": []}
    assert cat.df_summary.to_dict(orient="split", index=False) == expected_dict

    cat.add(source, metadata={"meta0": "a", "meta1": "b"})
    expected_dict = {"columns": ["meta0", "meta1"], "data": [[{"a"}, {"b"}]]}
    assert cat.df_summary.to_dict(orient="split", index=False) == expected_dict

    cat.add(source, metadata={"meta0": "c", "meta1": "d"})
    expected_dict = {"columns": ["meta0", "meta1"], "data": [[{"a", "c"}, {"b", "d"}]]}
    assert cat.df_summary.to_dict(orient="split", index=False) == expected_dict

    cat.add(source, metadata={"meta0": "c", "meta1": "d"}, overwrite=True)
    expected_dict = {"columns": ["meta0", "meta1"], "data": [[{"c"}, {"d"}]]}
    assert cat.df_summary.to_dict(orient="split", index=False) == expected_dict

    cat.remove("gistemp")
    expected_dict = {"columns": [], "data": []}
    assert cat.df_summary.to_dict(orient="split", index=False) == expected_dict


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


def _add_cmip6(cat, source_path):
    """
    Add CMIP6 intake-esm catalog to catalog
    """
    cmip6 = intake.open_esm_datastore(
        str(source_path / "cmip6.json"),
    )
    cmip6.name = "cmip6"
    for table_id in cmip6.df.table_id.unique():
        if table_id == "Amon":
            realm = "atmos"
        elif table_id == "Lmon":
            realm = "land"
        variable = list(
            cmip6.df.loc[cmip6.df.table_id == table_id].variable_id.unique()
        )
        cat.add(cmip6, metadata={"realm": realm, "variable": variable})
    return cat
