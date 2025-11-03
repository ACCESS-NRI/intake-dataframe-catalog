# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0


import polars as pl
import pytest
from polars.testing import assert_frame_equal as pl_assert_frame_equal

from intake_dataframe_catalog._search import (
    _filter_iter_qcols_on_name,
    _group_and_filter_on_index,
    _match_and_filter,
    _promote_query_qcols,
)

"""
Tests in this file are extracted from other parametrisations, so don't really add
any extra coverage, but are helpful for documenting the intent of the helper
functions.

Best thought of as snapshot tests, generated from other tests using the claude to
extract the relevant parametrisations.
"""


@pytest.mark.parametrize(
    "lf, name_column, all_cols, tmp_cols, expected",
    [
        (
            pl.LazyFrame(
                schema={
                    "index": pl.UInt32,
                    "realm": pl.Utf8,
                    "variable": pl.Utf8,
                    "name": pl.Utf8,
                    "yaml": pl.Utf8,
                    "variable_matches": pl.List(pl.Utf8),
                }
            ),
            "name",
            ["realm", "variable", "name", "yaml"],
            ["variable_matches"],
            pl.LazyFrame(
                schema={
                    "realm": pl.List(pl.Utf8),
                    "variable": pl.List(pl.Utf8),
                    "name": pl.Utf8,
                    "yaml": pl.List(pl.Utf8),
                    "variable_matches": pl.List(pl.Utf8),
                }
            ),
        ),
        (
            pl.LazyFrame(
                {
                    "index": [0],
                    "realm": ["atmos"],
                    "variable": ["tas"],
                    "name": ["gistemp"],
                    "yaml": ["sources:\n  gistemp:\n    args:\n..."],
                    "variable_matches": [["tas"]],
                }
            ),
            "name",
            ["realm", "variable", "name", "yaml"],
            ["variable_matches"],
            pl.LazyFrame(
                {
                    "realm": [["atmos"]],
                    "variable": [["tas"]],
                    "name": ["gistemp"],
                    "yaml": [["sources:\n  gistemp:\n    args:\n..."]],
                    "variable_matches": [["tas"]],
                }
            ),
        ),
        (
            pl.LazyFrame(
                {
                    "index": [0, 3, 3, 4, 4],
                    "realm": ["atmos", "atmos", "atmos", "atmos", "atmos"],
                    "variable": ["tas", "hfls", "tasmax", "tasmax", "prsn"],
                    "name": ["gistemp", "cmip5", "cmip5", "cmip6", "cmip6"],
                    "yaml": [
                        "sources:\n  gistemp:\n    args:\n...",
                        "sources:\n  cmip5:\n    args:\n...",
                        "sources:\n  cmip5:\n    args:\n...",
                        "sources:\n  cmip6:\n    args:\n...",
                        "sources:\n  cmip6:\n    args:\n...",
                    ],
                    "realm_matches": [
                        ["atmos"],
                        ["atmos"],
                        ["atmos"],
                        ["atmos"],
                        ["atmos"],
                    ],
                }
            ),
            "name",
            ["realm", "variable", "name", "yaml"],
            ["realm_matches"],
            pl.LazyFrame(
                {
                    "realm": [["atmos"], ["atmos"], ["atmos"]],
                    "variable": [["tas"], ["hfls", "tasmax"], ["tasmax", "prsn"]],
                    "name": ["gistemp", "cmip5", "cmip6"],
                    "yaml": [
                        ["sources:\n  gistemp:\n    args:\n..."],
                        ["sources:\n  cmip5:\n    args:\n..."],
                        ["sources:\n  cmip6:\n    args:\n..."],
                    ],
                    "realm_matches": [["atmos"], ["atmos"], ["atmos"]],
                }
            ),
        ),
        (
            pl.LazyFrame(
                {
                    "index": [0, 0, 1, 1, 2],
                    "realm": ["atmos", "atmos", None, "ocean", "land"],
                    "variable": ["tas", None, "salt", "salt", "soiltemp"],
                    "name": ["exp1", "exp1", "exp2", "exp2", "exp3"],
                    "yaml": ["file1.yaml", None, "file2.yaml", "file2.yaml", None],
                    "variable_matches": [
                        ["tas"],
                        None,
                        ["salt"],
                        ["salt"],
                        ["soiltemp"],
                    ],
                }
            ),
            "name",
            ["realm", "variable", "name", "yaml"],
            ["variable_matches"],
            pl.LazyFrame(
                {
                    "realm": [["atmos"], ["ocean"], ["land"]],
                    "variable": [["tas"], ["salt"], ["soiltemp"]],
                    "name": ["exp1", "exp2", "exp3"],
                    "yaml": [["file1.yaml"], ["file2.yaml"], []],
                    "variable_matches": [["tas"], ["salt"], ["soiltemp"]],
                }
            ),
        ),
    ],
)
def test__group_and_filter_on_index(lf, name_column, all_cols, tmp_cols, expected):
    """
    Test our _group_and_filter_on_index function. We set `check_row_order=False`
    because the grouping may not preserve order.
    """
    output = _group_and_filter_on_index(lf, name_column, all_cols, tmp_cols)
    pl_assert_frame_equal(output.collect(), expected.collect(), check_row_order=False)


@pytest.mark.parametrize(
    "lf, query, expected_lf, expected_tmp_cols",
    [
        (
            pl.LazyFrame(
                {
                    "index": [0, 1, 2, 3],
                    "realm": ["atmos", "ocean", "ocnBgchem", "atmos"],
                    "variable": [
                        "['tas']",
                        "['NO2', 'SHF', 'ANGLE', 'REGIO…']",
                        "['fgco2']",
                        "['hfls', 'tasmax']",
                    ],
                    "name": ["gistemp", "cesm", "cmip5", "cmip5"],
                    "yaml": [
                        "sources:\n  gistemp:\n    args:\n…",
                        "sources:\n  cesm:\n    args:\n   …",
                        "sources:\n  cmip5:\n    args:\n  …",
                        "sources:\n  cmip5:\n    args:\n  …",
                    ],
                }
            ),
            {"variable": ["tas"]},
            pl.LazyFrame(
                schema={
                    "index": pl.Int64,
                    "realm": pl.Utf8,
                    "variable": pl.Utf8,
                    "name": pl.Utf8,
                    "yaml": pl.Utf8,
                    "variable_matches": pl.List(pl.Utf8),
                }
            ),
            ["variable_matches"],
        ),
        (
            pl.LazyFrame(
                {
                    "index": [0, 1, 2, 3, 4, 5],
                    "A": ["aaa", "aaa", "aba", "aba", "abA", "abA"],
                    "B": ["a", "b", "c", "a", "b", "c"],
                    "C": [0, 1, 2, 3, 4, 5],
                }
            ),
            {"B": ["^a$"], "C": [0, 3]},
            pl.LazyFrame(
                {
                    "index": [0, 3],
                    "A": ["aaa", "aba"],
                    "B": ["a", "a"],
                    "C": [0, 3],
                    "B_matches": [["^a$"], ["^a$"]],
                    "C_matches": [[0, None], [None, 3]],
                },
                schema={
                    "index": pl.Int64,
                    "A": pl.Utf8,
                    "B": pl.Utf8,
                    "C": pl.Int64,
                    "B_matches": pl.List(pl.Utf8),
                    "C_matches": pl.List(pl.Int32),
                },  # Necessary for some reason to specify schema as pl picks i32 here
            ),
            ["B_matches", "C_matches"],
        ),
    ],
)
def test__match_and_filter(lf, query, expected_lf, expected_tmp_cols):
    output_lf, tmp_cols = _match_and_filter(lf, query)

    pl_assert_frame_equal(output_lf.collect(), expected_lf.collect())
    assert tmp_cols == expected_tmp_cols


@pytest.mark.parametrize(
    "lf, query, name_column, agg_cols, iterable_qcols, group_on_names, expected",
    [
        (
            pl.LazyFrame(
                {
                    "A": ["cat0", "cat1", "cat1"],
                    "B": [["a", "b"], ["a", "b"], ["a"]],
                    "C": [["cx", "cy"], ["cx", "cz"], ["cz", "cy"]],
                    "D": [[0], [0], [0]],
                    "E": [["xxx"], ["xxx"], ["yyy"]],
                    "B_matches": [["a", "b"], ["a", "b"], ["a"]],
                    "D_matches": [[0], [0], [0]],
                }
            ),
            {"B": ["a", "b"], "D": [0]},
            "A",
            {"B", "D_matches", "D", "E", "C", "B_matches"},
            {"B", "D"},
            False,
            pl.LazyFrame(
                {
                    "A": ["cat0", "cat1", "cat1"],
                    "B": [["a", "b"], ["a", "b"], ["a"]],
                    "C": [["cx", "cy"], ["cx", "cz"], ["cz", "cy"]],
                    "D": [[0], [0], [0]],
                    "E": [["xxx"], ["xxx"], ["yyy"]],
                    "B_matches": [["a", "b"], ["a", "b"], ["a"]],
                    "D_matches": [[0], [0], [0]],
                }
            ),
        ),
        (
            pl.LazyFrame(
                {
                    "A": ["cat0", "cat1", "cat1"],
                    "B": [["a", "b"], ["a", "b"], ["a"]],
                    "C": [["cx", "cy"], ["cx", "cz"], ["cz", "cy"]],
                    "D": [[0, 1], [0], [0, 1]],
                    "E": [["xxx"], ["xxx"], ["yyy"]],
                    "B_matches": [["a", "b"], ["a", "b"], ["a"]],
                    "D_matches": [[0, 1], [0], [0, 1]],
                }
            ),
            {"B": ["a", "b"], "D": [0, 1]},
            "A",
            {"D_matches", "C", "D", "B_matches", "E", "B"},
            {"B", "D"},
            False,
            pl.LazyFrame(
                {
                    "A": ["cat0"],
                    "B": [["a", "b"]],
                    "C": [["cx", "cy"]],
                    "D": [[0, 1]],
                    "E": [["xxx"]],
                    "B_matches": [["a", "b"]],
                    "D_matches": [[0, 1]],
                }
            ),
        ),
        (
            pl.LazyFrame(
                {
                    "realm": [["atmos"], ["ocnBgchem"], ["atmos"], ["atmos"]],
                    "variable": [
                        ["tas"],
                        ["fgco2"],
                        ["hfls", "tasmax"],
                        ["tasmax", "prsn"],
                    ],
                    "name": ["gistemp", "cmip5", "cmip5", "cmip6"],
                    "yaml": [
                        ["sources:\n  gistemp:\n    args:\n..."],
                        ["sources:\n  cmip5:\n    args:\n..."],
                        ["sources:\n  cmip5:\n    args:\n..."],
                        ["sources:\n  cmip6:\n    args:\n..."],
                    ],
                    "realm_matches": [["atmos"], ["ocnBgchem"], ["atmos"], ["atmos"]],
                }
            ),
            {"realm": ["atmos", "ocnBgchem"]},
            "name",
            {"realm", "yaml", "realm_matches", "variable"},
            {"realm"},
            True,
            pl.LazyFrame(
                {
                    "realm": [["ocnBgchem"], ["atmos"]],
                    "variable": [["fgco2"], ["hfls", "tasmax"]],
                    "name": ["cmip5", "cmip5"],
                    "yaml": [
                        ["sources:\n  cmip5:\n    args:\n..."],
                        ["sources:\n  cmip5:\n    args:\n..."],
                    ],
                    "realm_matches": [["ocnBgchem"], ["atmos"]],
                }
            ),
        ),
        (
            pl.LazyFrame(
                {
                    "name": ["exp1", "exp2", "exp3"],
                    "variable": [["tas", "pr"], ["tas"], ["pr", "hfls"]],
                    "realm": [["atmos"], ["atmos"], ["atmos"]],
                    "variable_matches": [["tas", "pr"], ["tas"], []],
                }
            ),
            {"variable": ["tas", "pr"]},  # Single iterable query column
            "name",
            {"variable", "realm", "variable_matches"},
            {"variable"},  # Single column in iterable_qcols
            False,
            pl.LazyFrame(
                {
                    "name": ["exp1"],
                    "variable": [["tas", "pr"]],
                    "realm": [["atmos"]],
                    "variable_matches": [["tas", "pr"]],
                }
            ),
        ),
        (
            # Edge case: Empty matches - all rows have empty match lists
            pl.LazyFrame(
                {
                    "name": ["exp1", "exp2", "exp3"],
                    "variable": [["tas"], ["pr"], ["hfls"]],
                    "realm": [["atmos"], ["ocean"], ["land"]],
                    "variable_matches": [[], [], []],  # All empty matches
                    "realm_matches": [[], [], []],  # All empty matches
                }
            ),
            {"variable": ["tas", "pr"], "realm": ["atmos"]},
            "name",
            {"variable", "realm", "variable_matches", "realm_matches"},
            {"variable", "realm"},
            False,
            pl.LazyFrame(
                schema={
                    "name": pl.Utf8,
                    "variable": pl.List(pl.Utf8),
                    "realm": pl.List(pl.Utf8),
                    "variable_matches": pl.List(pl.Null),
                    "realm_matches": pl.List(pl.Null),
                }
            ),  # Empty LazyFrame with correct schema
        ),
        (
            # Edge case: No rows satisfy the length requirement
            pl.LazyFrame(
                {
                    "name": ["exp1", "exp2", "exp3"],
                    "variable": [["tas"], ["pr"], ["hfls"]],
                    "realm": [["atmos"], ["ocean"], ["land"]],
                    "variable_matches": [
                        ["tas"],
                        ["pr"],
                        ["hfls"],
                    ],  # Only 1 match each
                    "realm_matches": [
                        ["atmos"],
                        ["ocean"],
                        ["land"],
                    ],  # Only 1 match each
                }
            ),
            {
                "variable": ["tas", "pr", "hfls"],
                "realm": ["atmos", "ocean"],
            },  # Requires 3 and 2 matches respectively
            "name",
            {"variable", "realm", "variable_matches", "realm_matches"},
            {"variable", "realm"},
            False,
            pl.LazyFrame(
                schema={
                    "name": pl.Utf8,
                    "variable": pl.List(pl.Utf8),
                    "realm": pl.List(pl.Utf8),
                    "variable_matches": pl.List(pl.Utf8),
                    "realm_matches": pl.List(pl.Utf8),
                }
            ),
        ),
    ],
)
def test__filter_iter_qcols_on_name(
    lf: pl.LazyFrame,
    query: dict[str, list],
    name_column: str,
    agg_cols: set[str],
    iterable_qcols: set[str],
    group_on_names: bool,
    expected: pl.LazyFrame,
):
    """
    Test our _filter_iter_qcols_on_name function.
    """
    output = _filter_iter_qcols_on_name(
        lf, query, name_column, agg_cols, iterable_qcols, group_on_names
    )
    pl_assert_frame_equal(output.collect(), expected.collect(), check_row_order=False)


@pytest.mark.parametrize(
    "lf, query, columns_with_iterables, all_cols, cols_to_deiter, expected_lf, expected_iterable_cols, expected_all_cols, expected_cols_to_deiter",
    [
        (
            pl.LazyFrame(
                {
                    "realm": ["atmos", "ocean", "ocnBgchem", "atmos", "atmos", "land"],
                    "variable": [
                        ["tas"],
                        ["PO4", "KMT", "SHF"],
                        ["fgco2"],
                        ["hfls", "tasmax"],
                        ["tasmax", "prsn"],
                        ["gpp", "residualFrac"],
                    ],
                    "name": ["gistemp", "cesm", "cmip5", "cmip5", "cmip6", "cmip6"],
                    "yaml": [
                        "sources:\n  gistemp:\n    args:\n…",
                        "sources:\n  cesm:\n    args:\n   …",
                        "sources:\n  cmip5:\n    args:\n  …",
                        "sources:\n  cmip5:\n    args:\n  …",
                        "sources:\n  cmip6:\n    args:\n  …",
                        "sources:\n  cmip6:\n    args:\n  …",
                    ],
                }
            ),
            {"realm": ["atmos", "ocnBgchem"]},
            {"variable"},
            ["realm", "variable", "name", "yaml"],
            {"yaml", "realm"},
            pl.LazyFrame(
                {
                    "realm": [
                        ["atmos"],
                        ["ocean"],
                        ["ocnBgchem"],
                        ["atmos"],
                        ["atmos"],
                        ["land"],
                    ],
                    "variable": [
                        ["tas"],
                        ["PO4", "KMT", "SHF"],
                        ["fgco2"],
                        ["hfls", "tasmax"],
                        ["tasmax", "prsn"],
                        ["gpp", "residualFrac"],
                    ],
                    "name": ["gistemp", "cesm", "cmip5", "cmip5", "cmip6", "cmip6"],
                    "yaml": [
                        "sources:\n  gistemp:\n    args:\n…",
                        "sources:\n  cesm:\n    args:\n   …",
                        "sources:\n  cmip5:\n    args:\n  …",
                        "sources:\n  cmip5:\n    args:\n  …",
                        "sources:\n  cmip6:\n    args:\n  …",
                        "sources:\n  cmip6:\n    args:\n  …",
                    ],
                }
            ),
            {"realm"},
            {"variable", "realm"},
            {"yaml", "realm"},
        ),
    ],
)
def test__promote_query_qcols(
    lf,
    query,
    columns_with_iterables,
    all_cols,
    cols_to_deiter,
    expected_lf,
    expected_iterable_cols,
    expected_all_cols,
    expected_cols_to_deiter,
):
    output_lf, iterable_qcols, columns_with_iterables, cols_to_deiter = (
        _promote_query_qcols(
            lf, query, columns_with_iterables, all_cols, cols_to_deiter
        )
    )

    assert iterable_qcols == expected_iterable_cols
    assert columns_with_iterables == expected_all_cols
    assert cols_to_deiter == expected_cols_to_deiter

    pl_assert_frame_equal(output_lf.collect(), expected_lf.collect())
