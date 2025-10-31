# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0


import re

import pandas as pd
import pytest

from intake_dataframe_catalog._search import _is_pattern, search


@pytest.mark.parametrize(
    "value, expected",
    [
        (2, False),
        ("foo", False),
        ("foo\\**bar", True),
        ("foo\\?*bar", True),
        ("foo\\?\\*bar", False),
        ("foo\\*bar", False),
        (r"foo\*bar*", True),
        ("^foo", True),
        ("^foo.*bar$", True),
        (re.compile("hist.*", flags=re.IGNORECASE), True),
    ],
)
def test_is_pattern(value, expected):
    """
    Test is_pattern function
    """
    assert _is_pattern(value) == expected


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            {},
            [
                {
                    "A": "aaa",
                    "B": "a",
                    "C": 0,
                },
                {
                    "A": "aaa",
                    "B": "b",
                    "C": 1,
                },
                {
                    "A": "aba",
                    "B": "c",
                    "C": 2,
                },
                {
                    "A": "aba",
                    "B": "a",
                    "C": 3,
                },
                {
                    "A": "abA",
                    "B": "b",
                    "C": 4,
                },
                {
                    "A": "abA",
                    "B": "c",
                    "C": 5,
                },
            ],
        ),
        (
            {"A": ["aaa"]},
            [
                {
                    "A": "aaa",
                    "B": "a",
                    "C": 0,
                },
                {
                    "A": "aaa",
                    "B": "b",
                    "C": 1,
                },
            ],
        ),
        (
            {"A": ["aaa"], "B": ["a"]},
            [
                {
                    "A": "aaa",
                    "B": "a",
                    "C": 0,
                },
            ],
        ),
        ({"A": ["aaa"], "C": [2]}, []),
        (
            {"B": ["a", "b"]},
            [
                {
                    "A": "aaa",
                    "B": "a",
                    "C": 0,
                },
                {
                    "A": "aaa",
                    "B": "b",
                    "C": 1,
                },
                {
                    "A": "aba",
                    "B": "a",
                    "C": 3,
                },
                {
                    "A": "abA",
                    "B": "b",
                    "C": 4,
                },
            ],
        ),
        (
            {"B": ["a", "b"], "C": [0, 3]},
            [
                {
                    "A": "aaa",
                    "B": "a",
                    "C": 0,
                },
                {
                    "A": "aba",
                    "B": "a",
                    "C": 3,
                },
            ],
        ),
        (
            {"B": ["^a$"], "C": [0, 3]},
            [
                {
                    "A": "aaa",
                    "B": "a",
                    "C": 0,
                },
                {
                    "A": "aba",
                    "B": "a",
                    "C": 3,
                },
            ],
        ),
        (
            {"A": ["^a.*a$"]},
            [
                {
                    "A": "aaa",
                    "B": "a",
                    "C": 0,
                },
                {
                    "A": "aaa",
                    "B": "b",
                    "C": 1,
                },
                {
                    "A": "aba",
                    "B": "c",
                    "C": 2,
                },
                {
                    "A": "aba",
                    "B": "a",
                    "C": 3,
                },
            ],
        ),
        (
            {"A": ["(?i)^a.*a$"]},
            [
                {
                    "A": "aaa",
                    "B": "a",
                    "C": 0,
                },
                {
                    "A": "aaa",
                    "B": "b",
                    "C": 1,
                },
                {
                    "A": "aba",
                    "B": "c",
                    "C": 2,
                },
                {
                    "A": "aba",
                    "B": "a",
                    "C": 3,
                },
                {
                    "A": "abA",
                    "B": "b",
                    "C": 4,
                },
                {
                    "A": "abA",
                    "B": "c",
                    "C": 5,
                },
            ],
        ),
    ],
)
def test_search(query, expected):
    df = pd.DataFrame(
        {
            "A": ["aaa", "aaa", "aba", "aba", "abA", "abA"],
            "B": ["a", "b", "c", "a", "b", "c"],
            "C": [0, 1, 2, 3, 4, 5],
        }
    )
    results = search(df=df, query=query, columns_with_iterables=[], name_column="A")
    assert isinstance(results, pd.DataFrame)
    assert results.to_dict(orient="records") == expected


@pytest.mark.parametrize(
    "query, require_all, expected",
    [
        (
            {"B": ["a", "b"], "D": [0]},
            False,
            [
                {"A": "cat0", "B": ["a", "b"], "C": ("cx", "cy"), "D": {0}, "E": "xxx"},
                {"A": "cat1", "B": ["a", "b"], "C": ("cx", "cz"), "D": {0}, "E": "xxx"},
                {"A": "cat1", "B": ["a"], "C": ("cz", "cy"), "D": {0}, "E": "yyy"},
            ],
        ),
        (
            {"B": ["a", "b"], "D": [0, 1]},
            False,
            [
                {
                    "A": "cat0",
                    "B": ["a", "b"],
                    "C": ("cx", "cy"),
                    "D": {0, 1},
                    "E": "xxx",
                },
                {"A": "cat1", "B": ["a", "b"], "C": ("cx", "cz"), "D": {0}, "E": "xxx"},
                {"A": "cat1", "B": ["a"], "C": ("cz", "cy"), "D": {0, 1}, "E": "yyy"},
            ],
        ),
        (
            {"B": ["a", "b"], "D": [0]},
            True,
            [
                {"A": "cat0", "B": ["a", "b"], "C": ("cx", "cy"), "D": {0}, "E": "xxx"},
                {"A": "cat1", "B": ["a", "b"], "C": ("cx", "cz"), "D": {0}, "E": "xxx"},
                {"A": "cat1", "B": ["a"], "C": ("cz", "cy"), "D": {0}, "E": "yyy"},
            ],
        ),
        (
            {"B": ["a", "b"], "D": [0, 1]},
            True,
            [
                {
                    "A": "cat0",
                    "B": ["a", "b"],
                    "C": ("cx", "cy"),
                    "D": {0, 1},
                    "E": "xxx",
                },
            ],
        ),
        (
            {"C": ["cx", "cy"], "E": ["xxx"]},
            False,
            [
                {
                    "A": "cat0",
                    "B": ["a", "b"],
                    "C": ("cx", "cy"),
                    "D": {0, 1},
                    "E": "xxx",
                },
                {
                    "A": "cat1",
                    "B": ["a", "b", "c"],
                    "C": ("cx",),
                    "D": {0, 2},
                    "E": "xxx",
                },
            ],
        ),
        (
            {"C": ["cx", "cy"], "E": ["xxx"]},
            True,
            [
                {
                    "A": "cat0",
                    "B": ["a", "b"],
                    "C": ("cx", "cy"),
                    "D": {0, 1},
                    "E": "xxx",
                },
            ],
        ),
        (
            {"A": ["cat1"], "B": ["c"], "C": ["cz"], "D": [2]},
            True,
            [
                {"A": "cat1", "B": ["c"], "C": ("cz",), "D": {2}, "E": "xxx"},
            ],
        ),
        (
            {"A": ["cat1"], "B": ["c"], "C": ["cx"], "D": [1]},
            True,
            [],
        ),
        (
            {"A": ["cat.*"], "B": ["a", "c"], "C": ["c.*"]},
            False,
            [
                {"A": "cat0", "B": ["a"], "C": ("cx", "cy"), "D": {0, 1}, "E": "xxx"},
                {
                    "A": "cat1",
                    "B": ["a", "c"],
                    "C": ("cx", "cz"),
                    "D": {0, 2},
                    "E": "xxx",
                },
                {
                    "A": "cat1",
                    "B": ["c", "a"],
                    "C": ("cz", "cy"),
                    "D": {0, 1},
                    "E": "yyy",
                },
            ],
        ),
        (
            {"A": ["cat.*"], "B": ["a", "c"], "C": ["c.*"]},
            True,
            [
                {
                    "A": "cat1",
                    "B": ["a", "c"],
                    "C": ("cx", "cz"),
                    "D": {0, 2},
                    "E": "xxx",
                },
                {
                    "A": "cat1",
                    "B": ["c", "a"],
                    "C": ("cz", "cy"),
                    "D": {0, 1},
                    "E": "yyy",
                },
            ],
        ),
    ],
)
def test_search_columns_with_iterables(query, require_all, expected):
    df = pd.DataFrame(
        {
            "A": ["cat0", "cat1", "cat1"],
            "B": [["a", "b"], ["a", "b", "c"], ["c", "d", "a"]],
            "C": [("cx", "cy"), ("cx", "cz"), ("cz", "cy")],
            "D": [{0, 1}, {0, 2}, {0, 1}],
            "E": ["xxx", "xxx", "yyy"],
        }
    )
    results = search(
        df=df,
        query=query,
        columns_with_iterables=["B", "C", "D"],
        name_column="A",
        require_all=require_all,
    ).to_dict(orient="records")
    assert results == expected


def test_search_variable_regex_and_exact():
    """
    Simulate a catalog DataFrame. Reproduced from
    https://github.com/ACCESS-NRI/access-nri-intake-catalog/issues/527
    """
    df = pd.DataFrame(
        {
            "model": ["ACCESS-OM2-1", "ACCESS-OM2-2", "ACCESS-OM2-3"],
            "variable": [
                ["tx_trans", "ty_trans", "mld", "area_t", "dht", "dzt"],
                ["tx_trans", "ty_trans", "mld", "area_t"],  # missing dht/dzt
                [
                    "tx_trans",
                    "ty_trans",
                    "mld",
                    "dht",
                    "dzt",
                ],  # missing area_t - but will match twice on "^d[hz]t$",
            ],
            "experiment": ["exp1", "exp2", "exp3"],
        }
    )

    variable = [
        "tx_trans",
        "ty_trans",
        "mld",
        "area_t",
        "^d[hz]t$",
    ]

    # Should only match rows containing all required variables - only exp1 as it has both dht and dzt
    results = search(
        df=df,
        query={"model": ["ACCESS-OM2.*"], "variable": variable},
        columns_with_iterables=["variable"],
        name_column="experiment",
        require_all=True,
    )
    matched_experiments = results["experiment"].tolist()
    assert matched_experiments == [
        "exp1"
    ], f"Failed case 1: Expected only exp1, got {matched_experiments}"

    # Now try with all variables as regex
    variable_regex = [
        "^tx_trans$",
        "^ty_trans$",
        "^mld$",
        "^area_t$",
        "^d[hz]t$",
    ]
    results_regex = search(
        df=df,
        query={"model": ["ACCESS-OM2.*"], "variable": variable_regex},
        columns_with_iterables=["variable"],
        name_column="experiment",
        require_all=True,
    )
    matched_experiments_regex = results_regex["experiment"].tolist()
    assert matched_experiments_regex == [
        "exp1"
    ], f"Failed case 2: Expected only exp1, got {matched_experiments_regex}"
