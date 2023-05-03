import re

import pytest

import pandas as pd

from intake_dataframe_catalog._search import is_pattern, search


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
    assert is_pattern(value) == expected


@pytest.mark.parametrize(
    "query, expected",
    [
        ({}, []),
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
            {"B": [re.compile(r"^a$")], "C": [0, 3]},
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
            {"A": [re.compile("^a.*a$", flags=re.IGNORECASE)]},
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
    results = search(df=df, query=query, columns_with_iterables=[])
    assert isinstance(results, pd.DataFrame)
    assert results.to_dict(orient="records") == expected


@pytest.mark.parametrize(
    "query, require_all_on, expected",
    [
        (
            {"B": ["a", "b"], "D": [0]},
            None,
            [
                {"A": "aaa", "B": ["a", "b"], "C": ("cx", "cy"), "D": {0}},
                {"A": "aba", "B": ["a", "b"], "C": ("cx", "cz"), "D": {0}},
                {"A": "abA", "B": ["a"], "C": ("cx", "cy"), "D": {0}},
            ],
        ),
        (
            {"B": ["a", "b"], "D": [0, 1]},
            None,
            [
                {"A": "aaa", "B": ["a", "b"], "C": ("cx", "cy"), "D": {0, 1}},
                {"A": "aba", "B": ["a", "b"], "C": ("cx", "cz"), "D": {0}},
                {"A": "abA", "B": ["a"], "C": ("cx", "cy"), "D": {0, 1}},
            ],
        ),
        (
            {"B": ["a", "b"], "D": [0]},
            "A",
            [
                {"A": "aaa", "B": ["a", "b"], "C": ("cx", "cy"), "D": {0}},
                {"A": "aba", "B": ["a", "b"], "C": ("cx", "cz"), "D": {0}},
            ],
        ),
        (
            {"B": ["a", "b"], "D": [0, 1]},
            "A",
            [
                {"A": "aaa", "B": ["a", "b"], "C": ("cx", "cy"), "D": {0, 1}},
            ],
        ),
        (
            {"A": ["aba"], "B": ["c"], "C": ["cz"], "D": [2]},
            "A",
            [
                {"A": "aba", "B": ["c"], "C": ("cz",), "D": {2}},
            ],
        ),
        (
            {"A": ["aba"], "B": ["c"], "C": ["cz"], "D": [1]},
            "A",
            [],
        ),
        (
            {"A": ["a.*a"], "B": ["a", "c"], "C": ["c.*"]},
            None,
            [
                {"A": "aaa", "B": ["a"], "C": ("cx", "cy"), "D": {0, 1}},
                {"A": "aba", "B": ["a", "c"], "C": ("cx", "cz"), "D": {0, 2}},
            ],
        ),
        (
            {"A": ["a.*a"], "B": ["a", "c"], "C": ["c.*"]},
            "A",
            [
                {"A": "aba", "B": ["a", "c"], "C": ("cx", "cz"), "D": {0, 2}},
            ],
        ),
    ],
)
def test_search_columns_with_iterables(query, require_all_on, expected):
    df = pd.DataFrame(
        {
            "A": ["aaa", "aba", "abA"],
            "B": [["a", "b"], ["a", "b", "c"], ["c", "d", "a"]],
            "C": [("cx", "cy"), ("cx", "cz"), ("cx", "cy")],
            "D": [{0, 1}, {0, 2}, {0, 1}],
        }
    )
    results = search(
        df=df,
        query=query,
        columns_with_iterables=["B", "C", "D"],
        require_all_on=require_all_on,
    ).to_dict(orient="records")
    assert results == expected
