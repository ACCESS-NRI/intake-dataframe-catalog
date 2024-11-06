# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

# Stolen and adapted from https://github.com/intake/intake-esm/blob/main/intake_esm/_search.py

from collections.abc import Collection
from re import Pattern
from typing import Any, Union

import pandas as pd
import polars as pl


def _is_pattern(input: Union[str, Pattern, Collection]) -> bool:
    """
    Check whether the passed value is a pattern

    Parameters
    ----------
    value: str or Pattern
        The value to check
    """
    if isinstance(input, Pattern):
        return True  # Obviously, it's a pattern
    if isinstance(input, Collection) and not isinstance(input, str):
        return any(_is_pattern(item) for item in input)  # Recurse into the collection
    wildcard_chars = {"*", "?", "$", "^"}
    try:
        value_ = input
        for char in wildcard_chars:
            value_ = value_.replace(rf"\{char}", "")
        return any(char in value_ for char in wildcard_chars)
    except (TypeError, AttributeError):
        return False


def search(
    df: pd.DataFrame,
    query: dict[str, Any],
    columns_with_iterables: Union[list[str], set[str]],
    name_column: str,
    require_all: bool = False,
) -> pd.DataFrame:
    """
    Search for entries in the catalog.

    Parameters
    ----------
    df: :py:class:`~pandas.DataFrame`
        A dataframe to search
    query: dict
        A dictionary of query parameters to execute against the dataframe
    columns_with_iterables: list
        Columns in the dataframe that have iterables
    name_column: str
        The name column in the dataframe catalog
    require_all: bool
        If True, groupby name_column and return only entries that match
        for all elements in each group
    Returns
    -------
    dataframe: :py:class:`~pandas.DataFrame`
            A new dataframe with the entries satisfying the query criteria.
    """

    if not query:
        return df

    lf: pl.LazyFrame = pl.from_pandas(df).lazy()
    col_order = lf.columns

    # Keep the iterable columns and their dtypes hanging around for later
    iterable_dtypes = {
        colname: type(df[colname].iloc[0]) for colname in columns_with_iterables
    }
    iterable_qcols = [colname for colname in columns_with_iterables if colname in query]

    lf = lf.with_row_index()
    for column in columns_with_iterables:
        lf = lf.explode(column)

    for colname, subquery in query.items():
        if lf.schema[colname] == pl.Utf8 and _is_pattern(subquery):
            pattern = "|".join(subquery)
            lf = lf.filter(pl.col(colname).str.contains(pattern))
        else:
            lf = lf.filter(pl.col(colname).is_in(subquery))

    lf = lf.group_by("index").agg(
        [
            pl.col(col).implode().flatten().unique(maintain_order=True)
            for col in col_order
        ]
    )

    lf = lf.drop("index").select(col_order)

    if require_all and iterable_qcols and not lf.collect().is_empty():
        # Drop rows where list.len() >= query.len()
        lf = lf.filter(
            [
                pl.col(colname).list.len() >= len(query[colname])
                for colname in iterable_qcols
            ]
        )

    # Now we 'de-iterable' the non-iterable columns.
    non_iter_cols = [col for col in lf.columns if col not in columns_with_iterables]
    lf = lf.explode(non_iter_cols)

    df = lf.collect().to_pandas()
    for col, dtype in iterable_dtypes.items():
        df[col] = df[col].apply(lambda x: dtype(x))

    return df
