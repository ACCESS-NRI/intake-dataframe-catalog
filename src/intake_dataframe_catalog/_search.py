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
    if require_all and len(query.get(name_column, [""])) > 1:
        return df.head(0)

    lf: pl.LazyFrame = pl.from_pandas(df).lazy()
    all_cols = lf.columns

    # Keep the iterable columns and their dtypes hanging around for later
    iterable_dtypes = {
        colname: type(df[colname].iloc[0]) for colname in columns_with_iterables
    }
    iterable_qcols = [colname for colname in columns_with_iterables if colname in query]

    filter_first = True
    if require_all and not iterable_qcols:
        # If we've specified require all but we don't have any iterable columns
        # in the query, we shouldn't do anything. Previous behaviour seemed to
        # promote the query columns to iterables at this point.
        iterable_qcols = [colname for colname in query if colname in all_cols]
        columns_with_iterables = [*columns_with_iterables, *iterable_qcols]
        lf = lf.with_columns(
            [pl.col(colname).cast(pl.List(pl.Utf8)) for colname in iterable_qcols]
        )
        filter_first = False

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
            for col in all_cols
        ]
    )

    lf = lf.drop("index").select(all_cols)
    lf = lf.explode(name_column)

    if require_all and iterable_qcols and not lf.collect().is_empty():
        # Find rows where list.len() >= query.len(), and get all the names in those rows
        if filter_first:
            nl = (
                lf.filter(
                    [
                        pl.col(colname).list.len() >= len(query[colname])
                        for colname in iterable_qcols
                    ]
                )
                .select(name_column)
                .collect()
                .to_series()
            )
            lf = lf.filter(pl.col(name_column).is_in(nl))
        else:
            nl = (
                lf.group_by(name_column)
                .agg(
                    [
                        pl.col(col).explode().flatten().unique(maintain_order=True)
                        for col in all_cols
                        if col != name_column
                    ]
                )
                .filter(
                    [
                        pl.col(colname).list.len() >= len(query[colname])
                        for colname in iterable_qcols
                    ]
                )
                .select(name_column)
                .collect()
                .to_series()
            )
            lf = lf.filter(pl.col(name_column).is_in(nl))

    # Now we 'de-iterable' the non-iterable columns.
    non_iter_cols = [
        col
        for col in lf.collect_schema().names()
        if col not in [*columns_with_iterables, name_column]
    ]
    lf = lf.explode(non_iter_cols)
    if not filter_first:
        # We also need to 'de-iterable' the query columns
        lf = lf.explode(iterable_qcols)

    df = lf.collect().to_pandas()
    for col, dtype in iterable_dtypes.items():
        df[col] = df[col].apply(lambda x: dtype(x))

    return df
