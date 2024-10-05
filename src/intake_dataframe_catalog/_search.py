# Copyright 2023 ACCESS-NRI and contributors. See the top-level COPYRIGHT file for details.
# SPDX-License-Identifier: Apache-2.0

# Stolen and adapted from https://github.com/intake/intake-esm/blob/main/intake_esm/_search.py

from typing import Any, Union

import pandas as pd
import polars as pl


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

    # TODO: Make this also work for regex queries.
    pl_df: pl.DataFrame = pl.from_pandas(df)

    iterable_dtypes = {
        colname: type(df[colname].iloc[0]) for colname in columns_with_iterables
    }

    if not query:
        return pl_df.to_pandas()

    iterable_query = {
        colname: col_query
        for colname, col_query in query.items()
        if colname in columns_with_iterables
    }

    non_iterable_query = {
        colname: col_query
        for colname, col_query in query.items()
        if colname not in columns_with_iterables
    }

    columns_without_iterables = list(set(df.columns) - set(columns_with_iterables))

    col_order = pl_df.columns
    pl_df_iterables = pl_df.with_row_index().drop(columns_without_iterables)
    pl_df_non_iterables = pl_df.with_row_index().drop(columns_with_iterables)

    if iterable_query and require_all:
        # Filter for columns in columns_with_iterables
        pl_df_iterables = pl_df_iterables.filter(
            [
                pl.col(colname).list.set_intersection(col_query) == col_query
                for colname, col_query in iterable_query.items()
            ]
        ).with_columns(
            [
                pl.col(colname).list.set_intersection(col_query).alias(colname)
                for colname, col_query in iterable_query.items()
            ]
        )
    elif iterable_query:
        # Filter for columns in columns_with_iterables
        pl_df_iterables = pl_df_iterables.filter(
            [
                pl.col(colname).list.set_intersection(col_query).len() > 0
                for colname, col_query in iterable_query.items()
            ]
        ).with_columns(
            [
                pl.col(colname).list.set_intersection(col_query).alias(colname)
                for colname, col_query in iterable_query.items()
            ]
        )

    # Filter for columns not in columns_with_iterables. Implication here is that
    # require_all only affects the columns in columns_with_iterables - can we
    # double check that's correct?
    if non_iterable_query:
        pl_df_non_iterables = pl_df_non_iterables.filter(
            [
                pl.col(colname).is_in(col_query)
                for colname, col_query in non_iterable_query.items()
            ]
        ).with_columns(
            [
                pl.col(colname).alias(colname)
                for colname, col_query in non_iterable_query.items()
            ]
        )

    pl_df = (
        pl_df_iterables.join(pl_df_non_iterables, on="index")
        .drop("index")
        .select(col_order)
    )

    if pl_df.is_empty():
        return pd.DataFrame()

    df = pl_df.to_pandas()
    for col, dtype in iterable_dtypes.items():
        df[col] = df[col].apply(lambda x: dtype(x))

    return df
