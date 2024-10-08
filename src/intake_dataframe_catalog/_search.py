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
    col_order = pl_df.columns

    if not query:
        return pl_df.to_pandas()

    iterable_dtypes = {
        colname: type(df[colname].iloc[0]) for colname in columns_with_iterables
    }

    query = _or_iterable(query)

    iterable_query = {
        colname: col_query
        for colname, col_query in query.items()
        if colname in columns_with_iterables
    }

    iter_list_query = {
        colname: col_query
        for colname, col_query in iterable_query.items()
        if isinstance(col_query, list)
    }

    iter_str_query = {
        colname: col_query
        for colname, col_query in iterable_query.items()
        if isinstance(col_query, str)
    }

    non_iterable_query = {
        colname: col_query
        for colname, col_query in query.items()
        if colname not in columns_with_iterables
    }

    non_iter_list_query = {
        colname: col_query
        for colname, col_query in non_iterable_query.items()
        if isinstance(col_query, list)
    }

    non_iter_str_query = {
        colname: col_query
        for colname, col_query in non_iterable_query.items()
        if isinstance(col_query, str)
    }

    columns_without_iterables = list(set(df.columns) - set(columns_with_iterables))

    pl_df_iterables = pl_df.with_row_index().drop(columns_without_iterables)
    pl_df_non_iterables = pl_df.with_row_index().drop(columns_with_iterables)

    if iter_list_query and require_all:
        # Filter for columns in columns_with_iterables
        pl_df_iterables = pl_df_iterables.filter(
            [
                pl.col(colname).list.set_intersection(col_query) == col_query
                for colname, col_query in iter_list_query.items()
            ]
        ).with_columns(
            [
                pl.col(colname).list.set_intersection(col_query).alias(colname)
                for colname, col_query in iter_list_query.items()
            ]
        )
    elif iter_list_query:
        # Filter for columns in columns_with_iterables
        pl_df_iterables = pl_df_iterables.filter(
            [
                pl.col(colname).list.set_intersection(col_query).len() > 0
                for colname, col_query in iter_list_query.items()
            ]
        ).with_columns(
            [
                pl.col(colname).list.set_intersection(col_query).alias(colname)
                for colname, col_query in iter_list_query.items()
            ]
        )

    # Filter for columns not in columns_with_iterables. Implication here is that
    # require_all only affects the columns in columns_with_iterables - can we
    # double check that's correct?
    if non_iter_list_query:
        pl_df_non_iterables = pl_df_non_iterables.filter(
            [
                pl.col(colname).is_in(col_query)
                for colname, col_query in non_iter_list_query.items()
            ]
        ).with_columns(
            [
                pl.col(colname).alias(colname)
                for colname, col_query in non_iter_list_query.items()
            ]
        )

    if iter_str_query and require_all:
        # If we require all, no messing with the contained list elements to filter
        # missing searches is required.
        pl_df_iterables = pl_df_iterables.filter(
            [
                pl.col(colname)
                .list.eval(pl.element().str.contains(col_query).all())
                .list.first()
                for colname, col_query in iter_str_query.items()
            ]
        )
    elif iter_str_query:
        pl_df_iterables = (
            pl_df_iterables.filter(
                [
                    pl.col(colname)
                    .list.eval(pl.element().str.contains(col_query).any())
                    .list.first()
                    for colname, col_query in iter_str_query.items()
                ]
            )
            .with_columns(
                # We need to subset list columns to only get the elements that match the query
                [
                    pl.col(colname)
                    .list.eval(
                        pl.when(pl.element().str.contains(col_query))
                        .then(pl.element())
                        .otherwise(pl.lit(None))
                    )
                    .list.drop_nulls()
                    for colname, col_query in iter_str_query.items()
                ]
            )
            .with_columns(
                # Sort, but only if the column is a list when we first read it in
                [
                    pl.col(colname).list.sort().alias(colname)
                    for colname in iter_str_query
                    if isinstance(df.loc[0, colname], list)
                ]
            )
        )

    if non_iter_str_query:
        pl_df_non_iterables = pl_df_non_iterables.filter(
            [
                pl.col(colname).str.contains(col_query)
                for colname, col_query in non_iter_str_query.items()
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


def _or_iterable(query: dict[str, Any]) -> dict[str, Any]:
    """
    Change all values which are lists of strings to strings using an '|'.join.
    This should hopefully make searching for regex terms easier.
    """
    new_query = {}
    for key, value in query.items():
        if isinstance(value, list) and all(isinstance(x, str) for x in value):
            new_query[key] = "|".join(value)
        else:
            new_query[key] = value

    return new_query
