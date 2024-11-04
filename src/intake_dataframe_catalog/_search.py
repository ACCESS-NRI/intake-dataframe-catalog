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
    init_df = pl_df
    col_order = pl_df.columns

    if not query:
        return pl_df.to_pandas()

    iterable_dtypes = {
        colname: type(df[colname].iloc[0]) for colname in columns_with_iterables
    }

    pl_df = pl_df.with_row_index()
    for column in columns_with_iterables:
        pl_df = pl_df.explode(column)

    for colname, subquery in query.items():
        pl_df = pl_df.filter(pl.col(colname).is_in(subquery))
        """
        f1_df = pl_df.filter(pl.col(colname).is_in(subquery))
        f2_df = pl_df.filter([pl.col(colname).str.contains(subq) for subq in subquery])
        pl_df = pl.concat([f1_df, f2_df])
        # Sort by index to maintain order
        pl_df = pl_df.sort(by="index", maintain_order=True)
        """

    pl_df = pl_df.group_by("index").agg(
        [
            pl.col(col).implode().flatten().unique(maintain_order=True)
            for col in col_order
        ]
    )

    pl_df = pl_df.drop("index").select(col_order)

    # Now we 'de-iterable' the non-iterable columns.
    non_iter_cols = [col for col in pl_df.columns if col not in columns_with_iterables]
    pl_df = pl_df.explode(non_iter_cols)

    if require_all:
        # Drop rows where list.len() >= query.len()
        pl_df = pl_df.filter(
            [
                pl.col(colname).list.len() == len(query[colname])
                for colname in columns_with_iterables
                if colname in query
            ]
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
