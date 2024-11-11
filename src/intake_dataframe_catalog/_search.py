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
    columns_with_iterables = set(columns_with_iterables)
    iterable_qcols = columns_with_iterables.intersection(query)
    cols_to_deiter = set(all_cols).difference(columns_with_iterables, {name_column})

    if require_all and not iterable_qcols:
        # If we've specified require all but we don't have any iterable columns
        # in the query we promote the query columns to iterables at this point.
        group_on_names = True
        iterable_qcols = set(query).intersection(all_cols)

        lf = lf.with_columns(
            [pl.col(colname).cast(pl.List(pl.Utf8)) for colname in iterable_qcols]
        )
        # Keep track of the newly promoted columns & the need to de-iterable them later
        columns_with_iterables.update(iterable_qcols)
        cols_to_deiter.update(iterable_qcols)
    else:
        group_on_names = False

    lf = lf.with_row_index()
    for column in columns_with_iterables:
        lf = lf.explode(column)

    for colname, subquery in query.items():
        if lf.schema[colname] == pl.Utf8 and _is_pattern(subquery):
            pattern = "|".join(subquery)
            lf = lf.filter(pl.col(colname).str.contains(pattern))
        else:
            lf = lf.filter(pl.col(colname).is_in(subquery))

    lf = (
        lf.group_by("index")  # Piece the exploded columns back together
        .agg(
            [  # Re-aggregate the exploded columns into lists, flatten them out (imploding creates nested lists) and drop duplicates
                pl.col(col).implode().flatten().unique(maintain_order=True)
                for col in all_cols
            ]
        )
        .drop("index")  # We don't need the index anymore
        .explode(name_column)  # Explode the name column back out so we can select on it
    )

    if require_all and iterable_qcols:
        if group_on_names:
            # Group by name_column and aggregate the other columns into lists
            # first in this instance. Essentially the opposite of the previous
            # group_by("index") operation.
            nl_lf = lf.group_by(name_column).agg(
                [
                    pl.col(col).explode().flatten().unique(maintain_order=True)
                    for col in (set(all_cols) - {name_column})
                ]
            )
        else:
            nl_lf = lf

        nl = (
            nl_lf.filter(
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

    df = lf.explode(list(cols_to_deiter)).collect().to_pandas()

    for col, dtype in iterable_dtypes.items():
        df[col] = df[col].apply(lambda x: dtype(x))

    return df
