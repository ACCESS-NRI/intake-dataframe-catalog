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
    columns_with_iterables: Collection[str],
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

    if isinstance(columns_with_iterables, str):
        columns_with_iterables = [columns_with_iterables]

    lf: pl.LazyFrame = pl.from_pandas(df)  # .lazy()
    all_cols = lf.collect_schema().names()

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

    schema = lf.collect_schema()
    matches_exprs, filter_exprs, tmp_cols = [], [], []

    for colname, subquery in query.items():
        is_string = schema[colname] == pl.Utf8
        is_regex = is_string and _is_pattern(subquery)

        if is_regex:
            match_exprs = [
                pl.when(pl.col(colname).str.contains(q)).then(pl.lit(q))
                for q in subquery
            ]
            matches_exprs.append(
                pl.concat_list(match_exprs)
                .list.drop_nulls()
                .alias(f"{colname}_matches")
            )
            tmp_cols.append(f"{colname}_matches")
        else:
            filter_exprs.append(pl.col(colname).is_in(subquery))

    if matches_exprs:
        lf = lf.with_columns(matches_exprs)
    if filter_exprs:
        lf = lf.filter(pl.all_horizontal(filter_exprs))

    lf = (
        lf.group_by("index")
        .agg(
            [
                pl.col(c).flatten().unique(maintain_order=True)
                for c in [*all_cols, *tmp_cols]
            ]
        )
        .explode(name_column)
    )

    if require_all and iterable_qcols:
        lf = lf.filter(
            pl.all_horizontal(
                [
                    pl.col(f"{c}_matches").list.len() >= len(query[c])
                    for c in iterable_qcols
                ]
            )
        )

    lf = lf.select(*all_cols)

    df = lf.explode(list(cols_to_deiter)).to_pandas()  # .collect().to_pandas()

    for col, dtype in iterable_dtypes.items():
        df[col] = df[col].apply(lambda x: dtype(x))

    return df
